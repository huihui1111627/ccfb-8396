#!/usr/bin/env bash
# End-to-end acceptance test for imstore.
#
# Brings up a fresh 6-disk 4+2 node, generates fake imaging files, uploads
# them over the S3 API, injects every class of required failure (random shard
# deletion, bit flips, full disk removal), exercises repair, and asserts that
# every object's SHA-256 matches what was written.
#
# Usage: test/e2e.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

WORK="$(mktemp -d)"
trap 'set +e; kill "$SERVER_PID" 2>/dev/null; rm -rf "$WORK"' EXIT

DATA="$WORK/data"
FAKES="$WORK/fakes"
CFG="$WORK/config.json"
PORT=18333
export IMSTORE_ACCESS_KEY=imstoreadmin
export IMSTORE_SECRET_KEY=imstoresecret
export IMSTORE_REGION=us-east-1

green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
step()  { printf '\n=== %s ===\n' "$*"; }

step "build"
go build -o "$WORK/imstore" ./cmd/imstore
go build -o "$WORK/imctl"   ./cmd/imctl

step "write 6-disk 4+2 config (6 distinct enclosures)"
mkdir -p "$DATA/meta"
vols=()
for i in 0 1 2 3 4 5; do
  vols+=("$(printf '{"id":"disk%d","path":"%s/disk%d","enclosure":"rack%d"}' "$i" "$DATA" "$i" "$i")")
done
IFS=, ; voljson="${vols[*]}" ; unset IFS
cat > "$CFG" <<EOF
{
  "listen": ":$PORT",
  "external_url": "http://127.0.0.1:$PORT",
  "region": "us-east-1",
  "access_key": "imstoreadmin",
  "secret_key": "imstoresecret",
  "console_token": "console-token",
  "data_shards": 4,
  "parity_shards": 2,
  "stripe_size": 1048576,
  "meta_db": "$DATA/meta/meta.db",
  "volumes": [$voljson],
  "repair": {"max_bytes_per_second": 8388608, "yield_when_busy": true, "interval_seconds": 3},
  "scrub_interval_seconds": 3600,
  "multipart_max_part_hours": 24
}
EOF

step "start server"
"$WORK/imstore" --config "$CFG" > "$WORK/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 50); do
  curl -s -o /dev/null "http://127.0.0.1:$PORT/" && break
  sleep 0.1
done

EP="http://127.0.0.1:$PORT"
step "create bucket and generate 25 fake images"
"$WORK/imctl" mb --endpoint "$EP" --bucket images
"$WORK/imctl" gen --out "$FAKES" --count 25 --min 16 --max 1024 --seed 7

step "upload over S3 API"
"$WORK/imctl" upload --endpoint "$EP" --bucket images --dir "$FAKES" >/dev/null

step "baseline hash verification"
"$WORK/imctl" verify --endpoint "$EP" --bucket images --manifest "$FAKES/upload-manifest.json" \
  | tee "$WORK/baseline.txt"
grep -q "ALL 25 OBJECTS VERIFIED" "$WORK/baseline.txt" || { red "baseline failed"; exit 1; }
green "baseline: all 25 objects match"

step "CHAOS 1: exactly 2 shards damaged per object (1 deleted + 1 bit flip)"
python3 -I test/inject.py --data-root "$DATA" --seed 99

step "read every object through failures (on-the-fly reconstruction)"
"$WORK/imctl" verify --endpoint "$EP" --bucket images --manifest "$FAKES/upload-manifest.json" \
  | tee "$WORK/chaos1.txt"
grep -q "ALL 25 OBJECTS VERIFIED" "$WORK/chaos1.txt" || { red "read-through-failure failed"; exit 1; }
green "tolerance: all 25 objects readable with 2 shards lost each"

step "trigger repair and wait for full shard restoration"
curl -s -X POST -H "Authorization: Bearer console-token" "$EP/api/v1/repair/scan" >/dev/null
want=$(find "$DATA" -name '*.ims' | wc -l | tr -d ' ')
# 25 objects * 6 shards = 150 once fully healed.
for _ in $(seq 1 100); do
  cur=$(find "$DATA" -name '*.ims' | wc -l | tr -d ' ')
  [ "$cur" = "150" ] && break
  sleep 0.3
done
[ "$(find "$DATA" -name '*.ims' | wc -l | tr -d ' ')" = "150" ] \
  || { red "repair did not restore 150 shards"; exit 1; }
"$WORK/imctl" verify --endpoint "$EP" --bucket images --manifest "$FAKES/upload-manifest.json" \
  | grep -q "ALL 25 OBJECTS VERIFIED"
green "repair: all shards rebuilt and hashes intact"

step "CHAOS 2: remove an entire volume (pull disk)"
curl -s -X POST -H "Authorization: Bearer console-token" "$EP/api/v1/volumes/disk3/offline" >/dev/null
"$WORK/imctl" verify --endpoint "$EP" --bucket images --manifest "$FAKES/upload-manifest.json" \
  | grep -q "ALL 25 OBJECTS VERIFIED"
green "disk pull: objects remain readable"

step "reinsert disk and rebuild the volume"
curl -s -X POST -H "Authorization: Bearer console-token" "$EP/api/v1/volumes/disk3/online" >/dev/null
for _ in $(seq 1 100); do
  n=$(find "$DATA/disk3" -name '*.ims' 2>/dev/null | wc -l | tr -d ' ')
  [ "$n" = "25" ] && break
  sleep 0.3
done
[ "$(find "$DATA/disk3" -name '*.ims' | wc -l | tr -d ' ')" = "25" ] \
  || { red "volume rebuild incomplete"; exit 1; }
"$WORK/imctl" verify --endpoint "$EP" --bucket images --manifest "$FAKES/upload-manifest.json" \
  | grep -q "ALL 25 OBJECTS VERIFIED"
green "volume rebuild: disk3 fully restored, hashes intact"

step "presigned link expiry"
fresh="$($WORK/imctl presign --endpoint "$EP" --bucket images \
  --key "$(python3 -c 'import json;print(json.load(open("'$FAKES'/upload-manifest.json"))[0]["key"])')" --expires 5m)"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$fresh")" = "200" ] || { red "fresh presign failed"; exit 1; }
expired="$($WORK/imctl presign --endpoint "$EP" --bucket images \
  --key "$(python3 -c 'import json;print(json.load(open("'$FAKES'/upload-manifest.json"))[0]["key"])')" --expires 1s)"
sleep 2
[ "$(curl -s -o /dev/null -w '%{http_code}' "$expired")" = "403" ] || { red "expired presign not rejected"; exit 1; }
green "presigned: fresh link 200, expired link 403"

step "multipart upload integrity"
dd if=/dev/urandom of="$WORK/big.dcm" bs=1024 count=1200 2>/dev/null
"$WORK/imctl" multipart --endpoint "$EP" --bucket images --key mp/big.dcm \
  --file "$WORK/big.dcm" --part-size 256
"$WORK/imctl" download --endpoint "$EP" --bucket images --key mp/big.dcm --out "$WORK/big-back.dcm"
[ "$(shasum -a 256 "$WORK/big.dcm" | cut -d' ' -f1)" = "$(shasum -a 256 "$WORK/big-back.dcm" | cut -d' ' -f1)" ] \
  || { red "multipart hash mismatch"; exit 1; }
green "multipart: downloaded bytes identical"

printf '\n'
green "########################################"
green "# ALL END-TO-END ACCEPTANCE CHECKS PASSED"
green "########################################"
