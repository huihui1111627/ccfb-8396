#!/usr/bin/env python3
"""Controlled chaos injector for imstore.

This guarantees every object loses EXACTLY two shards — the maximum a 4+2
Reed-Solomon layout can tolerate — split between whole-shard deletion and
single-bit corruption. Every object must remain fully readable and hash-clean.

Usage:
  python3 -I inject.py --data-root data
"""
import argparse
import os
import random
import re

SHARD_RE = re.compile(r"^(?P<oid>[0-9a-f]+(?:-p\d+)?)\.(?P<idx>\d+)\.ims$")


def group_shards(root):
    """Map object id -> {shard_index: path}."""
    objects = {}
    for dirpath, _, files in os.walk(root):
        for name in files:
            m = SHARD_RE.match(name)
            if not m:
                continue
            oid = m.group("oid")
            idx = int(m.group("idx"))
            objects.setdefault(oid, {})[idx] = os.path.join(dirpath, name)
    return objects


def flip_bit(path):
    with open(path, "r+b") as f:
        f.seek(512)  # inside payload, safely past the variable-length header
        b = f.read(1)
        f.seek(512)
        f.write(bytes([b[0] ^ 0x01]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--seed", type=int, default=99)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    objects = group_shards(args.data_root)
    deleted = flipped = 0
    for oid, shards in sorted(objects.items()):
        indices = sorted(shards)
        # Exactly two shards are damaged: one deleted, one bit-flipped.
        del_idx, flip_idx = rng.sample(indices, 2)
        os.remove(shards[del_idx])
        deleted += 1
        flip_bit(shards[flip_idx])
        flipped += 1
    print(f"controlled chaos: {deleted} shards deleted + {flipped} bit-flipped "
          f"(exactly 2 damaged per object) across {len(objects)} objects")


if __name__ == "__main__":
    main()
