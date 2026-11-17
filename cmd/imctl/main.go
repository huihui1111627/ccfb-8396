// Command imctl is the test and operations client for imstore. It generates
// fake imaging files, drives the S3 API with a self-contained SigV4 signer,
// injects chaos (shard deletion, bit flips, disk removal) and verifies that
// every object's hash survives.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "gen":
		err = cmdGen(args)
	case "mb":
		err = cmdMakeBucket(args)
	case "rb":
		err = cmdRemoveBucket(args)
	case "upload":
		err = cmdUpload(args)
	case "download":
		err = cmdDownload(args)
	case "ls":
		err = cmdList(args)
	case "rm":
		err = cmdDelete(args)
	case "verify":
		err = cmdVerify(args)
	case "chaos":
		err = cmdChaos(args)
	case "presign":
		err = cmdPresign(args)
	case "multipart":
		err = cmdMultipart(args)
	case "hash":
		err = cmdHashFile(args)
	default:
		usage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "imctl %s: %v\n", cmd, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `imctl - imstore test/operations client

Usage:
  imctl gen --out DIR --count N [--min KIB] [--max KIB] [--seed S]
  imctl mb   --endpoint URL --bucket NAME
  imctl rb   --endpoint URL --bucket NAME
  imctl upload   --endpoint URL --bucket NAME --dir DIR [--manifest FILE] [--prefix P]
  imctl download --endpoint URL --bucket NAME --key K --out FILE
  imctl ls   --endpoint URL --bucket NAME [--prefix P] [--max N]
  imctl rm   --endpoint URL --bucket NAME --key K
  imctl verify   --endpoint URL --bucket NAME --manifest FILE
  imctl chaos    --data-root DIR --mode delete-shard|flip-bit|remove-volume [--ratio F] [--volume DIR]
  imctl presign  --endpoint URL --bucket NAME --key K [--expires DURATION]
  imctl multipart --endpoint URL --bucket NAME --key K --file F [--part-size KIB]
  imctl hash --file F
`)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func newSignerFromFlags() *signer {
	return &signer{
		accessKey: envOr("IMSTORE_ACCESS_KEY", "imstoreadmin"),
		secretKey: envOr("IMSTORE_SECRET_KEY", "imstoresecret"),
		region:    envOr("IMSTORE_REGION", "us-east-1"),
	}
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 256*1024)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// seededRand returns a deterministic RNG for reproducible chaos.
func seededRand(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }
