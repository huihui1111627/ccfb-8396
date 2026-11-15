// Package rowhash provides canonical value encoding and row hashing shared
// by the source scanner, the DuckDB target, the reconciler and the simulator.
// The encoding is type-tagged so a SQLite INTEGER and a DuckDB BIGINT holding
// the same value hash identically.
package rowhash

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"
)

// Canonical encodes a driver value as a type-tagged canonical string.
func Canonical(v any) string {
	switch t := v.(type) {
	case nil:
		return "0:"
	case int64:
		return "1:" + strconv.FormatInt(t, 10)
	case int:
		return "1:" + strconv.Itoa(t)
	case int32:
		return "1:" + strconv.FormatInt(int64(t), 10)
	case int16:
		return "1:" + strconv.FormatInt(int64(t), 10)
	case float64:
		return "2:" + strconv.FormatFloat(t, 'g', -1, 64)
	case float32:
		return "2:" + strconv.FormatFloat(float64(t), 'g', -1, 64)
	case string:
		return "3:" + t
	case []byte:
		return "4:" + hex.EncodeToString(t)
	case bool:
		if t {
			return "1:1"
		}
		return "1:0"
	case time.Time:
		return "5:" + t.UTC().Format("2006-01-02 15:04:05.999999999")
	default:
		return "9:" + fmt.Sprintf("%v", t)
	}
}

const keySep = "\x1f"

// KeyString builds the storage key from key column values.
func KeyString(keyVals []any) string {
	parts := make([]string, len(keyVals))
	for i, v := range keyVals {
		parts[i] = Canonical(v)
	}
	return strings.Join(parts, keySep)
}

// HashRow hashes a full row (values in stable column order).
// Deprecated for sync paths; kept for tests.
func HashRow(vals []any) uint64 {
	h := fnv.New64a()
	var buf [64]byte
	for _, v := range vals {
		s := Canonical(v)
		h.Write([]byte(s))
		h.Write(buf[:1]) // single 0 byte separator
	}
	return h.Sum64()
}

// ColHash hashes one named column value. Row hashes are the SUM of column
// hashes, which makes them schema-evolution friendly: adding a column shifts
// every existing row's hash by the same constant (the hash of the default),
// so the persisted state can be adjusted without re-reading the source.
func ColHash(name string, v any) uint64 {
	h := fnv.New64a()
	h.Write([]byte(name))
	h.Write([]byte{0})
	h.Write([]byte(Canonical(v)))
	return h.Sum64()
}

// HashRowCols computes the additive row hash over named columns.
func HashRowCols(cols []string, vals []any) uint64 {
	var sum uint64
	for i, c := range cols {
		sum += ColHash(c, vals[i])
	}
	return sum
}

// BucketOf assigns a key string to one of k buckets.
func BucketOf(key string, k int) uint32 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return uint32(h.Sum64() % uint64(k))
}
