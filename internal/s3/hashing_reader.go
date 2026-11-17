package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
)

// errPayloadHash is returned when the received body does not match the signed
// x-amz-content-sha256 digest. ServeHTTP maps it to SignatureDoesNotMatch.
var errPayloadHash = errors.New("payload hash does not match x-amz-content-sha256")

// hashingReader wraps a request body and runs a final hash check once the
// whole payload has been consumed (or on Close). It enforces that the bytes a
// client actually sent match the x-amz-content-sha256 value it signed.
type hashingReader struct {
	src      io.Reader
	h        hash.Hash
	declared string
	fired    bool
}

// newHashingReader builds a body wrapper that verifies everything read
// against declaredHex (the signed content hash).
func newHashingReader(src io.Reader, declaredHex string) *hashingReader {
	return &hashingReader{src: src, h: sha256.New(), declared: declaredHex}
}

func (r *hashingReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		r.h.Write(p[:n])
	}
	// Do not surface the hash verdict on EOF: io.ReadFull treats EOF as a
	// normal end-of-stream and would drop a companion error. The authoritative
	// check happens in Close, which ServeHTTP always invokes.
	return n, err
}

func (r *hashingReader) Close() error {
	if r.fired {
		return nil
	}
	if _, err := io.Copy(io.Discard, r.src); err != nil {
		return err
	}
	r.fired = true
	return r.check()
}

func (r *hashingReader) check() error {
	if hex.EncodeToString(r.h.Sum(nil)) != r.declared {
		return errPayloadHash
	}
	return nil
}
