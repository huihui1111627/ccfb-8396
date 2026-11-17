package s3sig

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// chunkDecoder transparently decodes an AWS chunked request body.
//
// Wire format per chunk:
//
//	<hex chunk length>;chunk-signature=<sig>\r\n
//	<chunk bytes>\r\n
//
// A zero-length chunk terminates the stream. In signed mode every chunk's
// signature is validated against the rolling "previous signature" chain; in
// unsigned-trailer mode only framing is parsed.
type chunkDecoder struct {
	br      *bufio.Reader
	secret  string
	date    string // yyyyMMdd
	amzDate string // full yyyyMMdd'T'HHmmss'Z' from X-Amz-Date
	region  string
	prevSig string
	signed  bool
	done    bool
	pending bytes.Buffer
}

func newChunkDecoder(body io.Reader, secret, date, amzDate, region string, signed bool) *chunkDecoder {
	return &chunkDecoder{
		br:      bufio.NewReaderSize(body, 64*1024),
		secret:  secret,
		date:    date,
		amzDate: amzDate,
		region:  region,
		signed:  signed,
	}
}

// SetSeedSignature injects the already-verified outer signature, which seeds
// the chunk signature chain.
func (d *chunkDecoder) SetSeedSignature(sig string) { d.prevSig = sig }

func (d *chunkDecoder) Read(p []byte) (int, error) {
	if d.done {
		return 0, io.EOF
	}
	if d.pending.Len() == 0 {
		if err := d.loadChunk(); err != nil {
			return 0, err
		}
		if d.done {
			return 0, io.EOF
		}
	}
	return d.pending.Read(p)
}

func (d *chunkDecoder) loadChunk() error {
	line, err := d.br.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("%w: reading chunk header: %v", ErrSignature, err)
	}
	header := string(bytes.TrimRight(line, "\r\n"))
	sizeStr, sig, hasSig := parseChunkHeader(header)
	size, err := parseHexSize(sizeStr)
	if err != nil {
		return fmt.Errorf("%w: bad chunk size %q", ErrSignature, sizeStr)
	}
	if size == 0 {
		d.done = true
		// A trailing signature may follow on signed streams; drain to CRLF.
		_ = d.drainTrailer()
		return nil
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(d.br, buf); err != nil {
		return fmt.Errorf("%w: short chunk read: %v", ErrSignature, err)
	}
	// Consume the CRLF after chunk data.
	crlf := make([]byte, 2)
	if _, err := io.ReadFull(d.br, crlf); err != nil {
		return fmt.Errorf("%w: missing chunk CRLF", ErrSignature)
	}
	if d.signed {
		if !hasSig {
			return fmt.Errorf("%w: signed chunk missing signature", ErrSignature)
		}
		if d.prevSig == "" {
			return fmt.Errorf("%w: chunk signature seed not set", ErrSignature)
		}
		want := d.chunkSignature(buf)
		if !hmacEqual(want, sig) {
			return fmt.Errorf("%w: chunk signature mismatch", ErrSignature)
		}
		d.prevSig = sig
	}
	d.pending.Write(buf)
	return nil
}

func (d *chunkDecoder) drainTrailer() error {
	for {
		line, err := d.br.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if len(bytes.TrimSpace(line)) == 0 {
			return nil
		}
	}
}

// chunkSignature computes:
// HMAC-SHA256(signingKey, "AWS4-HMAC-SHA256-PAYLOAD\n" + date + "\n" +
// scope + "\n" + prevSig + "\n" + sha256("") + "\n" + sha256(chunk))
func (d *chunkDecoder) chunkSignature(chunk []byte) string {
	emptyHash := sha256.Sum256(nil)
	chunkHash := sha256.Sum256(chunk)
	sts := fmt.Sprintf("AWS4-HMAC-SHA256-PAYLOAD\n%s\n%s/%s/s3/aws4_request\n%s\n%s\n%s",
		d.amzDate, d.date, d.region, d.prevSig,
		hex.EncodeToString(emptyHash[:]), hex.EncodeToString(chunkHash[:]))
	kDate := hmacSHA256([]byte("AWS4"+d.secret), d.date)
	kRegion := hmacSHA256(kDate, d.region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	return hex.EncodeToString(hmacSHA256(kSigning, sts))
}

// parseChunkHeader splits "1a2b;chunk-signature=abc" into size and signature.
func parseChunkHeader(h string) (size, sig string, hasSig bool) {
	for i := 0; i < len(h); i++ {
		if h[i] == ';' {
			size = h[:i]
			rest := h[i+1:]
			const prefix = "chunk-signature="
			if len(rest) >= len(prefix) && rest[:len(prefix)] == prefix {
				return size, rest[len(prefix):], true
			}
			return size, "", false
		}
	}
	return h, "", false
}

func parseHexSize(s string) (int, error) {
	s = string(bytes.TrimSpace([]byte(s)))
	if s == "" {
		return 0, errors.New("empty size")
	}
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		var v int
		switch {
		case c >= '0' && c <= '9':
			v = int(c - '0')
		case c >= 'a' && c <= 'f':
			v = int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v = int(c-'A') + 10
		default:
			return 0, fmt.Errorf("invalid hex byte %q", c)
		}
		n = n<<4 + v
	}
	return n, nil
}
