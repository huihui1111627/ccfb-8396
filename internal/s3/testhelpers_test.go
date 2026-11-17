package s3_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"net/url"
	"testing"
	"time"
)

// newHMAC returns an HMAC-SHA256 hasher seeded with key.
func newHMAC(key []byte) interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
} {
	return hmac.New(sha256.New, key)
}

// presignForTest builds a presigned GET URL. A negative age shifts the
// signing timestamp into the past so the URL is already expired.
func presignForTest(t *testing.T, rawURL string, age time.Duration) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().Add(age).UTC()
	amzDate := when.Format("20060102T150405Z")
	date := when.Format("20060102")
	scope := date + "/us-east-1/s3/aws4_request"
	q := u.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", ak+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", "1")
	q.Set("X-Amz-SignedHeaders", "host")
	canonical := "GET\n" + u.EscapedPath() + "\n" + q.Encode() + "\n" +
		"host:" + u.Host + "\n\n" + "host\nUNSIGNED-PAYLOAD"
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonical))
	key := hmacChain([]byte("AWS4"+sk), date, "us-east-1", "s3", "aws4_request")
	sig := hex2(hmacSHA256(key, sts))
	q.Set("X-Amz-Signature", sig)
	u.RawQuery = q.Encode()
	return u.String()
}

func hex2(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = hexd[c>>4]
		out[i*2+1] = hexd[c&0x0f]
	}
	return string(out)
}
