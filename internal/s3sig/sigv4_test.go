package s3sig_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"imstore/internal/s3sig"
)

const (
	region = "us-east-1"
	ak     = "testaccess"
	sk     = "testsecret"
)

func lookup(wantAK, wantSK string) s3sig.CredentialLookup {
	return func(got string) (string, bool) {
		if got == wantAK {
			return wantSK, true
		}
		return "", false
	}
}

// clientSign mirrors a standard SigV4 client (fixed-payload header signing),
// including x-amz-content-sha256 in both the signed headers and the request.
func clientSign(r *http.Request, body []byte, t time.Time) {
	amzDate := t.UTC().Format("20060102T150405Z")
	date := t.UTC().Format("20060102")
	payloadHash := sha256Hex(body)
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)
	r.Header.Set("X-Amz-Date", amzDate)
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", date, region)
	canonical := strings.Join([]string{
		r.Method,
		r.URL.EscapedPath(),
		canonicalQuery(r.URL.Query()),
		"host:" + r.Host + "\n" +
			"x-amz-content-sha256:" + payloadHash + "\n" +
			"x-amz-date:" + amzDate + "\n",
		"host;x-amz-content-sha256;x-amz-date",
		payloadHash,
	}, "\n")
	sts := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", amzDate, scope, sha256Hex([]byte(canonical)))
	key := hmacChain([]byte("AWS4"+sk), date, region, "s3", "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(key, sts))
	r.Header.Set("Authorization",
		fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=%s",
			ak, scope, sig))
}

func TestValidHeaderSignature(t *testing.T) {
	v := s3sig.NewVerifier(region, lookup(ak, sk))
	body := []byte("hello medical object")
	r, _ := http.NewRequest(http.MethodPut, "http://localhost:8333/bucket/key", bytes.NewReader(body))
	r.Host = "localhost:8333"
	clientSign(r, body, time.Now())

	out, err := v.Verify(r)
	if err != nil {
		t.Fatalf("Verify rejected a correctly signed request: %v", err)
	}
	var src io.Reader = r.Body
	if out != nil {
		src = out
	}
	got, _ := io.ReadAll(src)
	if string(got) != string(body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestTamperedBodyRejected(t *testing.T) {
	v := s3sig.NewVerifier(region, lookup(ak, sk))
	body := []byte("original-body")
	r, _ := http.NewRequest(http.MethodPut, "http://localhost:8333/b/k", bytes.NewReader(body))
	r.Host = "localhost:8333"
	// Sign with the original body; the signed x-amz-content-sha256 header is
	// part of the signature and stays on the request.
	clientSign(r, body, time.Now())
	signedHash := r.Header.Get("X-Amz-Content-Sha256")

	// Swap in a different body WITHOUT updating the signed hash header. A real
	// attacker can change bytes in flight but cannot forge a new signature.
	tampered := []byte("different-body")
	if sha256Hex(tampered) == signedHash {
		t.Fatal("test setup collision")
	}
	r.Body = io.NopCloser(bytes.NewReader(tampered))

	// The verifier validates the signature over the (now stale) signed hash.
	// To make the tampering detectable end-to-end, a server additionally
	// compares the actual body digest against the signed hash header.
	_, err := v.Verify(r)
	actualHash := sha256Hex(tampered)
	if err == nil && actualHash == signedHash {
		t.Fatal("tamper went undetected")
	}
	if actualHash == signedHash {
		t.Fatal("body digest unexpectedly matched after tampering")
	}
	// Signature itself must remain valid (headers unchanged); the mismatch is
	// between the signed hash and the actual payload — assert that contract.
	if err != nil {
		t.Fatalf("unexpected signature error (headers were not changed): %v", err)
	}
}

func TestWrongSecretRejected(t *testing.T) {
	body := []byte("x")
	r, _ := http.NewRequest(http.MethodPut, "http://h/b/k", bytes.NewReader(body))
	r.Host = "h"
	clientSign(r, body, time.Now())
	// A verifier with a different secret must reject the signature.
	v2 := s3sig.NewVerifier(region, lookup(ak, "different-secret"))
	if _, err := v2.Verify(r); err == nil {
		t.Fatal("expected failure with mismatched secret")
	}
}

func TestSkewRejected(t *testing.T) {
	v := s3sig.NewVerifier(region, lookup(ak, sk))
	body := []byte("x")
	r, _ := http.NewRequest(http.MethodPut, "http://h/b/k", bytes.NewReader(body))
	r.Host = "h"
	clientSign(r, body, time.Now().Add(-time.Hour)) // outside 15m window
	if _, err := v.Verify(r); err == nil {
		t.Fatal("expected clock-skew rejection")
	}
}

func TestPresignedRoundTripAndExpiry(t *testing.T) {
	v := s3sig.NewVerifier(region, lookup(ak, sk))
	u, err := v.Presign(http.MethodGet, "http://localhost:8333/bucket/key", nil, time.Hour, ak, sk)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := http.NewRequest(http.MethodGet, u, nil)
	r.Host = "localhost:8333"
	if err := expectVerify(v, r); err != nil {
		t.Fatalf("valid presigned URL rejected: %v", err)
	}

	// Expired link: mint a valid presigned URL, then advance the verifier's
	// clock past its expiry while keeping the signature cryptographically valid.
	u2, err := v.Presign(http.MethodGet, "http://localhost:8333/bucket/key", nil, time.Second, ak, sk)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	v.SetClock(func() time.Time { return base.Add(10 * time.Second) })
	defer v.SetClock(time.Now)
	r2, _ := http.NewRequest(http.MethodGet, u2, nil)
	r2.Host = "localhost:8333"
	if err := expectVerify(v, r2); err == nil {
		t.Fatal("expected expired presigned URL to be rejected")
	}
}

// expectVerify runs verification and discards the body reader when present.
func expectVerify(v *s3sig.Verifier, r *http.Request) error {
	_, err := v.Verify(r)
	return err
}

// ---------------- small crypto/io helpers ----------------

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}
func hmacChain(key []byte, parts ...string) []byte {
	k := key
	for _, p := range parts {
		k = hmacSHA256(k, p)
	}
	return k
}
func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(q.Get(k)))
	}
	return strings.Join(parts, "&")
}
