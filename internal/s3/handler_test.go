package s3_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"imstore/internal/erasure"
	"imstore/internal/index"
	"imstore/internal/s3"
	"imstore/internal/store"
	"imstore/internal/volume"
)

const (
	ak = "ak"
	sk = "sk"
)

func newTestServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	root := t.TempDir()
	var vols []*volume.Volume
	for i := 0; i < 6; i++ {
		id := string(rune('a' + i))
		v, err := volume.New(id, filepath.Join(root, id), id)
		if err != nil {
			t.Fatal(err)
		}
		vols = append(vols, v)
	}
	vm := volume.NewManager(vols)
	idx, err := index.Open(filepath.Join(root, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	codec, err := erasure.New(4, 2, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(codec, vm, idx)
	h := s3.NewHandler(st, "us-east-1", ak, sk)
	srv := httptest.NewServer(h)
	return srv, func() { srv.Close(); idx.Close() }
}

func sha256Hex(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// sign applies a fixed-payload SigV4 header signature like a real client.
func sign(req *http.Request, body []byte) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	payloadHash := sha256Hex(body)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	scope := date + "/us-east-1/s3/aws4_request"
	canonical := req.Method + "\n" + req.URL.EscapedPath() + "\n" +
		canonicalSortedQuery(req.URL.Query()) + "\n" +
		"host:" + req.Host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n\n" +
		"host;x-amz-content-sha256;x-amz-date\n" + payloadHash
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonical))
	k := hmacChain([]byte("AWS4"+sk), date, "us-east-1", "s3", "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(k, sts))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+ak+"/"+scope+
		", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="+sig)
}

func canonicalSortedQuery(q url.Values) string {
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

func hmacSHA256(key []byte, data string) []byte {
	m := newHMAC(key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func hmacChain(key []byte, parts ...string) []byte {
	for _, p := range parts {
		m := newHMAC(key)
		m.Write([]byte(p))
		key = m.Sum(nil)
	}
	return key
}

func TestBucketAndObjectLifecycle(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	// Create bucket.
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/images", nil)
	req.Host = srv.URL[len("http://"):]
	sign(req, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Unsigned request must be rejected.
	bad, _ := http.NewRequest(http.MethodGet, srv.URL+"/images", nil)
	resp2, err := http.DefaultClient.Do(bad)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("unsigned request status = %d, want 403", resp2.StatusCode)
	}
	resp2.Body.Close()

	// Put object.
	payload := bytes.Repeat([]byte("medical-imaging"), 5000)
	preq, _ := http.NewRequest(http.MethodPut, srv.URL+"/images/ct/scan1.dcm", bytes.NewReader(payload))
	preq.Host = req.Host
	sign(preq, payload)
	presp, err := http.DefaultClient.Do(preq)
	if err != nil {
		t.Fatal(err)
	}
	if presp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(presp.Body)
		t.Fatalf("put status = %d: %s", presp.StatusCode, b)
	}
	presp.Body.Close()

	// Get object and compare.
	greq, _ := http.NewRequest(http.MethodGet, srv.URL+"/images/ct/scan1.dcm", nil)
	greq.Host = req.Host
	sign(greq, nil)
	gresp, err := http.DefaultClient.Do(greq)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(gresp.Body)
	gresp.Body.Close()
	if gresp.StatusCode != http.StatusOK || !bytes.Equal(got, payload) {
		t.Fatalf("get mismatch status=%d len=%d want=%d", gresp.StatusCode, len(got), len(payload))
	}

	// Range request.
	rreq, _ := http.NewRequest(http.MethodGet, srv.URL+"/images/ct/scan1.dcm", nil)
	rreq.Host = req.Host
	rreq.Header.Set("Range", "bytes=100-199")
	sign(rreq, nil)
	rresp, err := http.DefaultClient.Do(rreq)
	if err != nil {
		t.Fatal(err)
	}
	rb, _ := io.ReadAll(rresp.Body)
	rresp.Body.Close()
	if rresp.StatusCode != http.StatusPartialContent || !bytes.Equal(rb, payload[100:200]) {
		t.Fatalf("range mismatch status=%d len=%d", rresp.StatusCode, len(rb))
	}

	// List with prefix.
	lreq, _ := http.NewRequest(http.MethodGet, srv.URL+"/images?list-type=2&prefix=ct/", nil)
	lreq.Host = req.Host
	sign(lreq, nil)
	lresp, err := http.DefaultClient.Do(lreq)
	if err != nil {
		t.Fatal(err)
	}
	lbody, _ := io.ReadAll(lresp.Body)
	lresp.Body.Close()
	if lresp.StatusCode != http.StatusOK || !bytes.Contains(lbody, []byte("scan1.dcm")) {
		t.Fatalf("list failed: %s", lbody)
	}

	// Delete.
	dreq, _ := http.NewRequest(http.MethodDelete, srv.URL+"/images/ct/scan1.dcm", nil)
	dreq.Host = req.Host
	sign(dreq, nil)
	dresp, err := http.DefaultClient.Do(dreq)
	if err != nil {
		t.Fatal(err)
	}
	if dresp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d", dresp.StatusCode)
	}
	dresp.Body.Close()
}

func TestTamperedBodyRejectedOverHTTP(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	// Bucket first.
	breq, _ := http.NewRequest(http.MethodPut, srv.URL+"/b", nil)
	breq.Host = hostOf(srv.URL)
	sign(breq, nil)
	br, _ := http.DefaultClient.Do(breq)
	br.Body.Close()

	original := []byte("the patient record contents")
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/b/key", bytes.NewReader(original))
	req.Host = hostOf(srv.URL)
	sign(req, original)
	// Tamper after signing while keeping the SAME byte length. Go's transport
	// sends exactly ContentLength bytes, so a same-length replacement ensures
	// the server actually receives different bytes while the signed hash
	// header still describes the original payload.
	tampered := []byte("THE PATIENT RECORD CONTENTS")
	if len(tampered) != len(original) || bytes.Equal(tampered, original) {
		t.Fatal("test setup: tampered payload must be same length and different")
	}
	req.Body = io.NopCloser(bytes.NewReader(tampered))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("tampered body status = %d, want 403: %s", resp.StatusCode, body)
	}
}

func TestPresignedURLExpiry(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	breq, _ := http.NewRequest(http.MethodPut, srv.URL+"/b", nil)
	breq.Host = hostOf(srv.URL)
	sign(breq, nil)
	br, _ := http.DefaultClient.Do(breq)
	br.Body.Close()

	payload := []byte("presigned download")
	preq, _ := http.NewRequest(http.MethodPut, srv.URL+"/b/f", bytes.NewReader(payload))
	preq.Host = hostOf(srv.URL)
	sign(preq, payload)
	pr, _ := http.DefaultClient.Do(preq)
	pr.Body.Close()

	// Build a presigned URL with a 1-second expiry via the verifier's signer.
	// We re-create the signing here using the same algorithm as imctl.
	u := presignForTest(t, srv.URL+"/b/f", -2*time.Second) // already-expired
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	req.Host = hostOf(srv.URL)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expired presigned GET status = %d, want 403", resp.StatusCode)
	}
}

func hostOf(raw string) string {
	return raw[len("http://"):]
}
