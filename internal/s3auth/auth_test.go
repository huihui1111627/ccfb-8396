package s3auth_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"erasurestore/internal/s3auth"
)

const (
	testAK     = "AKIDTEST1234567890"
	testSK     = "secretkey-for-sigv4-test"
	testRegion = "us-east-1"
)

// signV4 用与服务端相同的规则构造一个带签名的请求，验证校验器能通过。
func signV4(r *http.Request, ak, sk, region, payloadHash string, t time.Time) {
	amzTime := t.UTC().Format("20060102T150405Z")
	shortDate := amzTime[:8]
	r.Header.Set("X-Amz-Date", amzTime)
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	var canon strings.Builder
	canon.WriteString(r.Method + "\n")
	canon.WriteString(canonicalURI(r.URL.Path) + "\n")
	canon.WriteString(canonicalQuery(r.URL.Query()) + "\n")
	canon.WriteString("host:" + r.Host + "\n")
	canon.WriteString("x-amz-content-sha256:" + payloadHash + "\n")
	canon.WriteString("x-amz-date:" + amzTime + "\n\n")
	canon.WriteString(strings.Join(signedHeaders, ";") + "\n")
	canon.WriteString(payloadHash)

	scope := shortDate + "/" + region + "/s3/aws4_request"
	h := sha256.Sum256([]byte(canon.String()))
	st := "AWS4-HMAC-SHA256\n" + amzTime + "\n" + scope + "\n" + hex.EncodeToString(h[:])

	k := hmacSHA([]byte("AWS4"+sk), []byte(shortDate))
	k = hmacSHA(k, []byte(region))
	k = hmacSHA(k, []byte("s3"))
	k = hmacSHA(k, []byte("aws4_request"))
	sig := hex.EncodeToString(hmacSHA(k, []byte(st)))

	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 "+
		"Credential="+ak+"/"+scope+", "+
		"SignedHeaders="+strings.Join(signedHeaders, ";")+", "+
		"Signature="+sig)
}

func hmacSHA(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func canonicalURI(p string) string { return p }
func canonicalQuery(v url.Values) string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	var parts []string
	for _, k := range keys {
		for _, val := range v[k] {
			parts = append(parts, k+"="+val)
		}
	}
	return strings.Join(parts, "&")
}

func TestValidSignatureAccepted(t *testing.T) {
	v := s3auth.NewVerifier(testAK, testSK, testRegion)
	r, _ := http.NewRequest(http.MethodPut, "http://127.0.0.1:9000/bucket/key", strings.NewReader("data"))
	r.Host = "127.0.0.1:9000"
	signV4(r, testAK, testSK, testRegion, hexSHA256("data"), time.Now())
	if _, err := v.Verify(r); err != nil {
		t.Fatalf("合法签名应通过: %v", err)
	}
}

func TestTamperedSignatureRejected(t *testing.T) {
	v := s3auth.NewVerifier(testAK, testSK, testRegion)
	r, _ := http.NewRequest(http.MethodPut, "http://127.0.0.1:9000/bucket/key", strings.NewReader("data"))
	r.Host = "127.0.0.1:9000"
	signV4(r, testAK, testSK, testRegion, hexSHA256("data"), time.Now())
	r.Header.Set("Authorization", r.Header.Get("Authorization")+"00")
	if _, err := v.Verify(r); err == nil {
		t.Fatal("被篡改的签名必须被拒绝")
	}
}

func TestWrongSecretRejected(t *testing.T) {
	v := s3auth.NewVerifier(testAK, testSK, testRegion)
	r, _ := http.NewRequest(http.MethodGet, "http://h/b/k", nil)
	r.Host = "h"
	signV4(r, testAK, "wrong-secret", testRegion, "UNSIGNED-PAYLOAD", time.Now())
	if _, err := v.Verify(r); err == nil {
		t.Fatal("错误 secret 算出的签名必须被拒绝")
	}
}

func TestExpiredRequestRejected(t *testing.T) {
	v := s3auth.NewVerifier(testAK, testSK, testRegion)
	r, _ := http.NewRequest(http.MethodGet, "http://h/b/k", nil)
	r.Host = "h"
	signV4(r, testAK, testSK, testRegion, "UNSIGNED-PAYLOAD", time.Now().Add(-time.Hour))
	if _, err := v.Verify(r); err == nil {
		t.Fatal("超过 15 分钟偏差的请求必须被拒绝")
	}
}

func hexSHA256(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
