package main

// Minimal SigV4 signer for imctl. It mirrors what the AWS SDK / aws cli do,
// so a successful round trip demonstrates wire-level S3 compatibility.

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
	"time"
)

const sigv4Algorithm = "AWS4-HMAC-SHA256"

type signer struct {
	accessKey string
	secretKey string
	region    string
}

// signRequest adds a fixed-payload SigV4 Authorization header. A nil body is
// hashed as the empty string, matching how the server canonicalises it.
func (s *signer) signRequest(req *http.Request, body []byte) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")

	if body == nil {
		body = []byte{}
	}
	payloadHash := sha256Hex(body)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	signedHeaders, canonicalHeaders := s.canonicalHeaders(req)
	canonicalQuery := canonicalQuery(req.URL.Query())
	// canonicalHeaders already ends in a single trailing newline; do not add
	// another one, or the canonical request diverges from the server's.
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := fmt.Sprintf("%s/%s/s3/aws4_request", date, s.region)
	sts := strings.Join([]string{
		sigv4Algorithm,
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := s.signingKey(date)
	signature := hex.EncodeToString(hmacSHA256(signingKey, sts))
	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		sigv4Algorithm, s.accessKey, scope, signedHeaders, signature))
}

// presign builds a presigned GET URL.
func (s *signer) presign(rawURL string, expires time.Duration) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", date, s.region)

	q := u.Query()
	q.Set("X-Amz-Algorithm", sigv4Algorithm)
	q.Set("X-Amz-Credential", s.accessKey+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", int(expires.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")

	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host}
	// Sign over the canonicalised (sorted, percent-encoded) query, not the
	// raw url.Values encoding — the two differ in escaping rules. The host
	// header block carries exactly one trailing newline.
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(u),
		canonicalQuery(q),
		"host:" + u.Host + "\n",
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")
	sts := strings.Join([]string{sigv4Algorithm, amzDate, scope, sha256Hex([]byte(canonicalRequest))}, "\n")
	sig := hex.EncodeToString(hmacSHA256(s.signingKey(date), sts))
	q.Set("X-Amz-Signature", sig)
	// Use the canonical (RFC3986-escaped) string verbatim. url.Values.Encode
	// would double-encode percent signs (%2F -> %252F) and break the match.
	u.RawQuery = canonicalQuery(q)
	return u.String(), nil
}

func (s *signer) signingKey(date string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+s.secretKey), date)
	kRegion := hmacSHA256(kDate, s.region)
	kService := hmacSHA256(kRegion, "s3")
	return hmacSHA256(kService, "aws4_request")
}

func (s *signer) canonicalHeaders(req *http.Request) (string, string) {
	hdrs := map[string]string{
		"host":                 req.Host,
		"x-amz-content-sha256": req.Header.Get("X-Amz-Content-Sha256"),
		"x-amz-date":           req.Header.Get("X-Amz-Date"),
	}
	keys := make([]string, 0, len(hdrs))
	for k := range hdrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString(":")
		b.WriteString(strings.TrimSpace(hdrs[k]))
		b.WriteString("\n")
	}
	return strings.Join(keys, ";"), strings.TrimSuffix(b.String(), "")
}

func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, awsEscape(k)+"="+awsEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

// awsEscape applies RFC3986 percent-encoding, matching the server's SigV4
// canonical query encoding ("/" becomes %2F, unlike url.QueryEscape).
func awsEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

func canonicalURI(u *url.URL) string {
	p := u.EscapedPath()
	if p == "" {
		return "/"
	}
	return p
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// do executes a signed request with an optional body and returns the response.
func (s *signer) do(method, url string, body []byte) (*http.Response, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		return nil, err
	}
	// NewRequest leaves Host empty; populate it from the URL so the host
	// header used in signing matches what the server canonicalises.
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	s.signRequest(req, body)
	return http.DefaultClient.Do(req)
}
