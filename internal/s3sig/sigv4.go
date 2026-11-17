// Package s3sig implements server-side validation of AWS Signature V4 as
// used by S3, including:
//
//   - Authorization header signing (with optional streaming chunked bodies)
//   - presigned-URL (query parameter) signing
//   - UNSIGNED-PAYLOAD and STREAMING-AWS4-HMAC-SHA256-PAYLOAD modes
//
// It is intentionally self-contained (no AWS SDK) so the validation logic is
// easy to audit, while remaining wire-compatible with standard S3 clients.
package s3sig

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Streaming payload modes advertised by clients.
const (
	streamingSigned   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingUnsigned = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	unsignedPayload   = "UNSIGNED-PAYLOAD"
	emptyPayloadHash  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// ErrSignature covers all authentication failures (deliberately generic in
// its wrapping so callers can return S3 SignatureDoesNotMatch).
var ErrSignature = errors.New("s3sig: signature validation failed")

// CredentialLookup returns the secret key for an access key id.
type CredentialLookup func(accessKey string) (secret string, ok bool)

// Verifier validates requests against one region and credential store.
type Verifier struct {
	region    string
	lookup    CredentialLookup
	clockSkew time.Duration
	now       func() time.Time
}

// NewVerifier builds a verifier. The default clock skew tolerance is 15m.
func NewVerifier(region string, lookup CredentialLookup) *Verifier {
	return &Verifier{region: region, lookup: lookup, clockSkew: 15 * time.Minute, now: time.Now}
}

// SetClock overrides the time source. Intended for expiry/skew testing.
func (v *Verifier) SetClock(now func() time.Time) { v.now = now }

// parsedAuth holds the components of an Authorization header.
type parsedAuth struct {
	algorithm  string
	accessKey  string
	date       string // yyyyMMdd
	region     string
	service    string
	signedHdrs []string
	signature  string
}

// Verify authenticates r. When the body is streaming-chunk signed, the
// returned io.Reader is a decoder that validates each chunk's signature as it
// is read; callers MUST consume it instead of r.Body directly.
func (v *Verifier) Verify(r *http.Request) (io.Reader, error) {
	if q := r.URL.Query(); q.Get("X-Amz-Algorithm") != "" {
		return nil, v.verifyPresigned(r, q)
	}
	return v.verifyHeader(r)
}

func (v *Verifier) verifyHeader(r *http.Request) (io.Reader, error) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return nil, fmt.Errorf("%w: missing Authorization header", ErrSignature)
	}
	p, err := parseAuthorization(auth)
	if err != nil {
		return nil, err
	}
	secret, ok := v.lookup(p.accessKey)
	if !ok {
		return nil, fmt.Errorf("%w: unknown access key", ErrSignature)
	}
	if p.region != v.region || p.service != "s3" {
		return nil, fmt.Errorf("%w: wrong region/service scope", ErrSignature)
	}
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return nil, fmt.Errorf("%w: missing X-Amz-Date", ErrSignature)
	}
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return nil, fmt.Errorf("%w: bad X-Amz-Date", ErrSignature)
	}
	if skew := v.now().Sub(t); skew > v.clockSkew || skew < -v.clockSkew {
		return nil, fmt.Errorf("%w: request date outside clock skew window", ErrSignature)
	}

	// Decide which payload hash mode applies.
	contentHash := r.Header.Get("X-Amz-Content-Sha256")
	mode := modeFromHash(contentHash)

	var body io.Reader = r.Body
	switch mode {
	case streamingSigned, streamingUnsigned:
		// Canonical request is signed with the trailer/placeholder hash.
		canonical := canonicalRequest(r, p.signedHdrs, contentHash)
		sts := stringToSign(t, p.date, v.region, canonical)
		sig := v.signature(secret, p.date, v.region, sts)
		if !hmacEqual(sig, p.signature) {
			return nil, fmt.Errorf("%w: streaming header signature mismatch", ErrSignature)
		}
		dec := newChunkDecoder(r.Body, secret, p.date, amzDate, v.region, mode == streamingSigned)
		dec.SetSeedSignature(p.signature)
		return dec, nil
	case unsignedPayload:
		canonical := canonicalRequest(r, p.signedHdrs, unsignedPayload)
		sts := stringToSign(t, p.date, v.region, canonical)
		sig := v.signature(secret, p.date, v.region, sts)
		if !hmacEqual(sig, p.signature) {
			return nil, fmt.Errorf("%w: unsigned-payload signature mismatch", ErrSignature)
		}
		return body, nil
	default:
		// Plain fixed-payload SHA-256 (or empty body hash).
		hash := contentHash
		if hash == "" {
			if r.Body == nil {
				hash = emptyPayloadHash
			} else {
				return nil, fmt.Errorf("%w: missing x-amz-content-sha256", ErrSignature)
			}
		}
		canonical := canonicalRequest(r, p.signedHdrs, hash)
		sts := stringToSign(t, p.date, v.region, canonical)
		sig := v.signature(secret, p.date, v.region, sts)
		if !hmacEqual(sig, p.signature) {
			return nil, fmt.Errorf("%w: signature mismatch", ErrSignature)
		}
		return body, nil
	}
}

func (v *Verifier) verifyPresigned(r *http.Request, q url.Values) error {
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		return fmt.Errorf("%w: unsupported presigned algorithm", ErrSignature)
	}
	cred := q.Get("X-Amz-Credential")
	parts := strings.Split(cred, "/")
	if len(parts) != 5 {
		return fmt.Errorf("%w: malformed credential", ErrSignature)
	}
	accessKey, date, region, service := parts[0], parts[1], parts[2], parts[3]
	secret, ok := v.lookup(accessKey)
	if !ok {
		return fmt.Errorf("%w: unknown access key", ErrSignature)
	}
	if region != v.region || service != "s3" {
		return fmt.Errorf("%w: wrong region/service scope", ErrSignature)
	}
	t, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	if err != nil {
		return fmt.Errorf("%w: bad X-Amz-Date", ErrSignature)
	}
	expires, err := time.ParseDuration(q.Get("X-Amz-Expires") + "s")
	if err != nil || expires <= 0 {
		return fmt.Errorf("%w: bad X-Amz-Expires", ErrSignature)
	}
	now := v.now()
	// Clock skew only tolerates a request whose signing timestamp is slightly
	// in the future; it must never extend an already-expired URL's lifetime.
	if now.Before(t.Add(-v.clockSkew)) {
		return fmt.Errorf("%w: presigned URL signed with a future timestamp", ErrSignature)
	}
	if now.Sub(t) > expires {
		return fmt.Errorf("%w: presigned URL expired", ErrSignature)
	}
	signedHdrs := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")

	// Rebuild the canonical query from the RAW (still percent-encoded) query
	// string. Using parsed url.Values would decode %2F back to "/", changing
	// the bytes the client signed. Parse each pair without unescaping values.
	canonical := canonicalRequestWithQuery(r, signedHdrs, "UNSIGNED-PAYLOAD",
		canonicalRawQuery(r.URL.RawQuery))
	sts := stringToSign(t, date, v.region, canonical)
	sig := v.signature(secret, date, v.region, sts)
	if !hmacEqual(sig, q.Get("X-Amz-Signature")) {
		return fmt.Errorf("%w: presigned signature mismatch", ErrSignature)
	}
	return nil
}

// Presign builds a presigned URL for an outgoing request (used by imctl and
// the console's "share link" feature).
func (v *Verifier) Presign(method, rawURL string, headers http.Header, expires time.Duration, accessKey, secret string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	now := v.now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	cred := fmt.Sprintf("%s/%s/%s/s3/aws4_request", accessKey, date, v.region)

	signedHdrs := []string{"host"}
	q := u.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", cred)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", int(expires.Seconds())))
	q.Set("X-Amz-SignedHeaders", strings.Join(signedHdrs, ";"))

	synth := &http.Request{Method: method, URL: u, Header: headers, Host: u.Host}
	canonical := canonicalRequestWithQuery(synth, signedHdrs, "UNSIGNED-PAYLOAD", encodeQuery(q))
	sts := stringToSign(now, date, v.region, canonical)
	sig := v.signature(secret, date, v.region, sts)
	q.Set("X-Amz-Signature", sig)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// ---------------- canonical request helpers ----------------

func canonicalRequest(r *http.Request, signedHdrs []string, payloadHash string) string {
	return canonicalRequestWithQuery(r, signedHdrs, payloadHash, encodeQuery(r.URL.Query()))
}

func canonicalRequestWithQuery(r *http.Request, signedHdrs []string, payloadHash, canonicalQuery string) string {
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteString("\n")
	b.WriteString(canonicalURI(r.URL.EscapedPath()))
	b.WriteString("\n")
	b.WriteString(canonicalQuery)
	b.WriteString("\n")
	b.WriteString(canonicalHeaders(r, signedHdrs))
	b.WriteString("\n")
	b.WriteString(strings.Join(signedHdrs, ";"))
	b.WriteString("\n")
	b.WriteString(payloadHash)
	return b.String()
}

// canonicalURI keeps the already-escaped path, collapsing an empty path to /.
func canonicalURI(escapedPath string) string {
	if escapedPath == "" {
		return "/"
	}
	return escapedPath
}

// canonicalRawQuery normalises an already-encoded query string for SigV4:
// pairs are sorted by (key, value) and the X-Amz-Signature parameter dropped.
// Existing percent-encoding is preserved byte-for-byte, which is essential
// because the client signed those exact encoded bytes.
func canonicalRawQuery(raw string) string {
	if raw == "" {
		return ""
	}
	type pair struct{ k, v string }
	var pairs []pair
	for _, piece := range strings.Split(raw, "&") {
		kv := strings.SplitN(piece, "=", 2)
		if len(kv) != 2 || kv[0] == "X-Amz-Signature" {
			continue
		}
		pairs = append(pairs, pair{kv[0], kv[1]})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, p.k+"="+p.v)
	}
	return strings.Join(out, "&")
}

// encodeQuery sorts and URI-encodes query parameters per SigV4 rules. AWS
// uses RFC3986 (unreserved set only), so "/" in a value is encoded as %2F;
// Go's url.QueryEscape leaves "/" alone and must not be used here.
func encodeQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		ek := awsEscape(k)
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		for _, val := range vals {
			parts = append(parts, ek+"="+awsEscape(val))
		}
	}
	return strings.Join(parts, "&")
}

// awsEscape applies RFC3986 percent-encoding (unreserved chars unescaped).
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

func canonicalHeaders(r *http.Request, signedHdrs []string) string {
	var b strings.Builder
	hdrs := append([]string(nil), signedHdrs...)
	sort.Strings(hdrs)
	for _, h := range hdrs {
		var val string
		switch h {
		case "host":
			val = r.Host
			if val == "" {
				val = r.URL.Host
			}
		default:
			// Signed header names are lowercase; http.Header stores them under
			// canonical MIME casing, so look both up.
			if vv := r.Header.Values(http.CanonicalHeaderKey(h)); len(vv) > 0 {
				val = strings.Join(vv, ",")
			} else {
				val = strings.Join(r.Header[h], ",")
			}
		}
		b.WriteString(strings.ToLower(h))
		b.WriteString(":")
		b.WriteString(strings.TrimSpace(compactSpaces(val)))
		b.WriteString("\n")
	}
	return b.String()
}

func compactSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func stringToSign(t time.Time, date, region, canonical string) string {
	h := sha256.Sum256([]byte(canonical))
	return fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s/%s/s3/aws4_request\n%s",
		t.UTC().Format("20060102T150405Z"), date, region, hex.EncodeToString(h[:]))
}

func (v *Verifier) signature(secret, date, region, sts string) string {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	return hex.EncodeToString(hmacSHA256(kSigning, sts))
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func hmacEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func modeFromHash(hash string) string {
	switch hash {
	case streamingSigned, streamingUnsigned, unsignedPayload:
		return hash
	default:
		return "fixed"
	}
}

func parseAuthorization(auth string) (*parsedAuth, error) {
	// Format: AWS4-HMAC-SHA256 Credential=...,SignedHeaders=...,Signature=...
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || parts[0] != "AWS4-HMAC-SHA256" {
		return nil, fmt.Errorf("%w: malformed Authorization", ErrSignature)
	}
	p := &parsedAuth{algorithm: parts[0]}
	for _, kv := range strings.Split(parts[1], ",") {
		kv = strings.TrimSpace(kv)
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			return nil, fmt.Errorf("%w: malformed Authorization component", ErrSignature)
		}
		key, val := kv[:eq], kv[eq+1:]
		switch key {
		case "Credential":
			c := strings.Split(val, "/")
			if len(c) != 5 {
				return nil, fmt.Errorf("%w: malformed Credential", ErrSignature)
			}
			p.accessKey, p.date, p.region, p.service = c[0], c[1], c[2], c[3]
		case "SignedHeaders":
			p.signedHdrs = strings.Split(val, ";")
		case "Signature":
			p.signature = val
		}
	}
	if p.accessKey == "" || p.signature == "" || len(p.signedHdrs) == 0 {
		return nil, fmt.Errorf("%w: incomplete Authorization", ErrSignature)
	}
	return p, nil
}
