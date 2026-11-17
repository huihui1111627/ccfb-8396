// Package s3auth 实现 AWS Signature V4 校验，支持普通请求与预签名 URL。
package s3auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	amzDate    = "X-Amz-Date"
	amzAlg     = "X-Amz-Algorithm"
	amzCred    = "X-Amz-Credential"
	amzSigned  = "X-Amz-SignedHeaders"
	amzSign    = "X-Amz-Signature"
	amzExpires = "X-Amz-Expires"
	amzContent = "X-Amz-Content-Sha256"
	sigV4Alg   = "AWS4-HMAC-SHA256"
)

// AmzContentSha256 返回内容哈希头名。
func AmzContentSha256() string { return amzContent }

// ErrSignature 签名校验失败的统一错误。
var ErrSignature = errors.New("签名校验失败")

// Credential 访问密钥。
type Credential struct {
	AccessKey string
	SecretKey string
	Region    string
}

// Verifier SigV4 校验器。
type Verifier struct {
	cred Credential
	now  func() time.Time
}

// NewVerifier 创建校验器。
func NewVerifier(accessKey, secretKey, region string) *Verifier {
	return &Verifier{cred: Credential{AccessKey: accessKey, SecretKey: secretKey, Region: region}, now: time.Now}
}

// IsPresigned 判断是否为预签名 URL 请求。
func IsPresigned(r *http.Request) bool {
	return r.URL.Query().Get(amzAlg) != ""
}

// Verify 校验请求签名，返回内容哈希声明值（普通请求）。
// 预签名请求不返回内容哈希。调用方应先用 DecodeBody 处理请求体。
func (v *Verifier) Verify(r *http.Request) (string, error) {
	if IsPresigned(r) {
		return "", v.verifyPresigned(r)
	}
	return "", v.verifyHeader(r)
}

func (v *Verifier) verifyHeader(r *http.Request) error {
	if r.Header.Get("Authorization") == "" {
		return fmt.Errorf("%w: 缺少 Authorization 头", ErrSignature)
	}
	auth := parseAuthHeader(r.Header.Get("Authorization"))
	if auth.algorithm != sigV4Alg {
		return fmt.Errorf("%w: 不支持的算法 %s", ErrSignature, auth.algorithm)
	}
	access, date, region, service, err := parseCredential(auth.credential)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSignature, err)
	}
	if access != v.cred.AccessKey {
		return fmt.Errorf("%w: AccessKey 不匹配", ErrSignature)
	}
	if err := v.validateScope(region, service); err != nil {
		return err
	}
	if err := v.validateDate(r.Header.Get(amzDate), date, 15*time.Minute); err != nil {
		return err
	}

	contentHash := r.Header.Get(amzContent)
	if contentHash == "" {
		return fmt.Errorf("%w: 缺少 %s 头", ErrSignature, amzContent)
	}
	signedHeaders := strings.Split(auth.signedHeaders, ";")
	cr := canonicalRequest(r, signedHeaders, contentHash, false)
	st := stringToSign(r.Header.Get(amzDate), scope(date, v.cred.Region, "s3"), cr)
	sig := v.signature(date, signedBytes(st))
	if !hmac.Equal([]byte(sig), []byte(auth.signature)) {
		return fmt.Errorf("%w: 签名不一致", ErrSignature)
	}
	return nil
}

func (v *Verifier) verifyPresigned(r *http.Request) error {
	q := r.URL.Query()
	if q.Get(amzAlg) != sigV4Alg {
		return fmt.Errorf("%w: 预签名算法不支持", ErrSignature)
	}
	access, date, region, service, err := parseCredential(q.Get(amzCred))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSignature, err)
	}
	if access != v.cred.AccessKey {
		return fmt.Errorf("%w: AccessKey 不匹配", ErrSignature)
	}
	if err := v.validateScope(region, service); err != nil {
		return err
	}
	amzTime := q.Get(amzDate)
	if err := v.validateDate(amzTime, date, 7*24*time.Hour); err != nil {
		return err
	}
	expires, err := parseExpires(q.Get(amzExpires))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSignature, err)
	}
	t, err := time.Parse("20060102T150405Z", amzTime)
	if err != nil {
		return fmt.Errorf("%w: 时间格式错误", ErrSignature)
	}
	if v.now().Sub(t) > expires {
		return fmt.Errorf("%w: 预签名链接已过期", ErrSignature)
	}

	signedHeaders := strings.Split(q.Get(amzSigned), ";")
	// 预签名的 content hash 固定为 UNSIGNED-PAYLOAD
	cr := canonicalRequest(r, signedHeaders, "UNSIGNED-PAYLOAD", true)
	st := stringToSign(amzTime, scope(date, v.cred.Region, "s3"), cr)
	sig := v.signature(date, signedBytes(st))
	if !hmac.Equal([]byte(sig), []byte(q.Get(amzSign))) {
		return fmt.Errorf("%w: 预签名不一致", ErrSignature)
	}
	return nil
}

func (v *Verifier) validateScope(region, service string) error {
	if region != v.cred.Region {
		return fmt.Errorf("%w: region 不匹配: %s != %s", ErrSignature, region, v.cred.Region)
	}
	if service != "s3" {
		return fmt.Errorf("%w: service 必须为 s3", ErrSignature)
	}
	return nil
}

func (v *Verifier) validateDate(amzTime, shortDate string, maxSkew time.Duration) error {
	t, err := time.Parse("20060102T150405Z", amzTime)
	if err != nil {
		return fmt.Errorf("%w: X-Amz-Date 格式错误", ErrSignature)
	}
	if d := t.Format("20060102"); d != shortDate {
		return fmt.Errorf("%w: 凭证日期与请求日期不一致", ErrSignature)
	}
	if diff := v.now().Sub(t); diff > maxSkew || diff < -maxSkew {
		return fmt.Errorf("%w: 请求时间超出允许偏差", ErrSignature)
	}
	return nil
}

// authParts 解析后的 Authorization 头。
type authParts struct {
	algorithm     string
	credential    string
	signedHeaders string
	signature     string
}

func parseAuthHeader(h string) authParts {
	p := authParts{}
	idx := strings.Index(h, " ")
	if idx < 0 {
		return p
	}
	p.algorithm = h[:idx]
	for _, part := range strings.Split(h[idx+1:], ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "Credential="):
			p.credential = strings.TrimPrefix(part, "Credential=")
		case strings.HasPrefix(part, "SignedHeaders="):
			p.signedHeaders = strings.TrimPrefix(part, "SignedHeaders=")
		case strings.HasPrefix(part, "Signature="):
			p.signature = strings.TrimPrefix(part, "Signature=")
		}
	}
	return p
}

// parseCredential 解析 AccessKey/date/region/service/request。
func parseCredential(c string) (access, date, region, service string, err error) {
	parts := strings.Split(c, "/")
	if len(parts) != 5 || parts[4] != "aws4_request" {
		return "", "", "", "", fmt.Errorf("凭证格式错误: %s", c)
	}
	return parts[0], parts[1], parts[2], parts[3], nil
}

func parseExpires(s string) (time.Duration, error) {
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, err
	}
	if n <= 0 || n > 7*24*3600 {
		return 0, fmt.Errorf("X-Amz-Expires 越界: %d", n)
	}
	return time.Duration(n) * time.Second, nil
}

// canonicalRequest 构造 SigV4 规范请求。
func canonicalRequest(r *http.Request, signedHeaders []string, contentHash string, presigned bool) string {
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteString("\n")
	b.WriteString(canonicalURI(r.URL.Path))
	b.WriteString("\n")
	b.WriteString(canonicalQuery(r, presigned))
	b.WriteString("\n")
	for _, h := range signedHeaders {
		b.WriteString(strings.ToLower(strings.TrimSpace(h)))
		b.WriteString(":")
		b.WriteString(canonicalHeaderValue(r, h))
		b.WriteString("\n")
	}
	b.WriteString("\n")
	b.WriteString(strings.Join(signedHeaders, ";"))
	b.WriteString("\n")
	b.WriteString(contentHash)
	return b.String()
}

func canonicalHeaderValue(r *http.Request, name string) string {
	name = strings.ToLower(name)
	if name == "host" {
		return strings.TrimSpace(r.Host)
	}
	return strings.TrimSpace(r.Header.Get(name))
}

// canonicalURI 路径按 RFC3986 编码（保留 /）。
func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	segs := strings.Split(path, "/")
	for i, s := range segs {
		segs[i] = awsURIEncode(s)
	}
	return strings.Join(segs, "/")
}

// canonicalQuery 参数按 key 排序后 RFC3986 编码。
func canonicalQuery(r *http.Request, presigned bool) string {
	values := r.URL.Query()
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if presigned && k == amzSign {
			continue // 签名本身不参与签名
		}
		for _, v := range values[k] {
			parts = append(parts, awsURIEncode(k)+"="+awsURIEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

// awsURIEncode AWS 风格 URI 编码：字母数字及 -_.~ 不编码。
func awsURIEncode(s string) string {
	const hexChars = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hexChars[c>>4])
			b.WriteByte(hexChars[c&15])
		}
	}
	return b.String()
}

func scope(shortDate, region, service string) string {
	return fmt.Sprintf("%s/%s/%s/aws4_request", shortDate, region, service)
}

func stringToSign(amzTime, credScope, canonical string) string {
	h := sha256.Sum256([]byte(canonical))
	return fmt.Sprintf("%s\n%s\n%s\n%s", sigV4Alg, amzTime, credScope, hex.EncodeToString(h[:]))
}

func signedBytes(s string) []byte { return []byte(s) }

// signature 派生签名密钥并计算 HMAC-SHA256。
func (v *Verifier) signature(shortDate string, st []byte) string {
	kDate := hmacSHA256([]byte("AWS4"+v.cred.SecretKey), []byte(shortDate))
	kRegion := hmacSHA256(kDate, []byte(v.cred.Region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return hex.EncodeToString(hmacSHA256(kSigning, st))
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}
