package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"erasurestore/internal/config"
	"erasurestore/internal/meta"
	"erasurestore/internal/s3auth"
	"erasurestore/internal/storage"
)

// Server S3 协议服务。
type Server struct {
	store    *storage.Store
	verifier *s3auth.Verifier
	cfg      *config.Config
}

// NewServer 创建 S3 服务。
func NewServer(st *storage.Store, cfg *config.Config) *Server {
	return &Server{
		store:    st,
		verifier: s3auth.NewVerifier(cfg.AccessKey, cfg.SecretKey, cfg.Region),
		cfg:      cfg,
	}
}

// Handler 返回带签名中间件的 http.Handler。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.route)
	return s.authMiddleware(mux)
}

// route 解析 path-style 请求：/  /bucket  /bucket/key。
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	var bucket, key string
	if path != "" {
		parts := strings.SplitN(path, "/", 2)
		bucket = parts[0]
		if len(parts) == 2 {
			key = parts[1]
		}
	}
	q := r.URL.Query()

	switch {
	// ---- 服务级 ----
	case bucket == "":
		if r.Method == http.MethodGet {
			s.listBuckets(w, r)
			return
		}
		writeError(w, r, errInvalidRequest, "不支持的服务级操作")
		return

	// ---- 桶级 ----
	case key == "":
		s.routeBucket(w, r, bucket, q)
		return

	// ---- 对象级 ----
	default:
		s.routeObject(w, r, bucket, key, q)
		return
	}
}

func (s *Server) routeBucket(w http.ResponseWriter, r *http.Request, bucket string, q map[string][]string) {
	switch {
	case has(q, "lifecycle"):
		switch r.Method {
		case http.MethodGet:
			s.getLifecycle(w, r, bucket)
		case http.MethodPut:
			s.putLifecycle(w, r, bucket)
		case http.MethodDelete:
			s.deleteLifecycle(w, r, bucket)
		default:
			writeError(w, r, errInvalidRequest, "method not allowed")
		}
	case has(q, "uploads") && r.Method == http.MethodGet:
		s.listMultipartUploads(w, r, bucket)
	case r.Method == http.MethodPut:
		s.createBucket(w, r, bucket)
	case r.Method == http.MethodGet:
		s.listObjects(w, r, bucket, q)
	case r.Method == http.MethodHead:
		s.headBucket(w, r, bucket)
	case r.Method == http.MethodDelete:
		s.deleteBucket(w, r, bucket)
	default:
		writeError(w, r, errInvalidRequest, "method not allowed")
	}
}

func (s *Server) routeObject(w http.ResponseWriter, r *http.Request, bucket, key string, q map[string][]string) {
	switch {
	case has(q, "uploads") && r.Method == http.MethodPost:
		s.initiateMPU(w, r, bucket, key)
	case has(q, "uploadId") && r.Method == http.MethodPost:
		if has(q, "abort") {
			s.abortMPU(w, r, bucket, key, first(q, "uploadId"))
		} else {
			s.completeMPU(w, r, bucket, key, first(q, "uploadId"))
		}
	case has(q, "uploadId") && r.Method == http.MethodGet:
		s.listParts(w, r, bucket, key, first(q, "uploadId"))
	case has(q, "partNumber") && has(q, "uploadId") && r.Method == http.MethodPut:
		s.uploadPart(w, r, bucket, key, first(q, "uploadId"))
	case r.Method == http.MethodPut:
		s.putObject(w, r, bucket, key)
	case r.Method == http.MethodGet:
		s.getObject(w, r, bucket, key)
	case r.Method == http.MethodHead:
		s.headObject(w, r, bucket, key)
	case r.Method == http.MethodDelete:
		if has(q, "delete") {
			s.deleteObjects(w, r, bucket)
		} else {
			s.deleteObject(w, r, bucket, key)
		}
	default:
		writeError(w, r, errInvalidRequest, "method not allowed")
	}
}

// ---- 中间件 ----

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		declared := r.Header.Get(s3auth.AmzContentSha256())
		// aws-chunked / 普通哈希：先处理请求体
		body, err := s3auth.DecodeBody(r, declared)
		if err != nil {
			writeError(w, r, errSignatureProblem, err.Error())
			return
		}
		r.Body = body
		if _, err := s.verifier.Verify(r); err != nil {
			if errors.Is(err, s3auth.ErrSignature) {
				writeError(w, r, errSignatureProblem, err.Error())
				return
			}
			writeError(w, r, errAccessDenied, err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---- 桶操作 ----

func (s *Server) createBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !validBucketName(bucket) {
		writeError(w, r, errInvalidBucketName, "")
		return
	}
	if err := s.store.Meta().CreateBucket(bucket); err != nil {
		if strings.Contains(err.Error(), "已存在") {
			writeError(w, r, errBucketAlreadyExists, err.Error())
			return
		}
		writeError(w, r, errInternal, err.Error())
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) headBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	ok, err := s.store.Meta().BucketExists(bucket)
	if err != nil {
		writeError(w, r, errInternal, err.Error())
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.store.Meta().DeleteBucket(bucket); err != nil {
		if strings.Contains(err.Error(), "不存在") {
			writeError(w, r, errNoSuchBucket, err.Error())
			return
		}
		if strings.Contains(err.Error(), "非空") {
			writeError(w, r, errBucketNotEmpty, err.Error())
			return
		}
		writeError(w, r, errInternal, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.store.Meta().ListBuckets()
	if err != nil {
		writeError(w, r, errInternal, err.Error())
		return
	}
	result := listAllMyBucketsResult{
		Xmlns: s3XMLNS,
		Owner: owner{ID: "erasurestore", DisplayName: "erasurestore"},
	}
	for _, b := range buckets {
		result.Buckets.Bucket = append(result.Buckets.Bucket, listBucketEntry{
			Name: b.Name, CreationDate: isoTime(b.CreatedAt),
		})
	}
	writeXML(w, http.StatusOK, result)
}

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request, bucket string, q map[string][]string) {
	ok, err := s.store.Meta().BucketExists(bucket)
	if err != nil {
		writeError(w, r, errInternal, err.Error())
		return
	}
	if !ok {
		writeError(w, r, errNoSuchBucket, "")
		return
	}
	prefix := first(q, "prefix")
	delimiter := first(q, "delimiter")
	maxKeys, _ := strconv.Atoi(first(q, "max-keys"))
	// V2 用 continuation-token；V1 用 marker
	token := first(q, "continuation-token")
	if token == "" {
		token = first(q, "marker")
	}
	res, err := s.store.Meta().ListObjects(bucket, prefix, delimiter, token, maxKeys)
	if err != nil {
		writeError(w, r, errInternal, err.Error())
		return
	}
	out := listBucketResult{
		Xmlns:          s3XMLNS,
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		MaxKeys:        maxKeys,
		IsTruncated:    res.IsTruncated,
		NextToken:      res.NextToken,
		NextStartAfter: res.NextToken,
	}
	for _, e := range res.Entries {
		out.Contents = append(out.Contents, objectEntry{
			Key: e.Key, LastModified: isoTime(e.ModifiedAt),
			ETag: quoteETag(e.ETag), Size: e.Size, StorageClass: "STANDARD",
		})
	}
	for _, cp := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: cp})
	}
	out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
	writeXML(w, http.StatusOK, out)
}

// ---- 对象操作 ----

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ok, err := s.store.Meta().BucketExists(bucket)
	if err != nil || !ok {
		writeError(w, r, errNoSuchBucket, "")
		return
	}
	opts := storage.PutOptions{
		ContentType: r.Header.Get("Content-Type"),
		UserMeta:    extractUserMeta(r.Header),
	}
	m, err := s.store.PutObject(r.Context(), bucket, key, r.Body, opts)
	if err != nil {
		writeStorageError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteETag(m.ETag))
	w.Header().Set("x-amz-server-side-encryption", "AES256")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m, err := s.store.Meta().GetObjectMeta(bucket, key)
	if err != nil {
		writeError(w, r, errNoSuchKey, err.Error())
		return
	}
	w.Header().Set("ETag", quoteETag(m.ETag))
	w.Header().Set("Content-Type", contentType(m))
	w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
	w.Header().Set("Last-Modified", m.ModifiedAt.UTC().Format(http.TimeFormat))
	w.Header().Set("x-amz-server-side-encryption", "AES256")
	for k, v := range m.UserMeta {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	w.WriteHeader(http.StatusOK)
	if err := s.store.GetObject(m, w); err != nil {
		// 头已写出，只能中断连接并记录
		log.Printf("ERROR 流式下载失败 %s/%s: %v", bucket, key, err)
		return
	}
}

func (s *Server) headObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m, err := s.store.Meta().GetObjectMeta(bucket, key)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", quoteETag(m.ETag))
	w.Header().Set("Content-Type", contentType(m))
	w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
	w.Header().Set("Last-Modified", m.ModifiedAt.UTC().Format(http.TimeFormat))
	for k, v := range m.UserMeta {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := s.store.DeleteObject(bucket, key); err != nil {
		writeStorageError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	var req deleteRequest
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, r, errMalformedXML, err.Error())
		return
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		writeError(w, r, errMalformedXML, err.Error())
		return
	}
	result := deleteResult{Xmlns: s3XMLNS}
	for _, o := range req.Objects {
		if err := s.store.DeleteObject(bucket, o.Key); err != nil {
			result.Error = append(result.Error, deleteErrorEntry{
				Key: o.Key, Code: errInternal.Code, Message: err.Error(),
			})
			continue
		}
		result.Deleted = append(result.Deleted, deletedEntry{Key: o.Key})
	}
	writeXML(w, http.StatusOK, result)
}

// ---- 分片上传 ----

func (s *Server) initiateMPU(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ok, _ := s.store.Meta().BucketExists(bucket)
	if !ok {
		writeError(w, r, errNoSuchBucket, "")
		return
	}
	uploadID, err := s.store.InitMPU(bucket, key, r.Header.Get("Content-Type"), extractUserMeta(r.Header))
	if err != nil {
		writeStorageError(w, r, err)
		return
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Xmlns: s3XMLNS, Bucket: bucket, Key: key, UploadID: uploadID,
	})
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	num, err := strconv.Atoi(first(r.URL.Query(), "partNumber"))
	if err != nil || num < 1 || num > 10000 {
		writeError(w, r, errInvalidArgument, "partNumber 非法")
		return
	}
	pm, err := s.store.PutPart(r.Context(), bucket, uploadID, num, r.Body)
	if err != nil {
		if strings.Contains(err.Error(), "元数据不存在") {
			writeError(w, r, errNoSuchUpload, err.Error())
			return
		}
		writeStorageError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteETag(pm.ETag))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) completeMPU(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	var req struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Part    []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, r, errMalformedXML, err.Error())
		return
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		writeError(w, r, errMalformedXML, err.Error())
		return
	}
	nums := make([]int, 0, len(req.Part))
	for _, p := range req.Part {
		nums = append(nums, p.PartNumber)
	}
	m, err := s.store.CompleteMPU(r.Context(), bucket, uploadID, nums)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "元数据不存在"):
			writeError(w, r, errNoSuchUpload, err.Error())
		case strings.Contains(err.Error(), "part"):
			writeError(w, r, errInvalidPart, err.Error())
		default:
			writeStorageError(w, r, err)
		}
		return
	}
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Xmlns:    s3XMLNS,
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket, Key: key, ETag: quoteETag(m.ETag),
	})
}

func (s *Server) abortMPU(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	if err := s.store.AbortMPU(bucket, uploadID); err != nil {
		writeError(w, r, errNoSuchUpload, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listParts(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	st, err := s.store.Meta().GetMPU(bucket, uploadID)
	if err != nil {
		writeError(w, r, errNoSuchUpload, err.Error())
		return
	}
	out := listPartsResult{Xmlns: s3XMLNS, Bucket: bucket, Key: key, UploadID: uploadID}
	for _, p := range st.Parts {
		out.Part = append(out.Part, partEntry{
			PartNumber: p.Number, LastModified: isoTime(p.CreatedAt),
			ETag: quoteETag(p.ETag), Size: p.Size,
		})
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) listMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	states, err := s.store.Meta().ListMPU(bucket)
	if err != nil {
		writeError(w, r, errInternal, err.Error())
		return
	}
	out := listMultipartUploadsResult{Xmlns: s3XMLNS, Bucket: bucket}
	for _, st := range states {
		out.Uploads = append(out.Uploads, mpuEntry{
			Key: st.Key, UploadID: st.UploadID, Initiated: isoTime(st.CreatedAt),
		})
	}
	writeXML(w, http.StatusOK, out)
}

// ---- 生命周期 ----

func (s *Server) putLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	var req struct {
		XMLName xml.Name `xml:"LifecycleConfiguration"`
		Rules   []struct {
			ID     string `xml:"ID"`
			Status string `xml:"Status"`
			Filter struct {
				Prefix string `xml:"Prefix"`
			} `xml:"Filter"`
			Expiration struct {
				Days int `xml:"Days"`
			} `xml:"Expiration"`
			AbortIncomplete struct {
				DaysAfterInitiation int `xml:"DaysAfterInitiation"`
			} `xml:"AbortIncompleteMultipartUpload"`
		} `xml:"Rule"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, r, errMalformedXML, err.Error())
		return
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		writeError(w, r, errMalformedXML, err.Error())
		return
	}
	rule := &meta.LifecycleRule{Bucket: bucket, Enabled: true}
	if len(req.Rules) > 0 {
		rr := req.Rules[0]
		rule.Prefix = rr.Filter.Prefix
		rule.ExpirationDays = rr.Expiration.Days
		rule.AbortIncompleteDays = rr.AbortIncomplete.DaysAfterInitiation
		rule.Enabled = rr.Status == "Enabled"
	}
	if err := s.store.Meta().SetLifecycleRule(rule); err != nil {
		writeError(w, r, errInternal, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	rule, err := s.store.Meta().GetLifecycleRule(bucket)
	if err != nil {
		writeError(w, r, ErrorCode{Code: "NoSuchLifecycleConfiguration", Message: "The lifecycle configuration does not exist.", StatusCode: http.StatusNotFound}, "")
		return
	}
	type lcRuleXML struct {
		ID     string `xml:"ID"`
		Status string `xml:"Status"`
		Filter struct {
			Prefix string `xml:"Prefix"`
		} `xml:"Filter"`
		Expiration struct {
			Days int `xml:"Days"`
		} `xml:"Expiration,omitempty"`
		AbortIncompleteMultipartUpload *struct {
			DaysAfterInitiation int `xml:"DaysAfterInitiation"`
		} `xml:"AbortIncompleteMultipartUpload,omitempty"`
	}
	type lcConfig struct {
		XMLName xml.Name    `xml:"LifecycleConfiguration"`
		Xmlns   string      `xml:"xmlns,attr"`
		Rules   []lcRuleXML `xml:"Rule"`
	}
	cfg := lcConfig{Xmlns: s3XMLNS}
	rr := lcRuleXML{ID: "rule-1"}
	rr.Status = map[bool]string{true: "Enabled", false: "Disabled"}[rule.Enabled]
	rr.Filter.Prefix = rule.Prefix
	rr.Expiration.Days = rule.ExpirationDays
	if rule.AbortIncompleteDays > 0 {
		rr.AbortIncompleteMultipartUpload = &struct {
			DaysAfterInitiation int `xml:"DaysAfterInitiation"`
		}{rule.AbortIncompleteDays}
	}
	cfg.Rules = []lcRuleXML{rr}
	writeXML(w, http.StatusOK, cfg)
}

func (s *Server) deleteLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.store.Meta().DeleteLifecycleRule(bucket); err != nil {
		writeError(w, r, errInternal, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- 辅助 ----

func writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	enc := xml.NewEncoder(w)
	_ = enc.Encode(v)
}

func has(q map[string][]string, key string) bool {
	_, ok := q[key]
	return ok
}

func first(q map[string][]string, key string) string {
	if vs := q[key]; len(vs) > 0 {
		return vs[0]
	}
	return ""
}

func quoteETag(etag string) string {
	if strings.HasPrefix(etag, "\"") {
		return etag
	}
	return "\"" + etag + "\""
}

func contentType(m *meta.ObjectMeta) string {
	if m.ContentType != "" {
		return m.ContentType
	}
	return "application/octet-stream"
}

func extractUserMeta(h http.Header) map[string]string {
	out := map[string]string{}
	const prefix = "X-Amz-Meta-"
	for k, vs := range h {
		if strings.HasPrefix(k, prefix) && len(vs) > 0 {
			out[strings.ToLower(strings.TrimPrefix(k, prefix))] = vs[0]
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '-' && c != '.' {
			return false
		}
	}
	return true
}

// 保留 context 引用，便于后续在 handler 间传递超时控制。
var _ = context.Background
