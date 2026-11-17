package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"imstore/internal/index"
	"imstore/internal/s3sig"
	"imstore/internal/store"
)

const defaultMaxKeys = 1000

// Handler serves the S3-compatible API.
type Handler struct {
	st        *store.Store
	verifier  *s3sig.Verifier
	region    string
	accessKey string
	secretKey string
}

// NewHandler wires the S3 handler.
func NewHandler(st *store.Store, region, accessKey, secretKey string) *Handler {
	v := s3sig.NewVerifier(region, func(ak string) (string, bool) {
		if ak == accessKey {
			return secretKey, true
		}
		return "", false
	})
	return &Handler{st: st, verifier: v, region: region, accessKey: accessKey, secretKey: secretKey}
}

// Verifier is exposed so the console can mint presigned links.
func (h *Handler) Verifier() *s3sig.Verifier { return h.verifier }

// Credentials returns the configured S3 access/secret pair (console presign).
func (h *Handler) Credentials() (string, string) { return h.accessKey, h.secretKey }

// route is the parsed S3 request: path style with optional bucket and key.
type route struct {
	bucket string
	key    string
	sub    string // ?uploads, ?partNumber etc. handled via query instead
}

func parseRoute(path string) route {
	p := strings.TrimPrefix(path, "/")
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return route{}
	}
	parts := strings.SplitN(p, "/", 2)
	r := route{bucket: parts[0]}
	if len(parts) == 2 {
		r.key = parts[1]
	}
	return r
}

// ServeHTTP dispatches and authenticates every request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := newRequestID()
	w.Header().Set("x-amz-request-id", reqID)
	w.Header().Set("Server", "imstore")

	rt := parseRoute(r.URL.Path)
	q := r.URL.Query()

	// Presigned requests carry auth in the query string.
	body, err := h.verifier.Verify(r)
	if err != nil {
		writeXMLError(w, r, withMessage(errSignatureDoesNotMatch, err.Error()), reqID)
		return
	}
	if body != nil {
		r.Body = io.NopCloser(body)
	}
	// For fixed-payload header signing, the signature covers the declared
	// content hash but not the bytes themselves. Re-hash the actual stream so
	// a body tampered in transit cannot slip past a valid-looking signature.
	if q.Get("X-Amz-Algorithm") == "" {
		declared := r.Header.Get("X-Amz-Content-Sha256")
		if declared != "" && declared != "UNSIGNED-PAYLOAD" &&
			!strings.HasPrefix(declared, "STREAMING-") {
			hashCheck := newHashingReader(r.Body, declared)
			r.Body = io.NopCloser(hashCheck)
			*r = *r.WithContext(context.WithValue(r.Context(), hashCheckKey{}, hashCheck))
		}
	}

	ctx := r.Context()
	switch {
	// ---- service level ----
	case rt.bucket == "":
		h.serviceOps(w, r, reqID)
	// ---- multipart sub-resources (present even with empty value) ----
	case queryHas(q, "uploads"):
		h.multipartUploads(w, r, rt, reqID)
	case queryHas(q, "uploadId"):
		h.multipartParts(w, r, rt, q.Get("uploadId"), reqID)
	// ---- bucket level ----
	case rt.key == "":
		h.bucketOps(w, r, rt, reqID)
	// ---- object level ----
	default:
		h.objectOps(w, r, ctx, rt, reqID)
	}
}

// queryHas reports whether key is present, tolerating an empty value
// (?uploads and ?uploads= both count, matching how S3 clients emit them).
func queryHas(q url.Values, key string) bool {
	_, ok := q[key]
	return ok
}

// hashCheckKey is the context key carrying the per-request digest verifier.
type hashCheckKey struct{}

func withMessage(base *s3Error, msg string) *s3Error {
	cp := *base
	cp.message = msg
	return &cp
}

// ---------------- service & buckets ----------------

func (h *Handler) serviceOps(w http.ResponseWriter, r *http.Request, reqID string) {
	switch r.Method {
	case http.MethodGet:
		buckets, err := h.st.Index().ListBuckets()
		if err != nil {
			writeXMLError(w, r, errInternal, reqID)
			return
		}
		out := listAllMyBucketsResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
		out.Owner = xmlOwner{ID: "imstore", DisplayName: "imstore"}
		for _, b := range buckets {
			out.Buckets.Bucket = append(out.Buckets.Bucket, xmlBucket{
				Name:         b.Name,
				CreationDate: b.CreatedAt.UTC().Format(time.RFC3339),
			})
		}
		writeXML(w, http.StatusOK, out)
	default:
		writeXMLError(w, r, errMethodNotAllowed, reqID)
	}
}

func (h *Handler) bucketOps(w http.ResponseWriter, r *http.Request, rt route, reqID string) {
	switch r.Method {
	case http.MethodPut:
		if err := h.st.Index().CreateBucket(rt.bucket); err != nil {
			if strings.Contains(err.Error(), "already exists") {
				writeXMLError(w, r, errBucketAlreadyExists, reqID)
				return
			}
			writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
			return
		}
		w.Header().Set("Location", "/"+rt.bucket)
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if err := h.st.Index().DeleteBucket(rt.bucket); err != nil {
			if errors.Is(err, index.ErrNotFound) {
				writeXMLError(w, r, errNoSuchBucket, reqID)
				return
			}
			writeXMLError(w, r, errBucketNotEmpty, reqID)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet, http.MethodHead:
		h.listObjects(w, r, rt, reqID)
	default:
		writeXMLError(w, r, errMethodNotAllowed, reqID)
	}
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, rt route, reqID string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	maxKeys := defaultMaxKeys
	if s := q.Get("max-keys"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxKeys = n
		}
	}
	startAfter := q.Get("start-after")
	if ct := q.Get("continuation-token"); ct != "" {
		startAfter = ct
	}
	if q.Get("list-type") != "2" && q.Get("marker") != "" {
		startAfter = q.Get("marker") // v1 compatibility
	}

	page, err := h.st.Index().ListObjects(rt.bucket, prefix, startAfter, delimiter, maxKeys)
	if err != nil {
		writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
		return
	}
	out := listBucketResult{
		Xmlns:             "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:              rt.bucket,
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		IsTruncated:       page.Truncated,
		ContinuationToken: q.Get("continuation-token"),
		NextToken:         page.NextStartKey,
	}
	for _, m := range page.Objects {
		out.Contents = append(out.Contents, xmlObject{
			Key:          m.Key,
			LastModified: m.CreatedAt.UTC().Format(time.RFC3339),
			ETag:         quoteETag(m.ETag),
			Size:         m.Size,
			StorageClass: "STANDARD",
			Owner:        xmlOwner{ID: "imstore", DisplayName: "imstore"},
		})
	}
	for _, cp := range page.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, xmlCommonPrefix{Prefix: cp})
	}
	out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		return
	}
	writeXML(w, http.StatusOK, out)
}

// ---------------- objects ----------------

func (h *Handler) objectOps(w http.ResponseWriter, r *http.Request, ctx context.Context, rt route, reqID string) {
	switch r.Method {
	case http.MethodPut:
		h.putObject(w, r, ctx, rt, reqID)
	case http.MethodGet:
		h.getObject(w, r, rt, reqID)
	case http.MethodHead:
		h.headObject(w, r, rt, reqID)
	case http.MethodDelete:
		h.deleteObject(w, r, rt, reqID)
	default:
		writeXMLError(w, r, errMethodNotAllowed, reqID)
	}
}

func (h *Handler) putObject(w http.ResponseWriter, r *http.Request, ctx context.Context, rt route, reqID string) {
	contentType := r.Header.Get("Content-Type")
	userMeta := map[string]string{}
	for k, vs := range r.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-meta-") {
			userMeta[strings.TrimPrefix(lk, "x-amz-meta-")] = vs[0]
		}
	}
	meta, err := h.st.PutObject(ctx, rt.bucket, rt.key, contentType, userMeta, r.Body)
	if err != nil {
		if errors.Is(err, errPayloadHash) {
			writeXMLError(w, r, withMessage(errSignatureDoesNotMatch, err.Error()), reqID)
			return
		}
		writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
		return
	}
	// The full payload has been streamed; verify its digest before we commit
	// a success response. This catches in-transit tampering.
	if hc, ok := ctx.Value(hashCheckKey{}).(*hashingReader); ok {
		if err := hc.Close(); errors.Is(err, errPayloadHash) {
			writeXMLError(w, r, withMessage(errSignatureDoesNotMatch, err.Error()), reqID)
			return
		} else if err != nil {
			writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
			return
		}
	}
	w.Header().Set("ETag", quoteETag(meta.ETag))
	w.Header().Set("x-amz-sha256", meta.SHA256)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getObject(w http.ResponseWriter, r *http.Request, rt route, reqID string) {
	reader, err := h.st.OpenObject(rt.bucket, rt.key)
	if err != nil {
		if errors.Is(err, store.ErrObjectNotFound) {
			writeXMLError(w, r, errNoSuchKey, reqID)
			return
		}
		writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
		return
	}
	meta := reader.Meta()
	h.writeObjectHeaders(w, meta)

	// Range support.
	if rg := r.Header.Get("Range"); rg != "" {
		start, end, ok := parseRange(rg, meta.Size)
		if !ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.Size))
			writeXMLError(w, r, withMessage(errInvalidArgument, "invalid Range"), reqID)
			return
		}
		if err := reader.SetRange(start, end); err != nil {
			writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, meta.Size))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.Copy(w, reader)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

func (h *Handler) headObject(w http.ResponseWriter, r *http.Request, rt route, reqID string) {
	meta, err := h.st.Index().GetObject(rt.bucket, rt.key)
	if err != nil {
		if errors.Is(err, index.ErrNotFound) {
			writeXMLError(w, r, errNoSuchKey, reqID)
			return
		}
		writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
		return
	}
	h.writeObjectHeaders(w, meta)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteObject(w http.ResponseWriter, r *http.Request, rt route, reqID string) {
	if err := h.st.DeleteObject(rt.bucket, rt.key); err != nil {
		if errors.Is(err, store.ErrObjectNotFound) {
			// S3 DELETE on a missing key still returns 204.
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) writeObjectHeaders(w http.ResponseWriter, meta *index.ObjectMeta) {
	if meta.ContentType != "" {
		w.Header().Set("Content-Type", meta.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("ETag", quoteETag(meta.ETag))
	w.Header().Set("Last-Modified", meta.CreatedAt.UTC().Format(http.TimeFormat))
	w.Header().Set("x-amz-sha256", meta.SHA256)
	for k, v := range meta.UserMeta {
		w.Header().Set("X-Amz-Meta-"+k, v)
	}
}

// quoteETag wraps a raw hex ETag in double quotes as S3 clients expect
// (multipart ETags already contain "-N" but still need quoting).
func quoteETag(etag string) string {
	if strings.HasPrefix(etag, "\"") {
		return etag
	}
	return "\"" + etag + "\""
}

// parseRange parses a single "bytes=start-end" range (suffix ranges
// "bytes=-N" and open-ended "bytes=N-" supported).
func parseRange(hdr string, size int64) (start, end int64, ok bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(hdr, prefix) {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(hdr, prefix)
	if strings.Contains(spec, ",") {
		return 0, 0, false // multiple ranges not supported
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false
	}
	lo, hi := spec[:dash], spec[dash+1:]
	if lo == "" {
		// suffix range: last N bytes
		n, err := strconv.ParseInt(hi, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, size, true
	}
	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if hi == "" {
		end = size
	} else {
		end, err = strconv.ParseInt(hi, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		end++ // end is exclusive internally
	}
	if start >= size || end <= start {
		return 0, 0, false
	}
	if end > size {
		end = size
	}
	return start, end, true
}

// ---------------- multipart ----------------

func (h *Handler) multipartUploads(w http.ResponseWriter, r *http.Request, rt route, reqID string) {
	switch r.Method {
	case http.MethodPost: // initiate
		ct := r.Header.Get("Content-Type")
		userMeta := map[string]string{}
		for k, vs := range r.Header {
			lk := strings.ToLower(k)
			if strings.HasPrefix(lk, "x-amz-meta-") {
				userMeta[strings.TrimPrefix(lk, "x-amz-meta-")] = vs[0]
			}
		}
		id, err := h.st.InitMultipart(rt.bucket, rt.key, ct, userMeta)
		if err != nil {
			writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
			return
		}
		writeXML(w, http.StatusOK, initiateMPResult{Bucket: rt.bucket, Key: rt.key, UploadID: id})
	case http.MethodGet: // list uploads
		ups, err := h.st.Index().ListMPUploads(rt.bucket, "")
		if err != nil {
			writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
			return
		}
		out := listMPUploadsResult{Bucket: rt.bucket}
		for _, u := range ups {
			out.Uploads = append(out.Uploads, xmlMPUpload{
				Key:       u.Key,
				UploadID:  u.UploadID,
				Initiated: u.CreatedAt.UTC().Format(time.RFC3339),
			})
		}
		writeXML(w, http.StatusOK, out)
	default:
		writeXMLError(w, r, errMethodNotAllowed, reqID)
	}
}

func (h *Handler) multipartParts(w http.ResponseWriter, r *http.Request, rt route, uploadID string, reqID string) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodPut: // UploadPart
		pn, err := strconv.Atoi(q.Get("partNumber"))
		if err != nil || pn < 1 {
			writeXMLError(w, r, errInvalidPart, reqID)
			return
		}
		pm, err := h.st.UploadPart(r.Context(), uploadID, pn, r.Body)
		if err != nil {
			if errors.Is(err, index.ErrNotFound) {
				writeXMLError(w, r, errNoSuchUpload, reqID)
				return
			}
			writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
			return
		}
		w.Header().Set("ETag", quoteETag(pm.ETag))
		w.WriteHeader(http.StatusOK)
	case http.MethodPost: // CompleteMultipartUpload
		var req completeMPRequest
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeXMLError(w, r, errMalformedXML, reqID)
			return
		}
		if err := xml.Unmarshal(body, &req); err != nil {
			writeXMLError(w, r, errMalformedXML, reqID)
			return
		}
		nums := make([]int, 0, len(req.Parts))
		for _, p := range req.Parts {
			nums = append(nums, p.PartNumber)
		}
		meta, err := h.st.CompleteMultipart(uploadID, nums)
		if err != nil {
			if errors.Is(err, index.ErrNotFound) {
				writeXMLError(w, r, errNoSuchUpload, reqID)
				return
			}
			writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
			return
		}
		writeXML(w, http.StatusOK, completeMPResult{
			Bucket: rt.bucket, Key: rt.key, ETag: quoteETag(meta.ETag),
		})
	case http.MethodDelete: // AbortMultipartUpload
		if err := h.st.AbortMultipart(uploadID); err != nil {
			if errors.Is(err, index.ErrNotFound) {
				writeXMLError(w, r, errNoSuchUpload, reqID)
				return
			}
			writeXMLError(w, r, withMessage(errInternal, err.Error()), reqID)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet: // ListParts
		parts, err := h.st.Index().ListMParts(uploadID)
		if err != nil {
			writeXMLError(w, r, errNoSuchUpload, reqID)
			return
		}
		out := listPartsResult{Bucket: rt.bucket, Key: rt.key, UploadID: uploadID}
		for _, p := range parts {
			out.Part = append(out.Part, xmlPart{
				PartNumber:   p.PartNumber,
				LastModified: time.Now().UTC().Format(time.RFC3339),
				ETag:         quoteETag(p.ETag),
				Size:         p.Size,
			})
		}
		writeXML(w, http.StatusOK, out)
	default:
		writeXMLError(w, r, errMethodNotAllowed, reqID)
	}
}

// newRequestID returns a short hex request id.
func newRequestID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
