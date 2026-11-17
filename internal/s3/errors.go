// Package s3 实现兼容常用 S3 接口的 HTTP 协议层。
package s3

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

// ErrorCode S3 错误码及其 HTTP 状态码。
type ErrorCode struct {
	Code       string
	Message    string
	StatusCode int
}

var (
	errAccessDenied        = ErrorCode{"AccessDenied", "Access Denied", http.StatusForbidden}
	errInvalidArgument     = ErrorCode{"InvalidArgument", "Invalid Argument", http.StatusBadRequest}
	errInvalidBucketName   = ErrorCode{"InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest}
	errBucketAlreadyExists = ErrorCode{"BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict}
	errBucketNotEmpty      = ErrorCode{"BucketNotEmpty", "The bucket you tried to delete is not empty.", http.StatusConflict}
	errNoSuchBucket        = ErrorCode{"NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound}
	errNoSuchKey           = ErrorCode{"NoSuchKey", "The specified key does not exist.", http.StatusNotFound}
	errNoSuchUpload        = ErrorCode{"NoSuchUpload", "The specified multipart upload does not exist.", http.StatusNotFound}
	errInvalidPart         = ErrorCode{"InvalidPart", "One or more of the specified parts could not be found.", http.StatusBadRequest}
	errInvalidPartOrder    = ErrorCode{"InvalidPartOrder", "The list of parts was not in ascending order.", http.StatusBadRequest}
	errMalformedXML        = ErrorCode{"MalformedXML", "The XML you provided was not well-formed.", http.StatusBadRequest}
	errSignatureProblem    = ErrorCode{"SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", http.StatusForbidden}
	errInvalidRequest      = ErrorCode{"InvalidRequest", "The request is invalid.", http.StatusBadRequest}
	errEntityTooSmall      = ErrorCode{"EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size.", http.StatusBadRequest}
	errInternal            = ErrorCode{"InternalError", "We encountered an internal error.", http.StatusInternalServerError}
	errServiceUnavailable  = ErrorCode{"ServiceUnavailable", "Reduce your request rate.", http.StatusServiceUnavailable}
	errNotImplemented      = ErrorCode{"NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented}
)

// xmlError S3 错误响应体。
type xmlError struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

func writeError(w http.ResponseWriter, r *http.Request, ec ErrorCode, detail string) {
	msg := ec.Message
	if detail != "" {
		msg = detail
	}
	body := xmlError{
		Code:      ec.Code,
		Message:   msg,
		Resource:  r.URL.Path,
		RequestID: requestID(r),
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(ec.StatusCode)
	enc := xml.NewEncoder(w)
	_ = enc.Encode(body)
}

// writeStorageError 把存储层错误映射为 S3 错误。
func writeStorageError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case isMetaNotFound(err):
		writeError(w, r, errNoSuchKey, err.Error())
	default:
		writeError(w, r, errInternal, err.Error())
	}
}

func isMetaNotFound(err error) bool {
	return err != nil && contains(err.Error(), "元数据不存在")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

func requestID(r *http.Request) string {
	if v := r.Header.Get("X-Request-Id"); v != "" {
		return v
	}
	return fmt.Sprintf("req-%p", r)
}
