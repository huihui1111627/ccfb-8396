// Package s3 implements the HTTP/XML S3-compatible API surface (bucket and
// object operations, multipart uploads and presigned access) on top of the
// erasure-coded store.
package s3

import (
	"encoding/xml"
	"net/http"
)

// S3 error codes and their HTTP status mapping.
type s3Error struct {
	code      string
	message   string
	status    int
	resource  string
	requestID string
}

func (e *s3Error) Error() string { return e.code + ": " + e.message }

var (
	errAccessDenied          = &s3Error{code: "AccessDenied", message: "Access Denied", status: http.StatusForbidden}
	errInvalidArgument       = &s3Error{code: "InvalidArgument", message: "Invalid Argument", status: http.StatusBadRequest}
	errInvalidPart           = &s3Error{code: "InvalidPart", message: "One or more of the specified parts could not be found", status: http.StatusBadRequest}
	errInvalidPartOrder      = &s3Error{code: "InvalidPartOrder", message: "The list of parts was not in ascending order", status: http.StatusBadRequest}
	errNoSuchUpload          = &s3Error{code: "NoSuchUpload", message: "The specified multipart upload does not exist", status: http.StatusNotFound}
	errNoSuchKey             = &s3Error{code: "NoSuchKey", message: "The specified key does not exist", status: http.StatusNotFound}
	errNoSuchBucket          = &s3Error{code: "NoSuchBucket", message: "The specified bucket does not exist", status: http.StatusNotFound}
	errBucketAlreadyExists   = &s3Error{code: "BucketAlreadyExists", message: "The requested bucket name is not available", status: http.StatusConflict}
	errBucketNotEmpty        = &s3Error{code: "BucketNotEmpty", message: "The bucket you tried to delete is not empty", status: http.StatusConflict}
	errSignatureDoesNotMatch = &s3Error{code: "SignatureDoesNotMatch", message: "The request signature we calculated does not match the signature you provided", status: http.StatusForbidden}
	errMissingFields         = &s3Error{code: "AuthorizationHeaderMalformed", message: "The authorization header is malformed", status: http.StatusBadRequest}
	errMethodNotAllowed      = &s3Error{code: "MethodNotAllowed", message: "The specified method is not allowed against this resource", status: http.StatusMethodNotAllowed}
	errInternal              = &s3Error{code: "InternalError", message: "We encountered an internal error", status: http.StatusInternalServerError}
	errEntityTooLarge        = &s3Error{code: "EntityTooLarge", message: "Your proposed upload exceeds the maximum allowed object size", status: http.StatusBadRequest}
	errMalformedXML          = &s3Error{code: "MalformedXML", message: "The XML you provided was not well-formed", status: http.StatusBadRequest}
)

// xmlError is the S3 REST error envelope.
type xmlError struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

// xmlOwner is the common owner block.
type xmlOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// listAllMyBucketsResult is GET / response body.
type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   xmlOwner
	Buckets struct {
		Bucket []xmlBucket `xml:"Bucket"`
	}
}

type xmlBucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// listBucketResult is GET /bucket?list-type=2 response body.
type listBucketResult struct {
	XMLName           xml.Name          `xml:"ListBucketResult"`
	Xmlns             string            `xml:"xmlns,attr"`
	Name              string            `xml:"Name"`
	Prefix            string            `xml:"Prefix"`
	Delimiter         string            `xml:"Delimiter,omitempty"`
	KeyCount          int               `xml:"KeyCount"`
	MaxKeys           int               `xml:"MaxKeys"`
	IsTruncated       bool              `xml:"IsTruncated"`
	NextToken         string            `xml:"NextToken,omitempty"`
	ContinuationToken string            `xml:"ContinuationToken,omitempty"`
	Contents          []xmlObject       `xml:"Contents"`
	CommonPrefixes    []xmlCommonPrefix `xml:"CommonPrefixes"`
}

type xmlObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        xmlOwner
}

type xmlCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// initiateMultipartUploadResult.
type initiateMPResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// completeMultipartUpload request/response.
type completeMPRequest struct {
	XMLName xml.Name         `xml:"CompleteMultipartUpload"`
	Parts   []completeMPPart `xml:"Part"`
}

type completeMPPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMPResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

// listMultipartUploadsResult.
type listMPUploadsResult struct {
	XMLName     xml.Name `xml:"ListMultipartUploadsResult"`
	Bucket      string
	Uploads     []xmlMPUpload `xml:"Upload"`
	IsTruncated bool
}

type xmlMPUpload struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

// listPartsResult.
type listPartsResult struct {
	XMLName     xml.Name `xml:"ListPartsResult"`
	Bucket      string
	Key         string
	UploadID    string    `xml:"UploadId"`
	Part        []xmlPart `xml:"Part"`
	IsTruncated bool
}

type xmlPart struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

func writeXML(w http.ResponseWriter, status int, v any) {
	body, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	body = append([]byte(xml.Header), body...)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", itoa(len(body)))
	w.WriteHeader(status)
	w.Write(body)
}

func writeXMLError(w http.ResponseWriter, r *http.Request, e *s3Error, reqID string) {
	env := xmlError{
		Code:      e.code,
		Message:   e.message,
		Resource:  r.URL.Path,
		RequestID: reqID,
	}
	body, _ := xml.MarshalIndent(env, "", "  ")
	body = append([]byte(xml.Header), body...)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", itoa(len(body)))
	w.WriteHeader(e.status)
	w.Write(body)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
