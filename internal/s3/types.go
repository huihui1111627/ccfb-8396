package s3

import (
	"encoding/xml"
	"time"
)

// 参考 AWS 文档的 S3 XML 响应结构。

type listAllMyBucketsResult struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	Xmlns   string      `xml:"xmlns,attr"`
	Owner   owner       `xml:"Owner"`
	Buckets listBuckets `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type listBuckets struct {
	Bucket []listBucketEntry `xml:"Bucket"`
}

type listBucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	NextToken      string         `xml:"ContinuationToken,omitempty"`
	NextStartAfter string         `xml:"NextContinuationToken,omitempty"`
	KeyCount       int            `xml:"KeyCount"`
	Contents       []objectEntry  `xml:"Contents"`
	CommonPrefixes []commonPrefix `xml:"CommonPrefixes"`
}

type objectEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	ContentType  string `xml:"-"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type listMultipartUploadsResult struct {
	XMLName xml.Name   `xml:"ListMultipartUploadsResult"`
	Xmlns   string     `xml:"xmlns,attr"`
	Bucket  string     `xml:"Bucket"`
	Uploads []mpuEntry `xml:"Upload"`
}

type mpuEntry struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

type listPartsResult struct {
	XMLName  xml.Name    `xml:"ListPartsResult"`
	Xmlns    string      `xml:"xmlns,attr"`
	Bucket   string      `xml:"Bucket"`
	Key      string      `xml:"Key"`
	UploadID string      `xml:"UploadId"`
	Part     []partEntry `xml:"Part"`
}

type partEntry struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type deleteRequest struct {
	XMLName xml.Name       `xml:"Delete"`
	Objects []deleteObject `xml:"Object"`
	Quiet   bool           `xml:"Quiet"`
}

type deleteObject struct {
	Key string `xml:"Key"`
}

type deleteResult struct {
	XMLName xml.Name           `xml:"DeleteResult"`
	Xmlns   string             `xml:"xmlns,attr"`
	Deleted []deletedEntry     `xml:"Deleted"`
	Error   []deleteErrorEntry `xml:"Error"`
}

type deletedEntry struct {
	Key string `xml:"Key"`
}

type deleteErrorEntry struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

const s3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

func isoTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
