package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3ListInput 包装 ListObjectsV2 入参（续页）。
func s3ListInput(bucket, prefix string, maxKeys int, token *string) *s3.ListObjectsV2Input {
	return &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		Prefix:            aws.String(prefix),
		MaxKeys:           aws.Int32(int32(maxKeys)),
		ContinuationToken: token,
	}
}

// s3PutInput 包装 PutObject 入参。
func s3PutInput(bucket, key string, body io.ReadSeeker) *s3.PutObjectInput {
	return &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        body,
		ContentType: aws.String("application/dicom"),
	}
}

// checkConsoleAPI 验证控制台的磁盘、修复、桶三类接口。
func checkConsoleAPI(addr, token string) bool {
	base := "http://" + addr
	client := &http.Client{Timeout: 5 * time.Second}
	get := func(path string) (int, []byte) {
		req, _ := http.NewRequest(http.MethodGet, base+path, nil)
		req.Header.Set("X-Console-Token", token)
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b
	}
	// 无 token 应被拒绝
	req, _ := http.NewRequest(http.MethodGet, base+"/api/disks", nil)
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return false
	}
	resp.Body.Close()

	for _, path := range []string{"/api/disks", "/api/buckets", "/api/repair/tasks", "/api/lifecycle/rules"} {
		code, body := get(path)
		if code != http.StatusOK {
			return false
		}
		var any any
		if err := json.Unmarshal(body, &any); err != nil {
			fmt.Printf("控制台 %s 返回非 JSON: %v\n", path, err)
			return false
		}
	}
	// 磁盘数应为 7
	_, disksBody := get("/api/disks")
	var disks []map[string]any
	if err := json.Unmarshal(disksBody, &disks); err != nil || len(disks) != 8 {
		return false
	}
	return true
}
