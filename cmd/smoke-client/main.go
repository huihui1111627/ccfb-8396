// Command smoke-client 用 AWS S3 SDK 对运行中的服务做联调冒烟测试。
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:19000", "服务地址")
	ak := flag.String("ak", "testaccesskey", "AccessKey")
	sk := flag.String("sk", "testsecretkey", "SecretKey")
	region := flag.String("region", "us-east-1", "Region")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(*region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*ak, *sk, "")),
	)
	must(err)
	cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(*endpoint)
		o.UsePathStyle = true
	})
	bucket := "smoke-bucket"

	// 1. 建桶
	if _, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		fmt.Println("建桶（可能已存在）:", err)
	} else {
		fmt.Println("✅ CreateBucket")
	}

	// 2. 上传对象（含用户自定义元数据）
	content := bytes.Repeat([]byte("ErasureStore-Smoke-"), 20000) // ~380KB
	sum := sha256.Sum256(content)
	_, err = cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("CT/study/smoke.dcm"),
		Body: bytes.NewReader(content), ContentType: aws.String("application/dicom"),
		Metadata: map[string]string{"patient": "test-001", "modality": "CT"},
	})
	must(err)
	fmt.Println("✅ PutObject（带自定义元数据）")

	// 3. 列举
	out, err := cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("CT/")})
	must(err)
	fmt.Printf("✅ ListObjectsV2 前缀 CT/ 命中 %d 个对象\n", len(out.Contents))

	// 4. 下载并比对哈希
	get, err := cli.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("CT/study/smoke.dcm")})
	must(err)
	got, _ := io.ReadAll(get.Body)
	get.Body.Close()
	gsum := sha256.Sum256(got)
	if hex.EncodeToString(gsum[:]) != hex.EncodeToString(sum[:]) {
		fmt.Println("❌ 下载哈希不一致")
		os.Exit(1)
	}
	fmt.Println("✅ GetObject 下载内容与上传哈希一致, ContentType=", aws.ToString(get.ContentType), "patient=", get.Metadata["patient"])

	// 5. HEAD
	head, err := cli.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("CT/study/smoke.dcm")})
	must(err)
	fmt.Printf("✅ HeadObject 大小=%d ETag=%s\n", aws.ToInt64(head.ContentLength), aws.ToString(head.ETag))

	// 6. 预签名链接（匿名 HTTP 可访问）
	presigner := s3.NewPresignClient(cli)
	ps, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("CT/study/smoke.dcm"),
	}, s3.WithPresignExpires(2*time.Minute))
	must(err)
	resp, err := http.Get(ps.URL)
	must(err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || sha256.Sum256(body) != sum {
		fmt.Println("❌ 预签名链接下载失败")
		os.Exit(1)
	}
	fmt.Println("✅ 预签名链接可匿名下载（无鉴权头）")

	// 7. 删除 + 404
	_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("CT/study/smoke.dcm")})
	must(err)
	_, err = cli.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("CT/study/smoke.dcm")})
	if err == nil {
		fmt.Println("❌ 删除后仍可 HEAD")
		os.Exit(1)
	}
	fmt.Println("✅ DeleteObject 后 HEAD 返回 404")
	fmt.Println("\n冒烟测试全部通过 ✅")
}

func must(err error) {
	if err != nil {
		fmt.Println("❌", err)
		os.Exit(1)
	}
}
