package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3Client 封装 AWS 官方 SDK，验证标准客户端可直连。
type s3Client struct {
	cli       *s3.Client
	presigner *s3.PresignClient
	endpoint  string
}

func newS3Client(endpoint, accessKey, secretKey, region string) (*s3Client, error) {
	cfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, err
	}
	cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return &s3Client{
		cli:       cli,
		presigner: s3.NewPresignClient(cli),
		endpoint:  endpoint,
	}, nil
}

func (c *s3Client) createBucket(ctx context.Context, bucket string) error {
	_, err := c.cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	return err
}

func (c *s3Client) putObject(ctx context.Context, bucket, key, path string) error {
	f, err := openFile(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = c.cli.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        f,
		ContentType: aws.String("application/dicom"),
	})
	return err
}

func (c *s3Client) getObject(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := c.cli.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (c *s3Client) listObjects(ctx context.Context, bucket, prefix string, maxKeys int) (*s3.ListObjectsV2Output, error) {
	return c.cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(int32(maxKeys)),
	})
}

func (c *s3Client) deleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.cli.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func (c *s3Client) headObject(ctx context.Context, bucket, key string) error {
	_, err := c.cli.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

// presignGet 生成预签名下载链接。
func (c *s3Client) presignGet(ctx context.Context, bucket, key string, expiry time.Duration) (string, error) {
	out, err := c.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", err
	}
	return out.URL, nil
}

// httpGet 用裸 HTTP 请求访问预签名链接（不带任何鉴权头）。
func httpGet(url string) (int, []byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body, nil
}

// batchDelete 批量删除（DeleteObjects API）。
func (c *s3Client) batchDelete(ctx context.Context, bucket string, keys []string) error {
	objs := make([]types.ObjectIdentifier, 0, len(keys))
	for _, k := range keys {
		objs = append(objs, types.ObjectIdentifier{Key: aws.String(k)})
	}
	_, err := c.cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{Objects: objs},
	})
	return err
}

// expectNotFound 判断是否为 404。
func isNotFound(err error) bool {
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var ns *types.NoSuchKey
	return errors.As(err, &ns)
}
