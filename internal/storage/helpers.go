package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

// 对 os 包的薄封装，便于测试中替换/统一错误判断。
var (
	osRemove   = os.Remove
	osRename   = os.Rename
	osMkdirAll = os.MkdirAll
	osOpenFile = os.OpenFile
)

var errNotFound = errors.New("文件不存在")

func osIsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }

// randHex 返回 n 字节随机数的十六进制字符串。
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// rand.Read 极少失败；退化为全零保证不 panic
		return hex.EncodeToString(b)
	}
	return hex.EncodeToString(b)
}

// ctxReadCloser 包装请求体，ctx 取消时读取立即中断。
type ctxReadCloser struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReadCloser) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := c.r.Read(p)
	if err == nil && c.ctx.Err() != nil {
		return n, c.ctx.Err()
	}
	return n, err
}
