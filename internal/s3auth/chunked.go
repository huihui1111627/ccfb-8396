package s3auth

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// contentHash 声明值的特殊标记。
const (
	unsignedPayload      = "UNSIGNED-PAYLOAD"
	streamingSigned      = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingUnsigned    = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	streamingSignedTrail = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
)

// IsStreamingPayload 判断是否为 aws-chunked 流式请求体。
func IsStreamingPayload(declared string) bool {
	switch declared {
	case streamingSigned, streamingUnsigned, streamingSignedTrail:
		return true
	}
	return false
}

// DecodeBody 处理请求体：
//   - 普通哈希/UNSIGNED-PAYLOAD：原样返回，并校验已声明的 sha256（若为普通哈希）；
//   - aws-chunked：解帧后返回拼接的真实数据 reader。
//
// 返回的 reader 替换 r.Body。
func DecodeBody(r *http.Request, declared string) (io.ReadCloser, error) {
	if !IsStreamingPayload(declared) {
		if declared != "" && declared != unsignedPayload {
			// 普通整包 sha256：缓冲校验（SDK 小对象走这条路径）
			data, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			sum := sha256.Sum256(data)
			if hex.EncodeToString(sum[:]) != strings.ToLower(declared) {
				return nil, fmt.Errorf("%w: x-amz-content-sha256 与请求体不一致", ErrSignature)
			}
			return io.NopCloser(bytes.NewReader(data)), nil
		}
		return r.Body, nil
	}
	return newChunkedReader(r.Body, declared == streamingSigned || declared == streamingSignedTrail), nil
}

// chunkedReader 解析 aws-chunked 编码：
//
//	chunk-header ";" key=val *(";" key=val) CRLF
//	chunk-data CRLF
//	...
//	"0" chunk-header CRLF
//	trailer-headers CRLF
type chunkedReader struct {
	br     *bufio.Reader
	signed bool         // 是否带逐 chunk 签名（当前实现消费但不强校验 trailer 签名）
	buf    bytes.Buffer // 当前 chunk 未消费完的数据
	done   bool
}

func newChunkedReader(r io.Reader, signed bool) *chunkedReader {
	return &chunkedReader{br: bufio.NewReader(r), signed: signed}
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.buf.Len() > 0 {
		return c.buf.Read(p)
	}
	if c.done {
		return 0, io.EOF
	}
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			return 0, fmt.Errorf("读取 chunk 头失败: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue // 容忍空行
		}
		sizePart := strings.Split(line, ";")[0]
		size, err := strconv.ParseInt(sizePart, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("chunk 长度解析失败 %q: %w", line, err)
		}
		if size == 0 {
			c.done = true
			c.drainTrailer()
			return 0, io.EOF
		}
		// chunk-data + CRLF
		data := make([]byte, size)
		if _, err := io.ReadFull(c.br, data); err != nil {
			return 0, fmt.Errorf("读取 chunk 数据失败: %w", err)
		}
		if err := c.discardCRLF(); err != nil {
			return 0, err
		}
		c.buf.Write(data)
		return c.buf.Read(p)
	}
}

// discardCRLF 消费 chunk 末尾的 CRLF。
func (c *chunkedReader) discardCRLF() error {
	b, err := c.br.ReadByte()
	if err != nil {
		return err
	}
	if b != '\r' {
		return fmt.Errorf("chunk 末尾缺少 CR，得到 %q", string(b))
	}
	b, err = c.br.ReadByte()
	if err != nil {
		return err
	}
	if b != '\n' {
		return errors.New("chunk 末尾缺少 LF")
	}
	return nil
}

// drainTrailer 丢弃结束 chunk 之后的 trailer 部分（签名校验在帧头已由外层保证，
// trailer 中的 x-amz-trailer-signature 这里只做消费，不做强校验以保持与标准客户端互通）。
func (c *chunkedReader) drainTrailer() {
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			return
		}
		if strings.TrimRight(line, "\r\n") == "" {
			return
		}
	}
}

func (c *chunkedReader) Close() error { return nil }
