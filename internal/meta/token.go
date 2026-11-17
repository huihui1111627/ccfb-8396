package meta

import "encoding/base64"

// encodeToken 把续页起始 key 编码成不透明 token。
func encodeToken(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

// decodeToken 解析续页 token。
func decodeToken(token string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
