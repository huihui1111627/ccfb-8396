package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

// FakeImage 一个模拟影像文件及其期望哈希。
type FakeImage struct {
	Key      string
	Path     string
	Size     int64
	SHA256   string
	Modality string
}

// generateFakeImages 在 dir 下生成 count 个大小不一的假影像文件（DICOM 风格前缀+随机像素）。
func generateFakeImages(dir string, count int) ([]FakeImage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewSource(20261118))
	modalities := []string{"CT", "MR", "DX", "US"}
	out := make([]FakeImage, 0, count)
	for i := 0; i < count; i++ {
		mod := modalities[i%len(modalities)]
		// 大小从 8KB 到 ~6MB 不等，制造不同 stripe 数量
		size := 8*1024 + rng.Intn(6*1024*1024)
		key := fmt.Sprintf("%s/2026/11/study-%04d/%s_IMG_%05d.dcm", mod, i/10, mod, i)
		path := filepath.Join(dir, fmt.Sprintf("img-%05d.dcm", i))

		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		h := sha256.New()
		// 先写一个伪造的 DICOM 前缀
		header := []byte(fmt.Sprintf("DICM-ERASURESTORE-%s-%05d-", mod, i))
		if _, err := f.Write(header); err != nil {
			return nil, err
		}
		h.Write(header)

		buf := make([]byte, 64*1024)
		remaining := size
		for remaining > 0 {
			n := len(buf)
			if remaining < n {
				n = remaining
			}
			if _, err := rng.Read(buf[:n]); err != nil {
				return nil, err
			}
			if _, err := f.Write(buf[:n]); err != nil {
				return nil, err
			}
			h.Write(buf[:n])
			remaining -= n
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		fi, _ := os.Stat(path)
		out = append(out, FakeImage{
			Key: key, Path: path, Size: fi.Size(),
			SHA256: hex.EncodeToString(h.Sum(nil)), Modality: mod,
		})
	}
	return out, nil
}

// fileSHA256 计算文件哈希。
func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := copyN(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
