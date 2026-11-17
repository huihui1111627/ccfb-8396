// Package config 定义服务端配置。
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// DiskConfig 描述一块由本地目录模拟的存储盘。
type DiskConfig struct {
	ID        string `json:"id"`
	Path      string `json:"path"`
	Enclosure string `json:"enclosure"` // 盘柜/故障域标识
}

// Config 是整个服务的配置。
type Config struct {
	DataShards   int          `json:"data_shards"`
	ParityShards int          `json:"parity_shards"`
	StripeSize   int64        `json:"stripe_size"` // 单 stripe 逻辑数据大小（字节）
	S3Addr       string       `json:"s3_addr"`
	ConsoleAddr  string       `json:"console_addr"`
	ConsoleToken string       `json:"console_token"`
	AccessKey    string       `json:"access_key"`
	SecretKey    string       `json:"secret_key"`
	Region       string       `json:"region"`
	Disks        []DiskConfig `json:"disks"`
	MetaDBPath   string       `json:"meta_db_path"`
	// 放置策略：strict=true 时，分片不允许落在同一盘柜（盘柜不足则请求失败）；
	// false（默认）时尽量分散到不同盘柜，不足时退化为不共盘。
	StrictEnclosure bool `json:"strict_enclosure"`
	// 修复默认限速，字节/秒；0 表示不限速。可在控制台动态调整。
	RepairBytesPerSec int64 `json:"repair_bytes_per_sec"`
}

// Load 从 JSON 文件读取配置。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := Default()
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Default 返回带默认值的配置（供测试直接构造）。
func Default() *Config {
	return &Config{
		DataShards:        4,
		ParityShards:      2,
		StripeSize:        1 << 20, // 1 MiB
		S3Addr:            ":9000",
		ConsoleAddr:       ":9001",
		ConsoleToken:      "admin123",
		AccessKey:         "testaccesskey",
		SecretKey:         "testsecretkey",
		Region:            "us-east-1",
		RepairBytesPerSec: 32 << 20, // 32 MiB/s
	}
}

// Validate 校验配置合法性。
func (c *Config) Validate() error {
	if c.DataShards <= 0 || c.ParityShards <= 0 {
		return fmt.Errorf("data_shards/parity_shards 必须为正数")
	}
	if c.StripeSize <= 0 {
		return fmt.Errorf("stripe_size 必须为正数")
	}
	if len(c.Disks) < c.DataShards+c.ParityShards {
		return fmt.Errorf("磁盘数 %d 少于 data+parity=%d", len(c.Disks), c.DataShards+c.ParityShards)
	}
	ids := map[string]bool{}
	paths := map[string]bool{}
	for _, d := range c.Disks {
		if d.ID == "" || d.Path == "" {
			return fmt.Errorf("磁盘的 id 和 path 不能为空")
		}
		if ids[d.ID] {
			return fmt.Errorf("磁盘 id 重复: %s", d.ID)
		}
		if paths[d.Path] {
			return fmt.Errorf("磁盘 path 重复: %s", d.Path)
		}
		ids[d.ID] = true
		paths[d.Path] = true
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return fmt.Errorf("access_key/secret_key 不能为空")
	}
	return nil
}
