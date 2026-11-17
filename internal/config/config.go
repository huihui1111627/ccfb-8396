// Package config defines the on-disk configuration format for an imstore node.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Config is the full node configuration loaded from a JSON file.
type Config struct {
	// Listen is the address that serves both the S3 API and the console.
	Listen string `json:"listen"`
	// ExternalURL is the URL clients use to reach this node (used for presigned URLs).
	ExternalURL string `json:"external_url"`
	// Region is the S3 region name used in SigV4 validation.
	Region string `json:"region"`
	// AccessKey / SecretKey are the S3 credentials.
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	// ConsoleToken guards the management console API.
	ConsoleToken string `json:"console_token"`

	// DataShards / ParityShards define the Reed-Solomon layout (e.g. 4+2).
	DataShards   int `json:"data_shards"`
	ParityShards int `json:"parity_shards"`
	// StripeSize is the size of the original data per EC stripe.
	// Shard size per stripe = StripeSize / DataShards and must divide evenly.
	StripeSize int64 `json:"stripe_size"`

	// MetaDB is the bbolt file holding object metadata.
	MetaDB string `json:"meta_db"`

	// Volumes is the list of simulated disks. Each local directory is one disk.
	Volumes []Volume `json:"volumes"`

	// Repair configures background reconstruction.
	Repair RepairConfig `json:"repair"`

	// ScrubIntervalSeconds is how often a full bit-rot scrub runs (0 disables).
	ScrubIntervalSeconds int `json:"scrub_interval_seconds"`

	// MultipartMaxPartHours is the age after which an incomplete multipart
	// upload is aborted by the lifecycle worker.
	MultipartMaxPartHours int `json:"multipart_max_part_hours"`
}

// Volume describes one simulated physical disk (a local directory).
type Volume struct {
	ID        string `json:"id"`
	Path      string `json:"path"`
	Enclosure string `json:"enclosure"`
}

// RepairConfig controls rebuild throttling.
type RepairConfig struct {
	// MaxBytesPerSecond caps total rebuild IO (read+write). 0 = unlimited.
	MaxBytesPerSecond int64 `json:"max_bytes_per_second"`
	// YieldWhenBusy, when true, pauses rebuilding while foreground IO is active.
	YieldWhenBusy bool `json:"yield_when_busy"`
	// IntervalSeconds is how often the repair loop scans for degraded objects.
	IntervalSeconds int `json:"interval_seconds"`
}

// Load reads and validates a configuration file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	c.applyDefaults()
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8333"
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.ExternalURL == "" {
		c.ExternalURL = "http://127.0.0.1" + c.Listen
	}
	if c.StripeSize == 0 {
		c.StripeSize = 1 << 20 // 1 MiB per stripe -> 256 KiB per shard at 4+2
	}
	if c.Repair.IntervalSeconds == 0 {
		c.Repair.IntervalSeconds = 10
	}
	if c.Repair.MaxBytesPerSecond == 0 {
		c.Repair.MaxBytesPerSecond = 8 << 20 // 8 MiB/s default so rebuilds stay gentle
	}
	if c.ScrubIntervalSeconds == 0 {
		c.ScrubIntervalSeconds = 3600
	}
	if c.MultipartMaxPartHours == 0 {
		c.MultipartMaxPartHours = 24
	}
}

// Validate checks the configuration for internal consistency.
func (c *Config) Validate() error {
	if c.AccessKey == "" || c.SecretKey == "" {
		return errors.New("access_key and secret_key are required")
	}
	if c.ConsoleToken == "" {
		return errors.New("console_token is required")
	}
	if c.DataShards < 1 || c.ParityShards < 1 {
		return errors.New("data_shards and parity_shards must be >= 1")
	}
	if len(c.Volumes) < c.DataShards+c.ParityShards {
		return fmt.Errorf("need at least %d volumes for %d+%d erasure coding, have %d",
			c.DataShards+c.ParityShards, c.DataShards, c.ParityShards, len(c.Volumes))
	}
	if c.StripeSize != 0 && c.StripeSize%int64(c.DataShards) != 0 {
		return fmt.Errorf("stripe_size %d must be divisible by data_shards %d", c.StripeSize, c.DataShards)
	}
	if c.MetaDB == "" {
		return errors.New("meta_db path is required")
	}
	seenID := map[string]bool{}
	seenPath := map[string]bool{}
	enclosures := map[string]bool{}
	for i, v := range c.Volumes {
		if v.ID == "" {
			return fmt.Errorf("volume[%d] missing id", i)
		}
		if v.Path == "" {
			return fmt.Errorf("volume %s missing path", v.ID)
		}
		if v.Enclosure == "" {
			return fmt.Errorf("volume %s missing enclosure", v.ID)
		}
		if seenID[v.ID] {
			return fmt.Errorf("duplicate volume id %q", v.ID)
		}
		if seenPath[filepath.Clean(v.Path)] {
			return fmt.Errorf("duplicate volume path %q", v.Path)
		}
		seenID[v.ID] = true
		seenPath[filepath.Clean(v.Path)] = true
		enclosures[v.Enclosure] = true
	}
	if len(enclosures) < c.DataShards+c.ParityShards {
		return fmt.Errorf("placement rule requires >= %d distinct enclosures (fault domains), have %d",
			c.DataShards+c.ParityShards, len(enclosures))
	}
	return nil
}

// TotalShards returns data + parity shard count.
func (c *Config) TotalShards() int { return c.DataShards + c.ParityShards }

// ShardBytes returns the per-shard size of one stripe.
func (c *Config) ShardBytes() int64 {
	if c.StripeSize == 0 {
		return (1 << 20) / int64(c.DataShards)
	}
	return c.StripeSize / int64(c.DataShards)
}
