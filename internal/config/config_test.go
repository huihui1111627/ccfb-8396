package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"imstore/internal/config"
)

func baseVolumes() []config.Volume {
	return []config.Volume{
		{ID: "v0", Path: "/tmp/v0", Enclosure: "e0"},
		{ID: "v1", Path: "/tmp/v1", Enclosure: "e1"},
		{ID: "v2", Path: "/tmp/v2", Enclosure: "e2"},
		{ID: "v3", Path: "/tmp/v3", Enclosure: "e3"},
		{ID: "v4", Path: "/tmp/v4", Enclosure: "e4"},
		{ID: "v5", Path: "/tmp/v5", Enclosure: "e5"},
	}
}

func validCfg() *config.Config {
	return &config.Config{
		Listen:       ":0",
		AccessKey:    "ak",
		SecretKey:    "sk",
		ConsoleToken: "tok",
		DataShards:   4,
		ParityShards: 2,
		StripeSize:   1 << 20,
		MetaDB:       filepath.Join(os.TempDir(), "meta.db"),
		Volumes:      baseVolumes(),
	}
}

func TestValidConfig(t *testing.T) {
	c := validCfg()
	if err := c.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if c.TotalShards() != 6 {
		t.Fatalf("total shards = %d, want 6", c.TotalShards())
	}
	if c.ShardBytes() != (1<<20)/4 {
		t.Fatalf("shard bytes = %d", c.ShardBytes())
	}
}

func TestRejectsMissingCredentials(t *testing.T) {
	c := validCfg()
	c.AccessKey = ""
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for missing access key")
	}
}

func TestRejectsTooFewVolumes(t *testing.T) {
	c := validCfg()
	c.Volumes = c.Volumes[:3]
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for too few volumes")
	}
}

func TestRejectsTooFewFaultDomains(t *testing.T) {
	c := validCfg()
	// Six volumes but only two distinct enclosures cannot satisfy the
	// "no two shards share an enclosure" placement rule.
	for i := range c.Volumes {
		c.Volumes[i].Enclosure = "shared"
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for insufficient fault domains")
	}
}

func TestRejectsUnalignedStripe(t *testing.T) {
	c := validCfg()
	c.StripeSize = 1000003 // not divisible by DataShards
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for unaligned stripe size")
	}
}

func TestRejectsDuplicateVolumeID(t *testing.T) {
	c := validCfg()
	c.Volumes[2].ID = c.Volumes[0].ID
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for duplicate volume id")
	}
}

func TestDefaultsApplied(t *testing.T) {
	c := validCfg()
	c.Listen = ""
	c.Region = ""
	c.StripeSize = 0
	c.MetaDB = filepath.Join(t.TempDir(), "meta.db")
	raw, _ := jsonMarshal(c)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Listen != ":8333" || loaded.Region != "us-east-1" {
		t.Fatal("defaults not applied")
	}
	if loaded.StripeSize != 1<<20 {
		t.Fatalf("default stripe size = %d", loaded.StripeSize)
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	c := validCfg()
	// Point meta db inside the temp dir so it is self-contained.
	c.MetaDB = filepath.Join(dir, "meta.db")
	raw, err := jsonMarshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.DataShards != 4 || loaded.ParityShards != 2 {
		t.Fatal("loaded config mismatch")
	}
}
