package placement_test

import (
	"path/filepath"
	"testing"

	"imstore/internal/placement"
	"imstore/internal/volume"
)

func TestPlacementDistinctEnclosuresAndDisks(t *testing.T) {
	// 8 volumes across 6 enclosures; two enclosures hold 2 disks.
	spec := []struct{ id, enc string }{
		{"v0", "e0"}, {"v1", "e1"}, {"v2", "e2"}, {"v3", "e3"},
		{"v4", "e4"}, {"v5", "e5"}, {"v6", "e0"}, {"v7", "e1"},
	}
	var vols []*volume.Volume
	dir := t.TempDir()
	for _, s := range spec {
		v, err := volume.New(s.id, filepath.Join(dir, s.id), s.enc)
		if err != nil {
			t.Fatal(err)
		}
		vols = append(vols, v)
	}
	p := placement.NewPlanner()

	for iter := 0; iter < 20; iter++ {
		ids, err := p.Plan(vols, 6)
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if len(ids) != 6 {
			t.Fatalf("got %d placements, want 6", len(ids))
		}
		seenDisk := map[string]bool{}
		seenEnc := map[string]bool{}
		for _, id := range ids {
			if seenDisk[id] {
				t.Fatalf("disk %s chosen twice in one placement", id)
			}
			seenDisk[id] = true
			var enc string
			for _, v := range vols {
				if v.ID == id {
					enc = v.Enclosure
				}
			}
			if seenEnc[enc] {
				t.Fatalf("enclosure %s hosts two shards of the same object", enc)
			}
			seenEnc[enc] = true
		}
	}
}

func TestPlacementFailsWhenFaultDomainsOffline(t *testing.T) {
	dir := t.TempDir()
	var vols []*volume.Volume
	for i := 0; i < 6; i++ {
		v, _ := volume.New(string(rune('a'+i)), filepath.Join(dir, string(rune('a'+i))), string(rune('a'+i)))
		vols = append(vols, v)
	}
	// Force half the enclosures offline.
	for i := 0; i < 3; i++ {
		vols[i].SetOffline("test")
	}
	healthy := []*volume.Volume{}
	for _, v := range vols {
		if v.Status() == volume.StatusOnline {
			healthy = append(healthy, v)
		}
	}
	p := placement.NewPlanner()
	if _, err := p.Plan(healthy, 6); err == nil {
		t.Fatal("expected failure with only 3 healthy fault domains")
	}
}
