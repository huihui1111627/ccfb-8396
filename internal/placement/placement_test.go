package placement_test

import (
	"path/filepath"
	"testing"

	"erasurestore/internal/config"
	"erasurestore/internal/disk"
	"erasurestore/internal/placement"
)

func newMgr(t *testing.T, disks []config.DiskConfig) *disk.Manager {
	t.Helper()
	for i := range disks {
		disks[i].Path = filepath.Join(t.TempDir(), disks[i].ID)
	}
	m, err := disk.NewManager(disks)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestPlanSpreadsAcrossDisksAndEnclosures(t *testing.T) {
	disks := []config.DiskConfig{
		{ID: "d1", Enclosure: "enc-a"}, {ID: "d2", Enclosure: "enc-a"},
		{ID: "d3", Enclosure: "enc-b"}, {ID: "d4", Enclosure: "enc-b"},
		{ID: "d5", Enclosure: "enc-c"}, {ID: "d6", Enclosure: "enc-c"},
	}
	m := newMgr(t, disks)
	p := placement.NewPlanner(m, false)

	got, err := p.Plan(6, nil)
	if err != nil {
		t.Fatal(err)
	}
	diskSet := map[string]bool{}
	encSet := map[string]int{}
	for _, d := range got {
		if diskSet[d.ID()] {
			t.Fatalf("分片共盘: %s", d.ID())
		}
		diskSet[d.ID()] = true
		encSet[d.Enclosure()]++
	}
	if len(got) != 6 {
		t.Fatalf("应放置 6 个分片，得到 %d", len(got))
	}
	// 3 个盘柜、6 分片，理想情况每个盘柜 2 个，至少不应有盘柜超过一半
	for enc, n := range encSet {
		if n > 3 {
			t.Fatalf("盘柜 %s 承载 %d 个分片，跨柜分散不足", enc, n)
		}
	}
}

func TestPlanExcludesGivenDisks(t *testing.T) {
	disks := []config.DiskConfig{
		{ID: "d1", Enclosure: "a"}, {ID: "d2", Enclosure: "a"},
		{ID: "d3", Enclosure: "b"}, {ID: "d4", Enclosure: "b"},
		{ID: "d5", Enclosure: "c"}, {ID: "d6", Enclosure: "c"},
		{ID: "d7", Enclosure: "d"},
	}
	m := newMgr(t, disks)
	p := placement.NewPlanner(m, false)
	got, err := p.Plan(2, map[string]bool{"d1": true, "d2": true, "d3": true, "d4": true, "d5": true})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range got {
		if d.ID() == "d1" || d.ID() == "d2" || d.ID() == "d3" || d.ID() == "d4" || d.ID() == "d5" {
			t.Fatalf("被排除的盘仍被选中: %s", d.ID())
		}
	}
}

func TestPlanNotEnoughDisks(t *testing.T) {
	disks := []config.DiskConfig{
		{ID: "d1", Enclosure: "a"}, {ID: "d2", Enclosure: "b"},
	}
	m := newMgr(t, disks)
	p := placement.NewPlanner(m, false)
	if _, err := p.Plan(6, nil); err == nil {
		t.Fatal("磁盘不足时应报错")
	}
}

func TestStrictEnclosure(t *testing.T) {
	// 只有 2 个盘柜却要放 3 个分片，strict 模式应失败
	disks := []config.DiskConfig{
		{ID: "d1", Enclosure: "a"}, {ID: "d2", Enclosure: "a"},
		{ID: "d3", Enclosure: "b"},
	}
	m := newMgr(t, disks)
	strict := placement.NewPlanner(m, true)
	if _, err := strict.Plan(3, nil); err == nil {
		t.Fatal("strict 模式盘柜不足应报错")
	}
	// 非 strict 模式允许同柜（只要不共盘）
	loose := placement.NewPlanner(m, false)
	if _, err := loose.Plan(3, nil); err != nil {
		t.Fatalf("非 strict 模式同柜应允许: %v", err)
	}
}
