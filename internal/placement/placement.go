// Package placement 实现基于故障域的分片放置算法。
//
// 规则：
//  1. 同一对象的任意两个分片不能落在同一块盘；
//  2. 尽量不落在同一个盘柜（enclosure）；strict 模式下盘柜不足直接报错；
//  3. 候选盘优先选择剩余空间最多的盘，均衡容量。
package placement

import (
	"errors"
	"fmt"
	"sort"

	"erasurestore/internal/disk"
)

// ErrNoEnoughDisks 在线磁盘不足以放置全部分片。
var ErrNoEnoughDisks = errors.New("在线磁盘数量不足")

// Planner 分片放置器。
type Planner struct {
	manager         *disk.Manager
	strictEnclosure bool
}

// NewPlanner 创建放置器。
func NewPlanner(m *disk.Manager, strictEnclosure bool) *Planner {
	return &Planner{manager: m, strictEnclosure: strictEnclosure}
}

// candidate 放置候选盘及其打分。
type candidate struct {
	d         *disk.Disk
	freeBytes uint64
	enc       string
}

// Plan 为 total 个分片选择磁盘，返回磁盘 ID 列表（顺序即 shardIndex 顺序）。
// excludeIDs 中的盘不参与选择（重建场景用来避开已有健康分片所在盘）。
func (p *Planner) Plan(total int, excludeIDs map[string]bool) ([]*disk.Disk, error) {
	all := p.manager.Online()
	cands := make([]candidate, 0, len(all))
	for _, d := range all {
		if excludeIDs != nil && excludeIDs[d.ID()] {
			continue
		}
		st := d.Stats()
		cands = append(cands, candidate{d: d, freeBytes: st.FreeBytes, enc: d.Enclosure()})
	}
	if len(cands) < total {
		return nil, fmt.Errorf("%w: 需要 %d 块，可用 %d 块", ErrNoEnoughDisks, total, len(cands))
	}

	// 贪心：每轮选一块，优先选“尚未使用的盘柜”，同条件下剩余空间最多者优先。
	usedDisks := map[string]bool{}
	usedEncs := map[string]bool{}
	picked := make([]*disk.Disk, 0, total)

	for len(picked) < total {
		// 先在新盘柜里找
		best := pickBest(cands, usedDisks, usedEncs, true)
		if best == nil {
			if p.strictEnclosure {
				return nil, fmt.Errorf("%w: strict 模式下盘柜数量不足，需要 %d 个不同盘柜", ErrNoEnoughDisks, total)
			}
			// 退化：允许同盘柜，但仍不允许同盘
			best = pickBest(cands, usedDisks, usedEncs, false)
		}
		if best == nil {
			return nil, fmt.Errorf("%w: 找不到满足故障域约束的磁盘", ErrNoEnoughDisks)
		}
		picked = append(picked, best.d)
		usedDisks[best.d.ID()] = true
		usedEncs[best.enc] = true
	}
	return picked, nil
}

// pickBest 在候选中挑出最优盘。newEnclosureOnly=true 时只考虑未使用过的盘柜。
func pickBest(cands []candidate, usedDisks, usedEncs map[string]bool, newEnclosureOnly bool) *candidate {
	filtered := make([]candidate, 0, len(cands))
	for _, c := range cands {
		if usedDisks[c.d.ID()] {
			continue
		}
		if newEnclosureOnly && usedEncs[c.enc] {
			continue
		}
		filtered = append(filtered, c)
	}
	if len(filtered) == 0 {
		return nil
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].freeBytes != filtered[j].freeBytes {
			return filtered[i].freeBytes > filtered[j].freeBytes
		}
		return filtered[i].d.ID() < filtered[j].d.ID()
	})
	return &filtered[0]
}
