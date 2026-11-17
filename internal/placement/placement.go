// Package placement chooses which volumes hold the shards of one object.
//
// The hard rule (requirement 3) is that no two shards of the same object land
// on the same disk or the same enclosure. Placement therefore first selects N
// distinct enclosures, then one volume inside each. Enclosure selection is
// least-loaded with a random tie-break so that objects written back to back
// do not all pile onto the same enclosure set.
package placement

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"imstore/internal/volume"
)

// ErrNotEnoughFaultDomains is returned when fewer than n distinct enclosures
// are currently available.
var ErrNotEnoughFaultDomains = errors.New("placement: not enough healthy fault domains")

// Planner performs placement decisions against the volume manager.
type Planner struct{}

// NewPlanner constructs a Planner.
func NewPlanner() *Planner { return &Planner{} }

// Plan returns exactly n volume ids, each from a distinct enclosure.
// Load balancing: enclosures are ordered by (total used bytes, shard count)
// ascending, with equal-load candidates shuffled using crypto/rand; the same
// ordering is applied when choosing a volume within an enclosure.
func (p *Planner) Plan(vols []*volume.Volume, n int) ([]string, error) {
	// Group healthy volumes by enclosure.
	byEnclosure := map[string][]*volume.Volume{}
	for _, v := range vols {
		if v.Status() != volume.StatusOnline {
			continue
		}
		byEnclosure[v.Enclosure] = append(byEnclosure[v.Enclosure], v)
	}
	if len(byEnclosure) < n {
		return nil, fmt.Errorf("%w: need %d enclosures, %d healthy",
			ErrNotEnoughFaultDomains, n, len(byEnclosure))
	}

	type group struct {
		enclosure string
		vols      []*volume.Volume
		load      int64
		shards    int64
	}
	groups := make([]*group, 0, len(byEnclosure))
	for enc, members := range byEnclosure {
		g := &group{enclosure: enc, vols: members}
		for _, v := range members {
			snap := v.Snapshot()
			g.load += snap.UsedBytes
			g.shards += snap.ShardCount
		}
		groups = append(groups, g)
	}

	// Deterministic ordering by load, then shard count; tie groups get
	// shuffled randomly afterwards so equally-loaded enclosures share work.
	sort.SliceStable(groups, func(i, j int) bool {
		if groups[i].load != groups[j].load {
			return groups[i].load < groups[j].load
		}
		return groups[i].shards < groups[j].shards
	})
	shuffleEqual(groups, func(g *group) [2]int64 { return [2]int64{g.load, g.shards} })

	picked := make([]string, 0, n)
	usedEnclosures := map[string]bool{}
	for _, g := range groups {
		if len(picked) == n {
			break
		}
		if usedEnclosures[g.enclosure] {
			continue
		}
		members := g.vols
		sort.SliceStable(members, func(i, j int) bool {
			si, sj := members[i].Snapshot(), members[j].Snapshot()
			if si.UsedBytes != sj.UsedBytes {
				return si.UsedBytes < sj.UsedBytes
			}
			return si.ShardCount < sj.ShardCount
		})
		shuffleEqualVols(members)
		picked = append(picked, members[0].ID)
		usedEnclosures[g.enclosure] = true
	}
	if len(picked) != n {
		return nil, fmt.Errorf("%w: could only satisfy %d of %d shards",
			ErrNotEnoughFaultDomains, len(picked), n)
	}
	return picked, nil
}

// shuffleEqual randomises the order of adjacent elements with equal keys.
func shuffleEqual[T any](items []T, key func(T) [2]int64) {
	i := 0
	for i < len(items) {
		j := i + 1
		ki := key(items[i])
		for j < len(items) && key(items[j]) == ki {
			j++
		}
		if j-i > 1 {
			cryptoShuffle(items[i:j])
		}
		i = j
	}
}

func shuffleEqualVols(vols []*volume.Volume) {
	i := 0
	for i < len(vols) {
		si := vols[i].Snapshot()
		j := i + 1
		for j < len(vols) {
			sj := vols[j].Snapshot()
			if sj.UsedBytes != si.UsedBytes || sj.ShardCount != si.ShardCount {
				break
			}
			j++
		}
		if j-i > 1 {
			cryptoShuffle(vols[i:j])
		}
		i = j
	}
}

func cryptoShuffle[T any](items []T) {
	for i := len(items) - 1; i > 0; i-- {
		jBig, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			// rand.Reader failures are extraordinarily unlikely; fall back to
			// leaving the segment in deterministic order rather than crashing.
			return
		}
		j := int(jBig.Int64())
		items[i], items[j] = items[j], items[i]
	}
}
