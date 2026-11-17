package volume

import (
	"fmt"
	"sync"
)

// Manager owns all volumes of a node and provides lookup helpers.
type Manager struct {
	mu      sync.RWMutex
	volumes map[string]*Volume
	order   []string // stable iteration order = config order
}

// NewManager builds a manager from already-created volumes.
func NewManager(vols []*Volume) *Manager {
	m := &Manager{
		volumes: make(map[string]*Volume, len(vols)),
		order:   make([]string, 0, len(vols)),
	}
	for _, v := range vols {
		m.volumes[v.ID] = v
		m.order = append(m.order, v.ID)
	}
	return m
}

// Get returns the volume with the given id.
func (m *Manager) Get(id string) (*Volume, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.volumes[id]
	if !ok {
		return nil, fmt.Errorf("volume: unknown volume %q", id)
	}
	return v, nil
}

// All returns volumes in config order.
func (m *Manager) All() []*Volume {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Volume, 0, len(m.order))
	for _, id := range m.order {
		out = append(out, m.volumes[id])
	}
	return out
}

// Healthy returns volumes currently accepting IO, in config order.
func (m *Manager) Healthy() []*Volume {
	all := m.All()
	out := make([]*Volume, 0, len(all))
	for _, v := range all {
		if v.Status() == StatusOnline {
			out = append(out, v)
		}
	}
	return out
}

// Snapshot returns stats for every volume.
func (m *Manager) Snapshot() []Stats {
	all := m.All()
	out := make([]Stats, 0, len(all))
	for _, v := range all {
		out = append(out, v.Snapshot())
	}
	return out
}
