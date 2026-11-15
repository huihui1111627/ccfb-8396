// Package syncer orchestrates sync batches: schema evolution, diff, and
// transactional apply, with crash-safe resume driven by the meta store's
// per-table batch state machine (pending -> diffed -> done).
package syncer

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"pmssync/internal/diff"
	"pmssync/internal/rowhash"
	"pmssync/internal/source"
	"pmssync/internal/store"
	"pmssync/internal/target"
)

type Syncer struct {
	St *store.Store
	Tg *target.Target

	mu      sync.Mutex
	running map[int64]bool // sourceID -> batch in flight
}

func New(st *store.Store, tg *target.Target) *Syncer {
	return &Syncer{St: st, Tg: tg, running: map[int64]bool{}}
}

// RunBatch starts a new batch for a source and runs it to completion.
// Returns the batch ID. Only one batch per source at a time.
func (s *Syncer) RunBatch(sourceID int64) (int64, error) {
	s.mu.Lock()
	if s.running[sourceID] {
		s.mu.Unlock()
		return 0, fmt.Errorf("source %d already has a batch running", sourceID)
	}
	s.running[sourceID] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.running, sourceID)
		s.mu.Unlock()
	}()

	batchID, err := s.resumeOrNew(sourceID)
	if err != nil {
		return 0, err
	}
	return batchID, s.runBatch(batchID)
}

// resumeOrNew continues an unfinished batch if one exists (its diff results
// and key state are already persisted), otherwise creates a fresh batch.
func (s *Syncer) resumeOrNew(sourceID int64) (int64, error) {
	if b, err := s.St.UnfinishedBatch(sourceID); err == nil && b != nil {
		if err := s.St.ReopenBatch(b.ID); err != nil {
			return 0, err
		}
		return b.ID, nil
	}
	return s.St.NewBatch(sourceID)
}

// ResumeAll continues batches left in 'running' state by a crash/kill.
func (s *Syncer) ResumeAll() {
	batches, err := s.St.RunningBatches()
	if err != nil {
		log.Printf("resume: list running batches: %v", err)
		return
	}
	for _, b := range batches {
		log.Printf("resume: continuing batch %d (source %d)", b.ID, b.SourceID)
		s.mu.Lock()
		s.running[b.SourceID] = true
		s.mu.Unlock()
		err := s.runBatch(b.ID)
		s.mu.Lock()
		delete(s.running, b.SourceID)
		s.mu.Unlock()
		if err != nil {
			log.Printf("resume: batch %d failed: %v", b.ID, err)
		}
	}
}

// runBatch executes (or continues) a batch. Idempotent per table:
//   - pending/failed -> run diff (persisted atomically), state -> diffed
//   - diffed         -> apply to DuckDB in one transaction, state -> done
func (s *Syncer) runBatch(batchID int64) error {
	batch, err := s.St.GetBatch(batchID)
	if err != nil {
		return err
	}
	src, err := s.St.GetSource(batch.SourceID)
	if err != nil {
		return err
	}
	sdb, err := source.Open(src.Path)
	if err != nil {
		s.fail(batch, err)
		return err
	}
	defer sdb.Close()

	tables, err := s.St.ListTables(src.ID)
	if err != nil {
		s.fail(batch, err)
		return err
	}
	for i := range tables {
		t := &tables[i]
		if !t.Enabled {
			continue
		}
		if err := s.St.EnsureBatchTable(batchID, t.ID); err != nil {
			s.fail(batch, err)
			return err
		}
	}
	btStatus := map[int64]string{}
	if bts, err := s.St.ListBatchTables(batchID); err == nil {
		for _, bt := range bts {
			btStatus[bt.TableID] = bt.Status
		}
	}

	for i := range tables {
		t := &tables[i]
		if !t.Enabled {
			continue
		}
		status := btStatus[t.ID]
		if status == "done" {
			continue
		}
		if err := s.syncTable(sdb, batchID, t, status); err != nil {
			// Keep 'diffed' state intact so resume retries the apply (the
			// persisted changes are still valid); only mark hard failures
			// that happened before the diff was persisted.
			mark := status
			if mark != "diffed" {
				mark = "failed"
			}
			_ = s.St.SetBatchTableStatus(batchID, t.ID, mark, err.Error())
			s.fail(batch, fmt.Errorf("table %s: %w", t.Name, err))
			return err
		}
	}
	if err := s.St.FinishBatch(batchID, "done", ""); err != nil {
		return err
	}
	return s.St.SourceSynced(src.ID, batchID)
}

func (s *Syncer) syncTable(sdb *source.DB, batchID int64, t *store.Table, status string) error {
	cols, err := sdb.TableSchema(t.Name)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("table %s not found or has no columns", t.Name)
	}
	// resolve key columns for pk mode
	if t.KeyMode == "pk" && len(t.KeyCols) == 0 {
		t.KeyCols = source.PKCols(cols)
		if len(t.KeyCols) == 0 {
			return fmt.Errorf("表 %s 没有主键，请在配置中指定业务键或改用整行指纹模式", t.Name)
		}
	}
	nsig := source.NameSig(cols)
	tsig := source.TypeSig(cols, target.DuckType)
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	newSig := nsig + "|" + tsig + "|" + strings.Join(names, ",")
	if newSig != t.SchemaSig {
		if err := s.handleSchemaChange(sdb, t, cols, newSig); err != nil {
			return err
		}
	}
	if status != "diffed" {
		if _, _, _, err := diff.Run(s.St, sdb, t, batchID); err != nil {
			return err
		}
	}
	changes, err := s.St.LoadChanges(batchID, t.ID)
	if err != nil {
		return err
	}
	if err := s.Tg.ApplyChanges(t.SourceID, t.Name, cols, changes, batchID, time.Now()); err != nil {
		return err
	}
	return s.St.SetBatchTableStatus(batchID, t.ID, "done", "")
}

func (s *Syncer) fail(batch *store.Batch, err error) {
	_ = s.St.FinishBatch(batch.ID, "failed", err.Error())
	_ = s.St.SourceFailed(batch.SourceID)
}

// handleSchemaChange evolves the target DDL and migrates the hash basis:
//   - type-only changes: evolve DDL, hashes unaffected (value-based hashing)
//   - added columns: shift every stored row hash by the constant default-value
//     delta (additive hashing makes this exact, no source rescan needed)
//   - dropped columns: old per-row values are unknowable, so re-baseline by
//     rescanning the source (data changes in the same batch window would be
//     absorbed into the baseline — a documented limitation)
func (s *Syncer) handleSchemaChange(sdb *source.DB, t *store.Table, cols []source.Col, newSig string) error {
	if err := s.Tg.EnsureTable(t.SourceID, t.Name, cols); err != nil {
		return err
	}
	if t.SchemaSig == "" {
		// first sync: table just created, no state to migrate
		return s.St.SetTableSchemaSig(t.ID, newSig)
	}
	parts := strings.SplitN(t.SchemaSig, "|", 3)
	if len(parts) != 3 {
		// unknown legacy signature: safest is a full re-baseline
		if err := diff.Rebaseline(s.St, sdb, t); err != nil {
			return err
		}
		return s.St.SetTableSchemaSig(t.ID, newSig)
	}
	oldNames := map[string]bool{}
	for _, n := range strings.Split(parts[2], ",") {
		if n != "" {
			oldNames[n] = true
		}
	}
	newNames := map[string]bool{}
	var added []source.Col
	for _, c := range cols {
		newNames[c.Name] = true
		if !oldNames[c.Name] {
			added = append(added, c)
		}
	}
	dropped := false
	for n := range oldNames {
		if !newNames[n] {
			dropped = true
			break
		}
	}
	if dropped {
		if err := diff.Rebaseline(s.St, sdb, t); err != nil {
			return err
		}
	} else if len(added) > 0 {
		var delta uint64
		for _, c := range added {
			delta += rowhash.ColHash(c.Name, c.Default)
		}
		if err := s.St.AdjustHashes(t.ID, delta); err != nil {
			return err
		}
	}
	return s.St.SetTableSchemaSig(t.ID, newSig)
}
