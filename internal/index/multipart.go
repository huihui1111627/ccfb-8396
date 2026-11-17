package index

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ---------------- multipart uploads ----------------

// CreateMPUpload registers a new in-progress multipart upload.
func (x *Index) CreateMPUpload(up MPUpload) error {
	raw, err := json.Marshal(up)
	if err != nil {
		return err
	}
	return x.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bkMPs).Put([]byte(up.UploadID), raw)
	})
}

// GetMPUpload fetches an upload descriptor.
func (x *Index) GetMPUpload(uploadID string) (*MPUpload, error) {
	var up MPUpload
	err := x.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bkMPs).Get([]byte(uploadID))
		if v == nil {
			return fmt.Errorf("%w: upload %s", ErrNotFound, uploadID)
		}
		return json.Unmarshal(v, &up)
	})
	if err != nil {
		return nil, err
	}
	return &up, nil
}

// PutMPart stores one completed part.
func (x *Index) PutMPart(uploadID string, p PartMeta) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return x.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bkMPParts).Put(mpPartKey(uploadID, p.PartNumber), raw)
	})
}

// GetMPart fetches one part's metadata.
func (x *Index) GetMPart(uploadID string, partNumber int) (*PartMeta, error) {
	var p PartMeta
	err := x.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bkMPParts).Get(mpPartKey(uploadID, partNumber))
		if v == nil {
			return fmt.Errorf("%w: part %d", ErrNotFound, partNumber)
		}
		return json.Unmarshal(v, &p)
	})
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// ListMParts returns all stored parts of an upload ordered by part number.
func (x *Index) ListMParts(uploadID string) ([]PartMeta, error) {
	var out []PartMeta
	prefix := []byte(uploadID + "\x00")
	err := x.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bkMPParts).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytesHasPrefix(k, prefix); k, v = c.Next() {
			var p PartMeta
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			out = append(out, p)
		}
		return nil
	})
	return out, err
}

// DeleteMPUpload removes an upload and all of its part records.
func (x *Index) DeleteMPUpload(uploadID string) (*MPUpload, error) {
	var removed MPUpload
	err := x.db.Update(func(tx *bolt.Tx) error {
		ups := tx.Bucket(bkMPs)
		raw := ups.Get([]byte(uploadID))
		if raw == nil {
			return fmt.Errorf("%w: upload %s", ErrNotFound, uploadID)
		}
		if err := json.Unmarshal(raw, &removed); err != nil {
			return err
		}
		if err := ups.Delete([]byte(uploadID)); err != nil {
			return err
		}
		parts := tx.Bucket(bkMPParts)
		prefix := []byte(uploadID + "\x00")
		c := parts.Cursor()
		var keys [][]byte
		for k, _ := c.Seek(prefix); k != nil && bytesHasPrefix(k, prefix); k, _ = c.Next() {
			keys = append(keys, append([]byte(nil), k...))
		}
		for _, k := range keys {
			if err := parts.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &removed, nil
}

// ListMPUploads returns all in-progress uploads, optionally filtered by
// bucket and key (S3 ListMultipartUploads scoping).
func (x *Index) ListMPUploads(bucket, key string) ([]MPUpload, error) {
	var out []MPUpload
	err := x.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bkMPs).ForEach(func(_, v []byte) error {
			var up MPUpload
			if err := json.Unmarshal(v, &up); err != nil {
				return err
			}
			if bucket != "" && up.Bucket != bucket {
				return nil
			}
			if key != "" && up.Key != key {
				return nil
			}
			out = append(out, up)
			return nil
		})
	})
	return out, err
}

// TouchMPUpload overwrites an upload's creation timestamp (test/aging support).
func (x *Index) TouchMPUpload(uploadID string, at time.Time) error {
	return x.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bkMPs)
		raw := b.Get([]byte(uploadID))
		if raw == nil {
			return fmt.Errorf("%w: upload %s", ErrNotFound, uploadID)
		}
		var up MPUpload
		if err := json.Unmarshal(raw, &up); err != nil {
			return err
		}
		up.CreatedAt = at
		out, err := json.Marshal(up)
		if err != nil {
			return err
		}
		return b.Put([]byte(uploadID), out)
	})
}

// OldMPUploads returns uploads created before cutoff (lifecycle cleanup).
func (x *Index) OldMPUploads(cutoff time.Time) ([]MPUpload, error) {
	var out []MPUpload
	err := x.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bkMPs).ForEach(func(_, v []byte) error {
			var up MPUpload
			if err := json.Unmarshal(v, &up); err != nil {
				return err
			}
			if up.CreatedAt.Before(cutoff) {
				out = append(out, up)
			}
			return nil
		})
	})
	return out, err
}

// ---------------- lifecycle rules ----------------

func lcKey(ruleID string) []byte { return []byte(ruleID) }

// PutLifecycleRule stores (or replaces) a lifecycle rule.
func (x *Index) PutLifecycleRule(r LifecycleRule) error {
	if r.Status == "" {
		r.Status = "Enabled"
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now().UTC()
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return x.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bkLCRules).Put(lcKey(r.ID), raw)
	})
}

// GetLifecycleRule fetches one rule.
func (x *Index) GetLifecycleRule(id string) (*LifecycleRule, error) {
	var r LifecycleRule
	err := x.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bkLCRules).Get(lcKey(id))
		if v == nil {
			return fmt.Errorf("%w: rule %s", ErrNotFound, id)
		}
		return json.Unmarshal(v, &r)
	})
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// ListLifecycleRules returns rules, optionally restricted to one bucket.
func (x *Index) ListLifecycleRules(bucket string) ([]LifecycleRule, error) {
	var out []LifecycleRule
	err := x.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bkLCRules).ForEach(func(_, v []byte) error {
			var r LifecycleRule
			if err := json.Unmarshal(v, &r); err != nil {
				return err
			}
			if bucket == "" || r.Bucket == bucket {
				out = append(out, r)
			}
			return nil
		})
	})
	return out, err
}

// DeleteLifecycleRule removes a rule and returns it if it existed.
func (x *Index) DeleteLifecycleRule(id string) (*LifecycleRule, error) {
	var removed LifecycleRule
	err := x.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bkLCRules)
		raw := b.Get(lcKey(id))
		if raw == nil {
			return fmt.Errorf("%w: rule %s", ErrNotFound, id)
		}
		if err := json.Unmarshal(raw, &removed); err != nil {
			return err
		}
		return b.Delete(lcKey(id))
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, err
		}
		return nil, err
	}
	return &removed, nil
}

// NextTaskID returns a monotonically increasing counter (used for repair job ids).
func (x *Index) NextTaskID(name string) (int64, error) {
	var id int64
	err := x.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bkCounters)
		key := []byte("task-" + name)
		raw := b.Get(key)
		if raw != nil {
			id = int64(binary.BigEndian.Uint64(raw))
		}
		id++
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(id))
		return b.Put(key, buf)
	})
	return id, err
}

// AllExpiredObjects lists objects matching an enabled rule whose age exceeds
// its expiration. The callback may stop iteration with an error.
func (x *Index) AllExpiredObjects(now time.Time, fn func(meta ObjectMeta, rule LifecycleRule) error) error {
	rules, err := x.ListLifecycleRules("")
	if err != nil {
		return err
	}
	var enabled []LifecycleRule
	for _, r := range rules {
		if r.Status == "Enabled" && r.ExpireDays > 0 {
			enabled = append(enabled, r)
		}
	}
	if len(enabled) == 0 {
		return nil
	}
	return x.ScanObjects(func(meta ObjectMeta) error {
		for _, r := range enabled {
			if r.Bucket != meta.Bucket {
				continue
			}
			if r.Prefix != "" && !bytesHasPrefix([]byte(meta.Key), []byte(r.Prefix)) {
				continue
			}
			cutoff := now.Add(-time.Duration(r.ExpireDays) * 24 * time.Hour)
			if meta.CreatedAt.Before(cutoff) {
				if err := fn(meta, r); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
