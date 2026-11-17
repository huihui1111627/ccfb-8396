// Package index is the metadata store for imstore. It wraps a bbolt database
// and maintains several key spaces:
//
//   - buckets:  one record per S3 bucket.
//   - objects:  "bucket\x00key" -> ObjectMeta. Sorted bbolt keys make prefix
//     listings and key-seek pagination O(log n + page size) even
//     with millions of objects.
//   - volobjs:  "volumeID\x00objectID" -> "" lets the repair worker find every
//     object that should have a shard on a given disk without a
//     full object scan.
//   - mps:      multipart upload descriptors.
//   - mpparts:  "uploadID\x00partNumber" -> PartMeta.
//   - lcrules:  per-bucket lifecycle rules.
//   - counters: repair task sequence numbers and similar singletons.
package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Bucket names.
var (
	bkObjects  = []byte("objects")
	bkVolObjs  = []byte("volobjs")
	bkBuckets  = []byte("buckets")
	bkMPs      = []byte("mps")
	bkMPParts  = []byte("mpparts")
	bkLCRules  = []byte("lcrules")
	bkCounters = []byte("counters")
)

// ErrNotFound is returned for missing metadata records.
var ErrNotFound = errors.New("index: not found")

// BucketInfo is the stored record for one S3 bucket.
type BucketInfo struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// PartMeta describes one completed segment of an object (a full write of one
// contiguous byte range of the final object).
type PartMeta struct {
	PartNumber  int      `json:"part_number"`
	ObjectID    string   `json:"object_id"` // on-disk shard object id for this part
	Size        int64    `json:"size"`
	ETag        string   `json:"etag"`
	SHA256      string   `json:"sha256"`
	StripeCount int      `json:"stripe_count"`
	VolumeIDs   []string `json:"volume_ids"` // TotalShards entries, by shard index
}

// ObjectMeta is the full metadata record of one stored object.
type ObjectMeta struct {
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	ObjectID    string            `json:"object_id"`
	Size        int64             `json:"size"`
	ETag        string            `json:"etag"`
	SHA256      string            `json:"sha256"`
	ContentType string            `json:"content_type"`
	UserMeta    map[string]string `json:"user_meta"`
	DataShards  int               `json:"data_shards"`
	ParityShds  int               `json:"parity_shards"`
	ShardSize   int64             `json:"shard_size"`
	StripeCount int               `json:"stripe_count"` // sum across parts; 1 for simple PUT
	VolumeIDs   []string          `json:"volume_ids"`   // for single-part objects
	Parts       []PartMeta        `json:"parts,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

// IsMultipart reports whether the object was assembled from multiple parts.
func (m ObjectMeta) IsMultipart() bool { return len(m.Parts) > 0 }

// AllParts returns the parts list (single synthetic part for simple PUTs).
func (m ObjectMeta) AllParts() []PartMeta {
	if len(m.Parts) > 0 {
		return m.Parts
	}
	return []PartMeta{{
		PartNumber:  1,
		ObjectID:    m.ObjectID,
		Size:        m.Size,
		ETag:        m.ETag,
		SHA256:      m.SHA256,
		StripeCount: m.StripeCount,
		VolumeIDs:   m.VolumeIDs,
	}}
}

// MPUpload is an in-progress multipart upload.
type MPUpload struct {
	UploadID    string            `json:"upload_id"`
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	ContentType string            `json:"content_type"`
	UserMeta    map[string]string `json:"user_meta"`
	CreatedAt   time.Time         `json:"created_at"`
}

// LifecycleRule is a per-bucket expiration rule.
type LifecycleRule struct {
	ID         string    `json:"id"`
	Bucket     string    `json:"bucket"`
	Prefix     string    `json:"prefix"`
	ExpireDays int       `json:"expire_days"`
	Status     string    `json:"status"` // Enabled | Disabled
	CreatedAt  time.Time `json:"created_at"`
}

// ListResult is one page of an object listing.
type ListResult struct {
	Objects        []ObjectMeta
	NextStartKey   string // opaque cursor: last key of this page
	Truncated      bool
	CommonPrefixes []string // synthetic "folder" entries when delimiter is set
}

// Index is the handle to the metadata database.
type Index struct {
	db *bolt.DB
}

// Open opens (or creates) the metadata database.
func Open(path string) (*Index, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("index: open %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bkObjects, bkVolObjs, bkBuckets, bkMPs, bkMPParts, bkLCRules, bkCounters} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Index{db: db}, nil
}

// Close releases the database.
func (x *Index) Close() error { return x.db.Close() }

// DB exposes the underlying bbolt handle for package-internal batch jobs.
func (x *Index) DB() *bolt.DB { return x.db }

func objectKey(bucket, key string) []byte {
	out := make([]byte, 0, len(bucket)+1+len(key))
	out = append(out, []byte(bucket)...)
	out = append(out, 0)
	out = append(out, []byte(key)...)
	return out
}

func volObjKey(volumeID, objectID string) []byte {
	out := make([]byte, 0, len(volumeID)+1+len(objectID))
	out = append(out, []byte(volumeID)...)
	out = append(out, 0)
	out = append(out, []byte(objectID)...)
	return out
}

func mpPartKey(uploadID string, partNumber int) []byte {
	return []byte(fmt.Sprintf("%s\x00%010d", uploadID, partNumber))
}

// ---------------- buckets ----------------

// CreateBucket stores a new bucket; it is an error if the name exists.
func (x *Index) CreateBucket(name string) error {
	return x.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bkBuckets)
		if b.Get([]byte(name)) != nil {
			return fmt.Errorf("index: bucket %q already exists", name)
		}
		info, err := json.Marshal(BucketInfo{Name: name, CreatedAt: time.Now().UTC()})
		if err != nil {
			return err
		}
		return b.Put([]byte(name), info)
	})
}

// DeleteBucket removes an empty bucket.
func (x *Index) DeleteBucket(name string) error {
	return x.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bkBuckets)
		if b.Get([]byte(name)) == nil {
			return fmt.Errorf("%w: bucket %s", ErrNotFound, name)
		}
		prefix := append([]byte(name), 0)
		c := tx.Bucket(bkObjects).Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytesHasPrefix(k, prefix); k, _ = c.Next() {
			return fmt.Errorf("index: bucket %s is not empty", name)
		}
		return b.Delete([]byte(name))
	})
}

// BucketExists reports whether a bucket exists.
func (x *Index) BucketExists(name string) (bool, error) {
	exists := false
	err := x.db.View(func(tx *bolt.Tx) error {
		exists = tx.Bucket(bkBuckets).Get([]byte(name)) != nil
		return nil
	})
	return exists, err
}

// ListBuckets returns all buckets ordered by name.
func (x *Index) ListBuckets() ([]BucketInfo, error) {
	var out []BucketInfo
	err := x.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bkBuckets).ForEach(func(_, v []byte) error {
			var info BucketInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return err
			}
			out = append(out, info)
			return nil
		})
	})
	return out, err
}

// ---------------- objects ----------------

// PutObject inserts or replaces an object and maintains the volume reverse
// index. The supplied old meta (may be nil) is used to remove stale reverse
// entries when an object is overwritten.
func (x *Index) PutObject(meta ObjectMeta, old *ObjectMeta) error {
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return x.db.Update(func(tx *bolt.Tx) error {
		objs := tx.Bucket(bkObjects)
		vo := tx.Bucket(bkVolObjs)
		if old != nil {
			for _, vid := range old.AllVolumeIDs() {
				if err := vo.Delete(volObjKey(vid, old.ObjectID)); err != nil {
					return err
				}
			}
		}
		if err := objs.Put(objectKey(meta.Bucket, meta.Key), raw); err != nil {
			return err
		}
		for _, vid := range meta.AllVolumeIDs() {
			if err := vo.Put(volObjKey(vid, meta.ObjectID), []byte(meta.Bucket+"\x00"+meta.Key)); err != nil {
				return err
			}
		}
		return nil
	})
}

// AllVolumeIDs returns the union of volume ids used by all parts (deduped in
// order of first appearance).
func (m ObjectMeta) AllVolumeIDs() []string {
	seen := map[string]bool{}
	var out []string
	add := func(ids []string) {
		for _, id := range ids {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	if len(m.Parts) > 0 {
		for _, p := range m.Parts {
			add(p.VolumeIDs)
		}
	} else {
		add(m.VolumeIDs)
	}
	return out
}

// GetObject fetches one object's metadata.
func (x *Index) GetObject(bucket, key string) (*ObjectMeta, error) {
	var meta ObjectMeta
	err := x.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bkObjects).Get(objectKey(bucket, key))
		if v == nil {
			return fmt.Errorf("%w: %s/%s", ErrNotFound, bucket, key)
		}
		return json.Unmarshal(v, &meta)
	})
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// DeleteObject removes an object and its volume reverse index entries.
func (x *Index) DeleteObject(bucket, key string) (*ObjectMeta, error) {
	var removed *ObjectMeta
	err := x.db.Update(func(tx *bolt.Tx) error {
		objs := tx.Bucket(bkObjects)
		raw := objs.Get(objectKey(bucket, key))
		if raw == nil {
			return fmt.Errorf("%w: %s/%s", ErrNotFound, bucket, key)
		}
		var meta ObjectMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return err
		}
		vo := tx.Bucket(bkVolObjs)
		for _, vid := range meta.AllVolumeIDs() {
			if err := vo.Delete(volObjKey(vid, meta.ObjectID)); err != nil {
				return err
			}
		}
		if err := objs.Delete(objectKey(bucket, key)); err != nil {
			return err
		}
		removed = &meta
		return nil
	})
	return removed, err
}

// ListObjects paginates objects in a bucket.
//
// prefix filters keys; startAfter resumes a previous page (pass the returned
// NextStartKey); maxKeys bounds the page; delimiter, when non-empty, folds
// keys sharing the next delimiter segment into CommonPrefixes (S3 folders).
func (x *Index) ListObjects(bucket, prefix, startAfter, delimiter string, maxKeys int) (ListResult, error) {
	res := ListResult{}
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	seek := append(append([]byte(bucket), 0), []byte(prefix)...)
	startCursor := append(append([]byte(bucket), 0), []byte(startAfter)...)
	err := x.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bkObjects).Cursor()
		k, v := c.Seek(seek)
		// Advance past the resume cursor if it is later than the prefix seek.
		if startAfter != "" {
			k2, v2 := c.Seek(startCursor)
			if k2 != nil && string(k2) > string(k) {
				k, v = k2, v2
				// Seek positions on the exact cursor key; move one forward so
				// startAfter itself is excluded (S3 semantics).
				if k != nil && string(k) == string(startCursor) {
					k, v = c.Next()
				}
			}
		}
		prefixBytes := append(append([]byte(bucket), 0), []byte(prefix)...)
		commonSeen := map[string]bool{}
		count := 0
		for ; k != nil && bytesHasPrefix(k, prefixBytes); k, v = c.Next() {
			key := string(k[len(bucket)+1:])
			if delimiter != "" {
				if idx := indexAfter(key, prefix, delimiter); idx >= 0 {
					cp := key[:idx]
					if !commonSeen[cp] {
						commonSeen[cp] = true
						res.CommonPrefixes = append(res.CommonPrefixes, cp)
						count++
					}
					if count >= maxKeys {
						res.Truncated = true
						break
					}
					continue
				}
			}
			var meta ObjectMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return err
			}
			res.Objects = append(res.Objects, meta)
			res.NextStartKey = key
			count++
			if count >= maxKeys {
				// Peek whether more entries exist to set Truncated.
				nk, _ := c.Next()
				if nk != nil && bytesHasPrefix(nk, prefixBytes) {
					res.Truncated = true
				}
				break
			}
		}
		return nil
	})
	return res, err
}

// indexAfter returns the end index of the first delimiter-delimited segment
// after prefix, or -1 when key belongs directly under prefix (no further
// delimiter in the remainder). With an empty prefix, a key without any
// delimiter is a top-level object, not a common prefix.
func indexAfter(key, prefix, delimiter string) int {
	if len(key) < len(prefix) || key[:len(prefix)] != prefix {
		return -1
	}
	rest := key[len(prefix):]
	if rest == "" {
		return -1
	}
	if idx := strings.Index(rest, delimiter); idx >= 0 {
		return len(prefix) + idx + len(delimiter)
	}
	return -1
}

func bytesHasPrefix(b, prefix []byte) bool {
	return len(b) >= len(prefix) && string(b[:len(prefix)]) == string(prefix)
}

// ScanObjects runs fn over every object (repair/scrub support). Returning an
// error stops the scan.
func (x *Index) ScanObjects(fn func(meta ObjectMeta) error) error {
	return x.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bkObjects).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var meta ObjectMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return err
			}
			if err := fn(meta); err != nil {
				return err
			}
		}
		return nil
	})
}

// ObjectsOnVolume enumerates (bucket, key) pairs whose object placed a shard
// on the given volume.
func (x *Index) ObjectsOnVolume(volumeID string, fn func(bucket, key string) error) error {
	prefix := append([]byte(volumeID), 0)
	return x.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bkVolObjs).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytesHasPrefix(k, prefix); k, v = c.Next() {
			parts := splitN(string(v), "\x00", 2)
			if len(parts) != 2 {
				continue
			}
			if err := fn(parts[0], parts[1]); err != nil {
				return err
			}
		}
		return nil
	})
}

func splitN(s, sep string, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n-1; i++ {
		idx := indexOf(s, sep)
		if idx < 0 {
			break
		}
		out = append(out, s[:idx])
		s = s[idx+len(sep):]
	}
	out = append(out, s)
	return out
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// CountObjects returns the total number of stored objects.
func (x *Index) CountObjects() (int64, error) {
	var n int64
	err := x.db.View(func(tx *bolt.Tx) error {
		n = int64(tx.Bucket(bkObjects).Stats().KeyN)
		return nil
	})
	return n, err
}
