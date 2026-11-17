package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"imstore/internal/index"
)

// PutObject streams a single-PUT object from r and commits its metadata.
// Overwrites replace the old record and best-effort delete old shards.
func (s *Store) PutObject(ctx context.Context, bucket, key, contentType string,
	userMeta map[string]string, r io.Reader,
) (*index.ObjectMeta, error) {
	objectID := NewObjectID()
	res, err := s.PutPart(ctx, objectID, r)
	if err != nil {
		return nil, err
	}
	old, _ := s.idx.GetObject(bucket, key) // nil when this is a new key
	meta := index.ObjectMeta{
		Bucket:      bucket,
		Key:         key,
		ObjectID:    objectID,
		Size:        res.Size,
		ETag:        res.ETag,
		SHA256:      res.SHA256,
		ContentType: contentType,
		UserMeta:    cloneMeta(userMeta),
		DataShards:  s.dataN,
		ParityShds:  s.parityN,
		ShardSize:   s.shardSz,
		StripeCount: res.StripeCount,
		VolumeIDs:   res.VolumeIDs,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.idx.PutObject(meta, old); err != nil {
		return nil, err
	}
	if old != nil {
		s.garbageCollect(*old)
	}
	return &meta, nil
}

func cloneMeta(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// InitMultipart opens a multipart upload and returns its upload id.
func (s *Store) InitMultipart(bucket, key, contentType string, userMeta map[string]string) (string, error) {
	if exists, err := s.idx.BucketExists(bucket); err != nil {
		return "", err
	} else if !exists {
		return "", fmt.Errorf("%w: bucket %s", ErrObjectNotFound, bucket)
	}
	uploadID := NewObjectID()
	err := s.idx.CreateMPUpload(index.MPUpload{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		UserMeta:    cloneMeta(userMeta),
		CreatedAt:   time.Now().UTC(),
	})
	return uploadID, err
}

// UploadPart streams one part of a multipart upload.
func (s *Store) UploadPart(ctx context.Context, uploadID string, partNumber int, r io.Reader) (*index.PartMeta, error) {
	if _, err := s.idx.GetMPUpload(uploadID); err != nil {
		return nil, err
	}
	if partNumber < 1 || partNumber > 10000 {
		return nil, errors.New("store: part number must be between 1 and 10000")
	}
	// Re-uploading the same part number replaces the previous segment.
	if old, err := s.idx.GetMPart(uploadID, partNumber); err == nil {
		s.deletePartShards(old)
	}
	objectID := uploadID + "-p" + fmt.Sprintf("%05d", partNumber)
	res, err := s.PutPart(ctx, objectID, r)
	if err != nil {
		return nil, err
	}
	pm := index.PartMeta{
		PartNumber:  partNumber,
		ObjectID:    objectID,
		Size:        res.Size,
		ETag:        res.ETag,
		SHA256:      res.SHA256,
		StripeCount: res.StripeCount,
		VolumeIDs:   res.VolumeIDs,
	}
	if err := s.idx.PutMPart(uploadID, pm); err != nil {
		return nil, err
	}
	return &pm, nil
}

// CompleteMultipart assembles uploaded parts into a final object.
// partNumbers may arrive in any order; they are sorted and validated to be
// contiguous starting at 1.
func (s *Store) CompleteMultipart(uploadID string, partNumbers []int) (*index.ObjectMeta, error) {
	up, err := s.idx.GetMPUpload(uploadID)
	if err != nil {
		return nil, err
	}
	nums := append([]int(nil), partNumbers...)
	sort.Ints(nums)
	if len(nums) == 0 || nums[0] != 1 {
		return nil, errors.New("store: multipart completion requires part 1 and at least one part")
	}
	for i := 1; i < len(nums); i++ {
		if nums[i] != nums[i-1]+1 {
			return nil, fmt.Errorf("store: parts must be contiguous; gap between %d and %d", nums[i-1], nums[i])
		}
	}
	parts := make([]index.PartMeta, 0, len(nums))
	var totalSize int64
	etagConcat := md5.New()
	for _, n := range nums {
		p, err := s.idx.GetMPart(uploadID, n)
		if err != nil {
			return nil, fmt.Errorf("store: part %d not uploaded: %w", n, err)
		}
		raw, decErr := hex.DecodeString(p.ETag)
		if decErr == nil {
			etagConcat.Write(raw)
		}
		parts = append(parts, *p)
		totalSize += p.Size
	}
	// S3 multipart ETag convention: md5(concat(part md5 bytes))-N
	etag := fmt.Sprintf("%s-%d", hex.EncodeToString(etagConcat.Sum(nil)), len(parts))

	// The overall SHA-256 is the hash of the part SHA-256 digests in order,
	// which is stable, cheap and recorded as the object-level integrity value.
	wholeHash := sha256Concat(parts)
	totalStripes := 0
	for _, p := range parts {
		totalStripes += p.StripeCount
	}

	old, _ := s.idx.GetObject(up.Bucket, up.Key)
	objectID := NewObjectID()
	meta := index.ObjectMeta{
		Bucket:      up.Bucket,
		Key:         up.Key,
		ObjectID:    objectID,
		Size:        totalSize,
		ETag:        etag,
		SHA256:      wholeHash,
		ContentType: up.ContentType,
		UserMeta:    cloneMeta(up.UserMeta),
		DataShards:  s.dataN,
		ParityShds:  s.parityN,
		ShardSize:   s.shardSz,
		StripeCount: totalStripes,
		Parts:       parts,
		CreatedAt:   time.Now().UTC(),
	}
	// Part shards keep their original object ids; register the final object's
	// reverse index by pointing VolumeIDs (single-part slot) at nothing and
	// relying on Parts for placement. AllVolumeIDs() unions both.
	if err := s.idx.PutObject(meta, old); err != nil {
		return nil, err
	}
	if old != nil {
		s.garbageCollect(*old)
	}
	if _, err := s.idx.DeleteMPUpload(uploadID); err != nil {
		return nil, err
	}
	return &meta, nil
}

// AbortMultipart discards an upload and all of its part shards. The index
// deletion and the part listing happen in separate transactions because bbolt
// forbids opening a new read transaction while a write transaction is open.
func (s *Store) AbortMultipart(uploadID string) error {
	parts, err := s.idx.ListMParts(uploadID)
	if err != nil {
		return err
	}
	up, err := s.idx.DeleteMPUpload(uploadID)
	if err != nil {
		return err
	}
	for i := range parts {
		s.deletePartShards(&parts[i])
	}
	_ = up
	return nil
}

// DeleteObject removes an object's metadata and its shard files.
func (s *Store) DeleteObject(bucket, key string) error {
	meta, err := s.idx.DeleteObject(bucket, key)
	if err != nil {
		if errors.Is(err, index.ErrNotFound) {
			return fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, key)
		}
		return err
	}
	s.garbageCollect(*meta)
	return nil
}

// garbageCollect best-effort removes every shard file of a dead object/part
// set. Failures are recorded on the volume but never block the caller; the
// scrub sweep reclaims anything left behind.
func (s *Store) garbageCollect(meta index.ObjectMeta) {
	for _, p := range meta.AllParts() {
		s.deletePartShards(&p)
	}
}

// deletePartShards removes the shard files of one part given its placement.
func (s *Store) deletePartShards(p *index.PartMeta) {
	for shardIdx, vid := range p.VolumeIDs {
		v, err := s.volumes.Get(vid)
		if err != nil {
			continue
		}
		if err := v.RemoveShard(p.ObjectID, shardIdx); err != nil {
			v.RecordError(err)
		}
	}
}

// sha256Concat hashes the concatenation of part SHA-256 hex digests.
func sha256Concat(parts []index.PartMeta) string {
	h := sha256.New()
	for _, p := range parts {
		io.WriteString(h, p.SHA256)
	}
	return hex.EncodeToString(h.Sum(nil))
}
