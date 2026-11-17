package main

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// manifestEntry records one uploaded object for later hash verification.
type manifestEntry struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	File   string `json:"file"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	ETag   string `json:"etag,omitempty"`
}

// ---------------- flag helper ----------------

func parseFlags(args []string) *flag.FlagSet {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func must(fs *flag.FlagSet, args []string) {
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

// ---------------- gen ----------------

func cmdGen(args []string) error {
	fs := parseFlags(args)
	out := fs.String("out", "", "output directory")
	count := fs.Int("count", 20, "number of files")
	minK := fs.Int("min", 16, "minimum size in KiB")
	maxK := fs.Int("max", 4096, "maximum size in KiB")
	seed := fs.Int64("seed", 42, "random seed")
	must(fs, args)
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	rng := seededRand(*seed)
	manifest := make([]manifestEntry, 0, *count)
	for i := 0; i < *count; i++ {
		sizeKib := *minK
		if *maxK > *minK {
			sizeKib = *minK + rng.Intn(*maxK-*minK+1)
		}
		// DICOM-ish naming: modality + study + instance.
		modality := []string{"CT", "MR", "XR", "US"}[rng.Intn(4)]
		name := fmt.Sprintf("%s_STUDY%04d_S%04d_IM%05d.dcm", modality, rng.Intn(1000), i, rng.Intn(100000))
		path := filepath.Join(*out, name)
		if err := writePseudoImage(path, int64(sizeKib)*1024, rng); err != nil {
			return err
		}
		sum, err := fileSHA256(path)
		if err != nil {
			return err
		}
		info, _ := os.Stat(path)
		manifest = append(manifest, manifestEntry{File: name, Size: info.Size(), SHA256: sum})
	}
	mb, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(filepath.Join(*out, "manifest.json"), mb, 0o644); err != nil {
		return err
	}
	fmt.Printf("generated %d pseudo-image files in %s (manifest.json written)\n", *count, *out)
	return nil
}

// writePseudoImage writes deterministic-looking pseudo-random bytes with a
// 128-byte DICOM-style preamble, so files differ in both size and content.
func writePseudoImage(path string, size int64, rng interface{ Read([]byte) (int, error) }) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	preamble := make([]byte, 128)
	for i := range preamble {
		preamble[i] = byte(i)
	}
	if _, err := f.Write(preamble); err != nil {
		return err
	}
	remaining := size - 128
	if remaining < 0 {
		remaining = size
	}
	buf := make([]byte, 64*1024)
	var written int64
	for written < remaining {
		n := int64(len(buf))
		if remaining-written < n {
			n = remaining - written
		}
		if _, err := rng.Read(buf[:n]); err != nil {
			return err
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return err
		}
		written += n
	}
	return nil
}

// ---------------- buckets ----------------

func cmdMakeBucket(args []string) error {
	fs := parseFlags(args)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8333", "")
	bucket := fs.String("bucket", "", "")
	must(fs, args)
	if *bucket == "" {
		return fmt.Errorf("--bucket is required")
	}
	s := newSignerFromFlags()
	resp, err := s.do(http.MethodPut, strings.TrimRight(*endpoint, "/")+"/"+*bucket, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("make bucket %s: %s\n%s", *bucket, resp.Status, b)
	}
	fmt.Printf("bucket %q created\n", *bucket)
	return nil
}

func cmdRemoveBucket(args []string) error {
	fs := parseFlags(args)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8333", "")
	bucket := fs.String("bucket", "", "")
	must(fs, args)
	s := newSignerFromFlags()
	resp, err := s.do(http.MethodDelete, strings.TrimRight(*endpoint, "/")+"/"+*bucket, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remove bucket: %s\n%s", resp.Status, b)
	}
	fmt.Printf("bucket %q removed\n", *bucket)
	return nil
}

// ---------------- upload / download / delete ----------------

func cmdUpload(args []string) error {
	fs := parseFlags(args)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8333", "")
	bucket := fs.String("bucket", "", "")
	dir := fs.String("dir", "", "directory of files to upload")
	manifestPath := fs.String("manifest", "", "manifest.json from gen (defaults to DIR/manifest.json)")
	prefix := fs.String("prefix", "", "key prefix")
	must(fs, args)
	if *bucket == "" || *dir == "" {
		return fmt.Errorf("--bucket and --dir are required")
	}
	if *manifestPath == "" {
		*manifestPath = filepath.Join(*dir, "manifest.json")
	}
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var entries []manifestEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return err
	}
	s := newSignerFromFlags()
	base := strings.TrimRight(*endpoint, "/")
	out := make([]manifestEntry, 0, len(entries))
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(*dir, e.File))
		if err != nil {
			return err
		}
		key := *prefix + e.File
		resp, err := s.do(http.MethodPut, base+"/"+*bucket+"/"+url.PathEscape(key), data)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("upload %s: %s\n%s", key, resp.Status, body)
		}
		etag := strings.Trim(resp.Header.Get("ETag"), "\"")
		e.Bucket, e.Key, e.ETag = *bucket, key, etag
		out = append(out, e)
		fmt.Printf("PUT %s (%d bytes) etag=%s\n", key, e.Size, etag)
	}
	upManifest, _ := json.MarshalIndent(out, "", "  ")
	upPath := filepath.Join(*dir, "upload-manifest.json")
	if err := os.WriteFile(upPath, upManifest, 0o644); err != nil {
		return err
	}
	fmt.Printf("uploaded %d objects; upload manifest -> %s\n", len(out), upPath)
	return nil
}

func cmdDownload(args []string) error {
	fs := parseFlags(args)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8333", "")
	bucket := fs.String("bucket", "", "")
	key := fs.String("key", "", "")
	out := fs.String("out", "", "output file")
	must(fs, args)
	s := newSignerFromFlags()
	u := strings.TrimRight(*endpoint, "/") + "/" + *bucket + "/" + url.PathEscape(*key)
	resp, err := s.do(http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("download: %s\n%s", resp.Status, b)
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return err
	}
	fmt.Printf("downloaded %s -> %s\n", *key, *out)
	return nil
}

func cmdDelete(args []string) error {
	fs := parseFlags(args)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8333", "")
	bucket := fs.String("bucket", "", "")
	key := fs.String("key", "", "")
	must(fs, args)
	s := newSignerFromFlags()
	resp, err := s.do(http.MethodDelete,
		strings.TrimRight(*endpoint, "/")+"/"+*bucket+"/"+url.PathEscape(*key), nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	fmt.Printf("deleted %s/%s (%s)\n", *bucket, *key, resp.Status)
	return nil
}

// ---------------- list ----------------

type s3ListResult struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	Name        string   `xml:"Name"`
	KeyCount    int      `xml:"KeyCount"`
	IsTruncated bool     `xml:"IsTruncated"`
	NextToken   string   `xml:"NextToken"`
	Contents    []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
		ETag string `xml:"ETag"`
	} `xml:"Contents"`
	CommonPrefixes []struct {
		Prefix string `xml:"Prefix"`
	} `xml:"CommonPrefixes"`
}

func cmdList(args []string) error {
	fs := parseFlags(args)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8333", "")
	bucket := fs.String("bucket", "", "")
	prefix := fs.String("prefix", "", "")
	maxKeys := fs.Int("max", 1000, "")
	must(fs, args)
	s := newSignerFromFlags()
	base := strings.TrimRight(*endpoint, "/") + "/" + *bucket
	q := url.Values{}
	q.Set("list-type", "2")
	if *prefix != "" {
		q.Set("prefix", *prefix)
	}
	q.Set("max-keys", fmt.Sprint(*maxKeys))
	resp, err := s.do(http.MethodGet, base+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("list: %s\n%s", resp.Status, body)
	}
	var res s3ListResult
	if err := xml.Unmarshal(body, &res); err != nil {
		return err
	}
	for _, cp := range res.CommonPrefixes {
		fmt.Printf("PRE %s\n", cp.Prefix)
	}
	for _, o := range res.Contents {
		fmt.Printf("%10d  %s\n", o.Size, o.Key)
	}
	fmt.Printf("key count=%d truncated=%v\n", res.KeyCount, res.IsTruncated)
	return nil
}

// ---------------- verify ----------------

func cmdVerify(args []string) error {
	fs := parseFlags(args)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8333", "")
	bucket := fs.String("bucket", "", "")
	manifestPath := fs.String("manifest", "", "upload-manifest.json")
	must(fs, args)
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		return err
	}
	var entries []manifestEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return err
	}
	s := newSignerFromFlags()
	base := strings.TrimRight(*endpoint, "/")
	tmp, err := os.MkdirTemp("", "imctl-verify-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	var failed int
	for _, e := range entries {
		u := base + "/" + *bucket + "/" + url.PathEscape(e.Key)
		resp, err := s.do(http.MethodGet, u, nil)
		if err != nil {
			fmt.Printf("FAIL %s: %v\n", e.Key, err)
			failed++
			continue
		}
		if resp.StatusCode >= 300 {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			fmt.Printf("FAIL %s: %s %s\n", e.Key, resp.Status, b)
			failed++
			continue
		}
		h := newSHA256Writer()
		if _, err := io.Copy(h, resp.Body); err != nil {
			fmt.Printf("FAIL %s: read: %v\n", e.Key, err)
			failed++
			resp.Body.Close()
			continue
		}
		resp.Body.Close()
		got := h.hex()
		if got != e.SHA256 {
			fmt.Printf("FAIL %s: hash mismatch recorded=%s got=%s\n", e.Key, e.SHA256, got)
			failed++
			continue
		}
		fmt.Printf("OK   %s sha256=%s\n", e.Key, got)
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d objects failed verification", failed, len(entries))
	}
	fmt.Printf("ALL %d OBJECTS VERIFIED\n", len(entries))
	return nil
}

// ---------------- presign ----------------

func cmdPresign(args []string) error {
	fs := parseFlags(args)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8333", "")
	bucket := fs.String("bucket", "", "")
	key := fs.String("key", "", "")
	expires := fs.Duration("expires", 15*time.Minute, "")
	must(fs, args)
	s := newSignerFromFlags()
	raw := strings.TrimRight(*endpoint, "/") + "/" + *bucket + "/" + url.PathEscape(*key)
	signed, err := s.presign(raw, *expires)
	if err != nil {
		return err
	}
	fmt.Println(signed)
	return nil
}

// ---------------- multipart ----------------

func cmdMultipart(args []string) error {
	fs := parseFlags(args)
	endpoint := fs.String("endpoint", "http://127.0.0.1:8333", "")
	bucket := fs.String("bucket", "", "")
	key := fs.String("key", "", "")
	file := fs.String("file", "", "")
	partKib := fs.Int("part-size", 512, "part size in KiB")
	must(fs, args)
	data, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	s := newSignerFromFlags()
	base := strings.TrimRight(*endpoint, "/")

	// Initiate.
	initURL := base + "/" + *bucket + "/" + url.PathEscape(*key) + "?uploads"
	resp, err := s.do(http.MethodPost, initURL, nil)
	if err != nil {
		return err
	}
	var init struct {
		UploadID string `xml:"UploadId"`
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("initiate: %s\n%s", resp.Status, body)
	}
	if err := xml.Unmarshal(body, &init); err != nil {
		return err
	}
	partSize := int64(*partKib) * 1024
	var nums []int
	for off, n := int64(0), 1; off < int64(len(data)); off, n = off+partSize, n+1 {
		end := off + partSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		u := fmt.Sprintf("%s/%s/%s?partNumber=%d&uploadId=%s",
			base, *bucket, url.PathEscape(*key), n, url.QueryEscape(init.UploadID))
		resp, err := s.do(http.MethodPut, u, data[off:end])
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("part %d failed: %s", n, resp.Status)
		}
		nums = append(nums, int(n))
	}
	// Complete.
	sort.Ints(nums)
	var xmlParts strings.Builder
	xmlParts.WriteString("<CompleteMultipartUpload>")
	for _, n := range nums {
		fmt.Fprintf(&xmlParts, "<Part><PartNumber>%d</PartNumber></Part>", n)
	}
	xmlParts.WriteString("</CompleteMultipartUpload>")
	completeURL := fmt.Sprintf("%s/%s/%s?uploadId=%s",
		base, *bucket, url.PathEscape(*key), url.QueryEscape(init.UploadID))
	resp, err = s.do(http.MethodPost, completeURL, []byte(xmlParts.String()))
	if err != nil {
		return err
	}
	cbody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("complete: %s\n%s", resp.Status, cbody)
	}
	fmt.Printf("multipart upload %s/%s completed in %d parts\n", *bucket, *key, len(nums))
	return nil
}

func cmdHashFile(args []string) error {
	fs := parseFlags(args)
	file := fs.String("file", "", "")
	must(fs, args)
	sum, err := fileSHA256(*file)
	if err != nil {
		return err
	}
	fmt.Println(sum)
	return nil
}
