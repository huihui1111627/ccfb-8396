package meta_test

import (
	"path/filepath"
	"testing"

	"erasurestore/internal/meta"
)

func TestPrefixPagination(t *testing.T) {
	st, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}

	// 写入 images/ 下 25 个对象 + 其他前缀 5 个
	for i := 0; i < 25; i++ {
		key := "images/" + pad(i) + ".dcm"
		if err := st.PutObjectMeta(&meta.ObjectMeta{
			ID: "id" + pad(i), Bucket: "b", Key: key, Size: int64(i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := st.PutObjectMeta(&meta.ObjectMeta{
			ID: "other" + pad(i), Bucket: "b", Key: "logs/" + pad(i) + ".log", Size: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// 分页，每页 10 条，只扫 images/ 前缀
	var token string
	seen := 0
	pages := 0
	for {
		res, err := st.ListObjects("b", "images/", "", token, 10)
		if err != nil {
			t.Fatal(err)
		}
		seen += len(res.Entries)
		pages++
		for _, e := range res.Entries {
			if len(e.Key) < 7 || e.Key[:7] != "images/" {
				t.Fatalf("前缀过滤失效: %s", e.Key)
			}
		}
		if !res.IsTruncated {
			break
		}
		token = res.NextToken
		if pages > 5 {
			t.Fatal("分页未收敛")
		}
	}
	if seen != 25 {
		t.Fatalf("期望分页累计 25 个 images 对象，得到 %d", seen)
	}
	if pages < 3 {
		t.Fatalf("期望至少 3 页，得到 %d", pages)
	}
}

func TestDelimiterCommonPrefixes(t *testing.T) {
	st, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_ = st.CreateBucket("b")
	keys := []string{"a/x/1", "a/x/2", "a/y/3", "a/top", "b/1"}
	for i, k := range keys {
		_ = st.PutObjectMeta(&meta.ObjectMeta{ID: "id" + pad(i), Bucket: "b", Key: k, Size: 1})
	}
	res, err := st.ListObjects("b", "a/", "/", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"a/x/": false, "a/y/": false}
	for _, cp := range res.CommonPrefixes {
		if _, ok := want[cp]; ok {
			want[cp] = true
		}
	}
	for k, found := range want {
		if !found {
			t.Fatalf("缺少公共前缀 %s，得到 %v", k, res.CommonPrefixes)
		}
	}
	if len(res.Entries) != 1 || res.Entries[0].Key != "a/top" {
		t.Fatalf("分隔符下应只剩 a/top 一个对象，得到 %+v", res.Entries)
	}
}

func pad(n int) string {
	if n < 10 {
		return "0" + string(rune('0'+n))
	}
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}
