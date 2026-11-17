package main

import (
	"io"
	"os"
)

func openFile(path string) (*os.File, error) { return os.Open(path) }

// copyN 把 r 全部拷贝到 w。
func copyN(w io.Writer, r io.Reader) (int64, error) {
	return io.Copy(w, r)
}
