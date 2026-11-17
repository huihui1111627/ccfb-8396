//go:build !darwin && !linux

package volume

import "errors"

func diskSpace(path string) (total, free int64, err error) {
	return 0, 0, errors.New("volume: disk space reporting unsupported on this platform")
}
