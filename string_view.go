package etp

import "unsafe"

// bytesToStringView is valid while the source frame lease is retained.
func bytesToStringView(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(value), len(value))
}
