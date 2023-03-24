package extsort

import (
	"bytes"
	"sort"
)

// Compares byte chunks. -1 for a < b and 0 for a == b.
type Compare func(a, b []byte) int

// Equal compares two byte chunks for equality.
type Equal func(a, b []byte) bool

func stdCompare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Options contains sorting options
type Options struct {
	// WorkDir specifies the working directory.
	// By default os.TempDir() is used.
	WorkDir string

	// Compare defines the compare function.
	// Default: bytes.Compare
	Compare Compare

	// Sort defines the sort function that is used.
	// Default: sort.Sort
	Sort func(sort.Interface)

	// Dedupe defines the compare function for de-duplication.
	// Default: nil (= do not de-dupe)
	// Keeps the last added item.
	Dedupe Equal

	// BufferSize limits the memory buffer used for sorting.
	// Default: 64MiB (must be at least 64KiB)
	BufferSize int

	// Compression optionally uses compression for temporary output.
	Compression Compression
}

func (o *Options) norm() *Options {
	var opt Options
	if o != nil {
		opt = *o
	}

	if opt.Compare == nil {
		opt.Compare = stdCompare
	}

	if opt.Sort == nil {
		opt.Sort = sort.Sort
	}

	if std := (1 << 26); opt.BufferSize < 1 {
		opt.BufferSize = std
	} else if min := (1 << 16); opt.BufferSize < min {
		opt.BufferSize = min
	}

	opt.Compression = opt.Compression.norm()

	return &opt
}
