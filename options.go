package extsort

import (
	"bytes"
)

// Less compares byte chunks.
type Less func(a, b []byte) bool

func stdLess(a, b []byte) bool {
	return bytes.Compare(a, b) < 0
}

// Options contains sorting options
type Options struct {
	// WorkDir specifies the working directory.
	// By default os.TempDir() is used.
	WorkDir string

	// Less defines the compare function.
	// Default: bytes.Compare() < 0
	Less Less

	// BufferSize limits the memory buffer used for sorting.
	// Default: 64MiB (must be at least 64KiB)
	BufferSize int

	// Compression optionally uses compression for temporary output.
	Compression Compression

	RemoveDuplicates bool
}

func (o *Options) norm() *Options {
	var opt Options
	if o != nil {
		opt = *o
	}

	if opt.Less == nil {
		opt.Less = stdLess
	}

	if std := (1 << 26); opt.BufferSize < 1 {
		opt.BufferSize = std
	} else if min := (1 << 16); opt.BufferSize < min {
		opt.BufferSize = min
	}

	opt.Compression = opt.Compression.norm()

	return &opt
}
