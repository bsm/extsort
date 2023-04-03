package extsort

import (
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/snappy"
)

// Compression codec.
type Compression uint8

// Supported compression types.
const (
	CompressionNone Compression = iota
	CompressionGzip
	CompressionSnappy
)

func (c Compression) norm() Compression {
	if c < CompressionNone || c > CompressionSnappy {
		return CompressionNone
	}
	return c
}

func (c Compression) newReader(r io.Reader) (io.ReadCloser, error) {
	switch c {
	case CompressionGzip:
		return gzip.NewReader(r)
	case CompressionSnappy:
		r := snappy.NewReader(r)
		return readerNoopCloser{Reader: r}, nil
	}
	return readerNoopCloser{Reader: r}, nil
}

func (c Compression) newWriter(w io.Writer) compressedWriter {
	switch c {
	case CompressionGzip:
		wr, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
		return wr
	case CompressionSnappy:
		wr := snappy.NewBufferedWriter(w)
		return wr
	}
	return &writerNoopCloser{Writer: w}
}

type compressedWriter interface {
	io.Writer
	Reset(w io.Writer)
	Close() error
}

type readerNoopCloser struct{ io.Reader }

func (readerNoopCloser) Close() error { return nil }

type writerNoopCloser struct{ io.Writer }

func (w *writerNoopCloser) Reset(wr io.Writer) { w.Writer = wr }
func (*writerNoopCloser) Close() error         { return nil }
