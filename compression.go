package extsort

import (
	"compress/gzip"
	"io"
	"io/ioutil"
)

// Compression codec.
type Compression uint8

// Supported compression types.
const (
	CompressionNone Compression = iota
	CompressionGzip
)

func (c Compression) norm() Compression {
	if c < CompressionNone || c > CompressionGzip {
		return CompressionNone
	}
	return c
}

func (c Compression) newReader(r io.Reader) (io.ReadCloser, error) {
	switch c {
	case CompressionGzip:
		return gzip.NewReader(r)
	}
	return plainReader{Reader: r}, nil
}

func (c Compression) newWriter() compressedWriter {
	switch c {
	case CompressionGzip:
		wr, _ := gzip.NewWriterLevel(ioutil.Discard, gzip.BestSpeed)
		return wr
	}
	return new(plainWriter)
}

type compressedWriter interface {
	io.Writer
	Reset(w io.Writer)
	Close() error
}

type plainReader struct{ io.Reader }

func (plainReader) Close() error { return nil }

type plainWriter struct{ io.Writer }

func (w *plainWriter) Reset(wr io.Writer) { w.Writer = wr }
func (*plainWriter) Close() error         { return nil }
