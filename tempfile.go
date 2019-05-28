package extsort

import (
	"bufio"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
)

type tempWriter struct {
	f *os.File
	c compressedWriter
	w *bufio.Writer

	scratch []byte
	offsets []int64
}

func newTempWriter(dir string, compress Compression) (*tempWriter, error) {
	f, err := ioutil.TempFile(dir, "extsort")
	if err != nil {
		return nil, err
	}

	c := compress.newWriter()
	c.Reset(f)

	w := bufio.NewWriterSize(c, 1<<16) // 64k
	return &tempWriter{f: f, c: c, w: w, scratch: make([]byte, binary.MaxVarintLen64)}, nil
}

func (t *tempWriter) Name() string {
	return t.f.Name()
}

func (t *tempWriter) Encode(p []byte) error {
	n := binary.PutUvarint(t.scratch, uint64(len(p)))
	if _, err := t.Write(t.scratch[:n]); err != nil {
		return err
	}
	if _, err := t.Write(p); err != nil {
		return err
	}
	return nil
}

func (t *tempWriter) Write(p []byte) (int, error) {
	return t.w.Write(p)
}

func (t *tempWriter) Flush() error {
	if err := t.w.Flush(); err != nil {
		return err
	}
	if err := t.c.Close(); err != nil {
		return err
	}

	pos, err := t.f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	t.offsets = append(t.offsets, pos)
	t.c.Reset(t.f)
	t.w.Reset(t.c)

	return nil
}

func (t *tempWriter) Close() (err error) {
	if e := t.c.Close(); e != nil {
		err = e
	}
	if e := t.f.Close(); e != nil {
		err = e
	}
	if e := os.Remove(t.f.Name()); e != nil {
		err = e
	}
	return
}

// --------------------------------------------------------------------

type tempReader struct {
	f *os.File

	readers  []io.ReadCloser
	sections []*bufio.Reader
}

func newTempReader(name string, offsets []int64, bufSize int, compress Compression) (*tempReader, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	r := &tempReader{
		f: f,

		readers:  make([]io.ReadCloser, 0, len(offsets)),
		sections: make([]*bufio.Reader, 0, len(offsets)),
	}
	slimit := bufSize / (len(offsets) + 1)
	offset := int64(0)
	for _, next := range offsets {
		crd, err := compress.newReader(io.NewSectionReader(r.f, offset, next-offset))
		if err != nil {
			_ = r.Close()
			return nil, err
		}
		r.sections = append(r.sections, bufio.NewReaderSize(crd, slimit))
		r.readers = append(r.readers, crd)
		offset = next
	}

	return r, nil
}

func (t *tempReader) NumSections() int {
	return len(t.sections)
}

func (t *tempReader) ReadNext(section int) ([]byte, error) {
	r := t.sections[section]
	if r == nil {
		return nil, nil
	}

	n, err := binary.ReadUvarint(r)
	if err == io.EOF {
		t.sections[section] = nil
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	data := make([]byte, int(n))
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (t *tempReader) Close() (err error) {
	for _, crd := range t.readers {
		if e := crd.Close(); e != nil {
			err = e
		}
	}
	if e := t.f.Close(); e != nil {
		err = e
	}
	return
}
