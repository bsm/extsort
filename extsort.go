package extsort

import (
	"bytes"
)

// Sorter is responsible for sorting.
type Sorter struct {
	opt *Options
	buf *memBuffer
	tw  *tempWriter
}

// New inits a sorter
func New(opt *Options) *Sorter {
	opt = opt.norm()
	return &Sorter{opt: opt, buf: &memBuffer{less: opt.Less}}
}

// Append appends a data chunk to the sorter.
func (s *Sorter) Append(data []byte) error {
	return s.AppendKV(data, nil)
}

func (s *Sorter) AppendKV(k, v []byte) error {
	if sz := s.buf.ByteSize(); sz > 0 && sz+len(k)+len(v) > s.opt.BufferSize {
		if err := s.flush(); err != nil {
			return err
		}
	}

	s.buf.Append(k, v)
	return nil
}

// Sort applies the sort algorithm and returns an interator.
func (s *Sorter) Sort() (*Iterator, error) {
	if err := s.flush(); err != nil {
		return nil, err
	}

	// free the write buffer
	s.buf.Free()

	// wrap in an iterator
	return newIterator(s.tw.Name(), s.tw.offsets, s.opt)
}

// Close stops the processing and removes temporary files.
func (s *Sorter) Close() error {
	if s.tw != nil {
		return s.tw.Close()
	}
	return nil
}

func (s *Sorter) Size() int64 {
	sum := int64(s.buf.ByteSize())
	if s.tw == nil {
		return sum
	}
	return sum + s.tw.Size()
}

func (s *Sorter) flush() error {
	if s.tw == nil {
		tw, err := newTempWriter(s.opt.WorkDir, s.opt.Compression)
		if err != nil {
			return err
		}
		s.tw = tw
	}

	s.buf.Sort()

	var last []byte // for dups
	for _, data := range s.buf.chunks {
		if s.opt.RemoveDuplicates {
			if bytes.Equal(data.k, last) {
				continue
			}
			last = append(last[:0], data.k...)
		}

		if err := s.tw.Encode(data); err != nil {
			return err
		}
	}
	if err := s.tw.Flush(); err != nil {
		return err
	}

	s.buf.Reset()
	return nil
}

// --------------------------------------------------------------------

// Iterator instances are used to iterate over sorted output.
type Iterator struct {
	tr         *tempReader
	heap       *minHeap
	removeDups bool

	data kv
	last []byte
	err  error
}

func newIterator(name string, offsets []int64, opt *Options) (*Iterator, error) {
	tr, err := newTempReader(name, offsets, opt.BufferSize, opt.Compression)
	if err != nil {
		return nil, err
	}

	iter := &Iterator{tr: tr, heap: &minHeap{less: opt.Less}, removeDups: opt.RemoveDuplicates}
	for i := 0; i < tr.NumSections(); i++ {
		if err := iter.fillHeap(i); err != nil {
			_ = tr.Close()
			return nil, err
		}
	}
	return iter, nil
}

// Next advances the iterator to the next item and returns true if successful.
func (i *Iterator) Next() bool {
	for i.next() {
		if !i.removeDups {
			return true
		}

		if bytes.Equal(i.data.k, i.last) {
			continue
		}
		i.last = append(i.last[:0], i.data.k...)
		return true
	}
	return false
}

func (i *Iterator) next() bool {
	if i.err != nil {
		return false
	}
	if i.heap.Len() == 0 {
		return false
	}

	section, data := i.heap.PopData()
	if err := i.fillHeap(section); err != nil {
		i.err = err
		return false
	}

	i.data = data
	return true
}

// Data returns the data at the current cursor position.
func (i *Iterator) Data() []byte {
	return i.data.k
}

func (i *Iterator) DataKV() (k, v []byte) {
	return i.data.k, i.data.v
}

// Err returns the error, if occurred.
func (i *Iterator) Err() error {
	return i.err
}

// Close closes the iterator.
func (i *Iterator) Close() error {
	return i.tr.Close()
}

func (i *Iterator) fillHeap(section int) error {
	data, err := i.tr.ReadNext(section)
	if err != nil {
		return err
	}
	if data.k != nil {
		i.heap.PushData(section, data)
	}
	return nil
}

type kv struct {
	k, v []byte
}
