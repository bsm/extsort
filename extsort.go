package extsort

import "github.com/valyala/bytebufferpool"

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
	if sz := s.buf.ByteSize(); sz > 0 && sz+len(data) > s.opt.BufferSize {
		if err := s.flush(); err != nil {
			return err
		}
	}

	s.buf.Append(data)
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

func (s *Sorter) flush() error {
	if s.tw == nil {
		tw, err := newTempWriter(s.opt.WorkDir, s.opt.Compression)
		if err != nil {
			return err
		}
		s.tw = tw
	}

	s.opt.Sort(s.buf)

	var last []byte // store last for de-duplication
	for _, data := range s.buf.chunks {
		if s.opt.Dedupe != nil {
			if last != nil && s.opt.Dedupe(data, last) {
				continue
			}
			last = append(last[:0], data...)
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
	tr   *tempReader
	heap *minHeap

	data   *bytebufferpool.ByteBuffer
	last   []byte
	dedupe Equal
	err    error
}

func newIterator(name string, offsets []int64, opt *Options) (*Iterator, error) {
	tr, err := newTempReader(name, offsets, opt.BufferSize, opt.Compression)
	if err != nil {
		return nil, err
	}

	iter := &Iterator{tr: tr, heap: &minHeap{less: opt.Less}, dedupe: opt.Dedupe}
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
		if i.dedupe != nil {
			if i.last != nil && i.dedupe(i.data.B, i.last) {
				continue
			}
			i.last = append(i.last[:0], i.data.B...)
		}
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
		bufferPool.Put(data)
		i.err = err
		return false
	}

	prev := i.data
	i.data = data
	if prev != nil {
		bufferPool.Put(prev)
	}
	return true
}

// Data returns the data at the current cursor position.
func (i *Iterator) Data() []byte {
	return i.data.B
}

// Err returns the error, if occurred.
func (i *Iterator) Err() error {
	return i.err
}

// Close closes the iterator.
func (i *Iterator) Close() error {
	if i.data != nil {
		bufferPool.Put(i.data)
		i.data = nil
	}

	return i.tr.Close()
}

func (i *Iterator) fillHeap(section int) error {
	data, err := i.tr.ReadNext(section)
	if err != nil {
		return err
	}
	if data != nil {
		i.heap.PushData(section, data)
	}
	return nil
}
