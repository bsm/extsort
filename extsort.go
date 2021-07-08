package extsort

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
	return s.Put(data, nil)
}

// Put inserts a key value pair into the sorter.
func (s *Sorter) Put(key, value []byte) error {
	if sz := s.buf.ByteSize(); sz > 0 && sz+len(key)+len(value) > s.opt.BufferSize {
		if err := s.flush(); err != nil {
			return err
		}
	}

	s.buf.Append(key, value)
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

// Size returns the buffered and written size.
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

	s.opt.Sort(s.buf)

	var lastKey []byte // store last for de-duplication
	for _, ent := range s.buf.ents {
		if s.opt.Dedupe != nil {
			key := ent.Key()
			if lastKey != nil && s.opt.Dedupe(key, lastKey) {
				continue
			}
			lastKey = key
		}

		if err := s.tw.Encode(ent); err != nil {
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

	ent     *entry
	lastKey []byte
	dedupe  Equal
	err     error
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
			key := i.ent.Key()
			if i.lastKey != nil && i.dedupe(key, i.lastKey) {
				continue
			}
			i.lastKey = append(i.lastKey[:0], key...)
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

	section, ent := i.heap.PopEntry()
	if err := i.fillHeap(section); err != nil {
		ent.Release()
		i.err = err
		return false
	}

	prev := i.ent
	i.ent = ent
	if prev != nil {
		prev.Release()
	}
	return true
}

// Key returns the key at the current cursor position.
func (i *Iterator) Key() []byte {
	return i.ent.Key()
}

// Value returns the value at the current cursor position.
func (i *Iterator) Value() []byte {
	return i.ent.Val()
}

// Data returns the data at the current cursor position (alias for Key).
func (i *Iterator) Data() []byte {
	return i.Key()
}

// Err returns the error, if occurred.
func (i *Iterator) Err() error {
	return i.err
}

// Close closes the iterator.
func (i *Iterator) Close() error {
	if i.ent != nil {
		i.ent.Release()
		i.ent = nil
	}

	return i.tr.Close()
}

func (i *Iterator) fillHeap(section int) error {
	ent, err := i.tr.ReadNext(section)
	if err != nil {
		return err
	}
	if ent != nil {
		i.heap.PushEntry(section, ent)
	}
	return nil
}
