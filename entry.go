package extsort

import "sync"

var entryPool sync.Pool

type entry struct {
	data   []byte
	keyLen int
}

func fetchEntry(kn, vn int) *entry {
	sz := kn + vn
	if v := entryPool.Get(); v != nil {
		if e := v.(*entry); sz <= cap(e.data) {
			e.data = e.data[:sz]
			e.keyLen = kn
			return e
		}
	}
	return &entry{
		data:   make([]byte, sz),
		keyLen: kn,
	}
}

func (e entry) Key() []byte {
	return e.data[:e.keyLen]
}

func (e entry) Val() []byte {
	return e.data[e.keyLen:]
}

func (e entry) ValLen() int {
	return len(e.data) - e.keyLen
}

func (e *entry) Release() {
	entryPool.Put(e)
}
