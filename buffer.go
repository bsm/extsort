package extsort

import (
	"container/heap"

	"github.com/twotwotwo/sorts"
)

type memBuffer struct {
	size   int
	chunks []kv
	less   Less
}

func (b *memBuffer) Append(k, v []byte) {
	n := len(b.chunks)
	if n < cap(b.chunks) {
		b.chunks = b.chunks[:n+1]
	} else {
		b.chunks = append(b.chunks, kv{})
	}

	chunk := &b.chunks[n]
	chunk.k = append(chunk.k[:0], k...)
	chunk.v = append(chunk.v[:0], v...)
	b.size += len(k) + len(v)
}

func (b *memBuffer) ByteSize() int      { return b.size }
func (b *memBuffer) Len() int           { return len(b.chunks) }
func (b *memBuffer) Less(i, j int) bool { return b.less(b.chunks[i].k, b.chunks[j].k) }
func (b *memBuffer) Swap(i, j int)      { b.chunks[i], b.chunks[j] = b.chunks[j], b.chunks[i] }
func (b *memBuffer) Sort()              { sorts.Quicksort(b) }

func (b *memBuffer) Reset() {
	b.size = 0
	b.chunks = b.chunks[:0]
}

func (b *memBuffer) Free() {
	b.size = 0
	b.chunks = nil
}

// --------------------------------------------------------------------

type heapItem struct {
	section int
	data    kv
}

type minHeap struct {
	items []heapItem
	less  Less
}

func (h *minHeap) Len() int           { return len(h.items) }
func (h *minHeap) Less(i, j int) bool { return h.less(h.items[i].data.k, h.items[j].data.k) }
func (h *minHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *minHeap) Push(x interface{}) { h.items = append(h.items, x.(heapItem)) }
func (h *minHeap) Pop() interface{} {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}

func (h *minHeap) PushData(section int, data kv) {
	heap.Push(h, heapItem{section: section, data: data})
}

func (h *minHeap) PopData() (int, kv) {
	ent := heap.Pop(h).(heapItem)
	return ent.section, ent.data
}
