package extsort

import (
	"container/heap"
	"sort"
)

type memBuffer struct {
	size   int
	chunks [][]byte
	less   Less
}

func (b *memBuffer) Append(data []byte) {
	n := len(b.chunks)
	if n < cap(b.chunks) {
		b.chunks = b.chunks[:n+1]
	} else {
		b.chunks = append(b.chunks, nil)
	}
	b.chunks[n] = append(b.chunks[n][:0], data...)
	b.size += len(data)
}

func (b *memBuffer) ByteSize() int      { return b.size }
func (b *memBuffer) Len() int           { return len(b.chunks) }
func (b *memBuffer) Less(i, j int) bool { return b.less(b.chunks[i], b.chunks[j]) }
func (b *memBuffer) Swap(i, j int)      { b.chunks[i], b.chunks[j] = b.chunks[j], b.chunks[i] }
func (b *memBuffer) Sort()              { sort.Sort(b) }

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
	data    []byte
}

type minHeap struct {
	items []heapItem
	less  Less
}

func (h *minHeap) Len() int           { return len(h.items) }
func (h *minHeap) Less(i, j int) bool { return h.less(h.items[i].data, h.items[j].data) }
func (h *minHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *minHeap) Push(x interface{}) { h.items = append(h.items, x.(heapItem)) }
func (h *minHeap) Pop() interface{} {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}

func (h *minHeap) PushData(section int, data []byte) {
	heap.Push(h, heapItem{section: section, data: data})
}

func (h *minHeap) PopData() (int, []byte) {
	ent := heap.Pop(h).(heapItem)
	return ent.section, ent.data
}
