package main

import "math/bits"

type Deque[T any] struct {
	buf              []T
	head, tail, mask uint64
}

func MakeDeque[T any](capacity uint64) *Deque[T] {
	capacity = ceilPow2(max(1, capacity))
	buf := make([]T, capacity)
	return &Deque[T]{buf: buf, mask: capacity - 1}
}

func (d *Deque[T]) Len() uint64 { return d.tail - d.head }
func (d *Deque[T]) Cap() uint64 { return uint64(len(d.buf)) }
func (d *Deque[T]) Empty() bool { return d.tail == d.head }
func (d *Deque[T]) Full() bool  { return d.Len() == d.Cap() }

func (d *Deque[T]) grow(minCapacity uint64) {
	newCap := ceilPow2(minCapacity)
	oldLen := d.Len()
	newBuf := make([]T, newCap)

	for i := uint64(0); i < oldLen; i++ {
		newBuf[i] = d.buf[(d.head+i)&d.mask]
	}

	d.buf = newBuf
	d.head = 0
	d.tail = oldLen
	d.mask = newCap - 1
}

func (d *Deque[T]) PushBack(ts ...T) {
	n := uint64(len(ts))
	if d.Len()+n > d.Cap() {
		d.grow(d.Len() + n)
	}
	for i, t := range ts {
		d.buf[(d.tail+uint64(i))&d.mask] = t
	}
	d.tail += n
}

// The last argument is the new front of the list.
func (d *Deque[T]) PushFront(ts ...T) {
	n := uint64(len(ts))
	if d.Len()+n > d.Cap() {
		d.grow(d.Len() + n)
	}
	base := d.head - 1
	for i, t := range ts {
		d.buf[(base-uint64(i))&d.mask] = t
	}
	d.head -= n
}

func (d *Deque[T]) PeekBack() (t T, ok bool) {
	if d.Empty() {
		return
	}
	t, ok = d.buf[(d.tail-1)&d.mask], true
	return
}

func (d *Deque[T]) PopBack() (t T, ok bool) {
	if t, ok = d.PeekBack(); ok {
		d.tail--
	}
	return
}

func (d *Deque[T]) PeekFront() (t T, ok bool) {
	if d.Empty() {
		return
	}
	t, ok = d.buf[d.head&d.mask], true
	return
}

func (d *Deque[T]) PopFront() (t T, ok bool) {
	if t, ok = d.PeekFront(); ok {
		d.head++
	}
	return
}

// [start, end) => Non-inclusive end, regular slice semantics.
func (d *Deque[T]) SliceCopy(start, end uint64) (t []T, ok bool) {
	if end > d.Len() || start >= end {
		return
	}
	t = make([]T, end-start)
	ok = d.CopySlice(start, t)
	return
}

// [start, end) => Non-inclusive end, regular slice semantics.
func (d *Deque[T]) SliceReverseCopy(start, end uint64) (t []T, ok bool) {
	if end > d.Len() || start >= end {
		return
	}
	t = make([]T, end-start)
	ok = d.CopyReverseSlice(end-1, t)
	return
}

// Copy `len(buf)` elements starting at `start` index.
// Returns false if not enough elements in the queue.
func (d *Deque[T]) CopySlice(start uint64, buf []T) bool {
	L := uint64(len(buf))
	if start+L > d.Len() {
		return false
	}

	base := d.head + start
	for i := range L {
		buf[i] = d.buf[(base+i)&d.mask]
	}

	return true
}

// Copy `len(buf)` elements starting at `end` index.
// Return false if `end` is out of bounds or the buffer is too big.
func (d *Deque[T]) CopyReverseSlice(end uint64, buf []T) bool {
	L := uint64(len(buf))
	if L-2 >= end || end >= d.Len() {
		return false
	}

	base := d.head + end
	for i := range L {
		buf[i] = d.buf[(base-i)&d.mask]
	}

	return true
}

func (d *Deque[T]) At(i uint64) (t T, ok bool) {
	if i >= d.Len() {
		return
	}
	t, ok = d.buf[(d.head+i)&d.mask], true
	return
}

func ceilPow2(x uint64) uint64 {
	msb := 63 - bits.LeadingZeros64(x)
	var result uint64 = 1 << msb
	if result < x {
		result <<= 1
	}
	return result
}
