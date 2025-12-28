package main

import (
	"container/heap"
	"strconv"
	"time"
)

type setJob struct {
	key, val string
	expiry   int64
	written  chan []string
}

type listJob struct {
	op      command
	args    []string
	written chan []string
}

type blpopStruct struct {
	response chan []string
	timedOut bool
}

type priorityQueueEntry[T any] struct {
	expiry int64
	data   T
}
type priorityQueue[T any] []*priorityQueueEntry[T]

// This background worker is the single consumer of `expirables`.
// It has a priority queue with all records expiring in the future.
// It waits for the next expiry time to arrive,
// or a new expiry value to arrive in `expirables`.
func stringWriter(state *sharedState) {
	expirables := &priorityQueue[string]{}
	heap.Init(expirables)

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	var expired <-chan time.Time
	for {
		expired = nil
		// Set timer for next expiring string.
		if entry, ok := expirables.Peek(); ok {
			d := time.Until(time.UnixMilli(entry.expiry))
			timer.Reset(d)
			expired = timer.C
		}

		select {
		// Perform SET op:
		case job := <-state.stringChan:
			if job.expiry != 0 {
				heap.Push(expirables, &priorityQueueEntry[string]{data: job.key, expiry: job.expiry})
			}
			state.stringMap.mu.Lock()
			state.stringMap.data[job.key] = stringMapEntry{val: job.val, expiry: job.expiry}
			state.stringMap.mu.Unlock()
			job.written <- []string{"0"}
		// Cleanup expired keys:
		case <-expired:
			now := time.Now().UnixMilli()
			var candidates []string
			for e, ok := expirables.Peek(); ok; e, ok = expirables.Peek() {
				if e.expiry <= now {
					heap.Pop(expirables)
					candidates = append(candidates, e.data)
				}
			}
			// Possibly remove from map.
			state.stringMap.mu.Lock()
			for _, c := range candidates {
				if v, ok := state.stringMap.data[c]; ok && v.expiry != 0 && v.expiry <= now {
					delete(state.stringMap.data, c)
				}
			}
			state.stringMap.mu.Unlock()
		}
	}
}

func listWriter(state *sharedState) {
	// Use a map to know who is waiting for responses for this key.
	// Use queues to maintain order between blocked requests.
	// Store pointers in both containers to maintain pointer stability.
	blockedChans := make(map[string]*Deque[*blpopStruct])
	expirables := &priorityQueue[*blpopStruct]{}
	heap.Init(expirables)

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	var expired <-chan time.Time
	for {
		select {
		case job := <-state.listChan:
			var result []string
			var pushed bool
			key := job.args[0]
			list, ok := state.listMap.data[key]
			if !ok {
				// TODO: should POP create the list?
				// Init list.
				list = MakeDeque[string](1)
				state.listMap.mu.Lock()
				state.listMap.data[key] = list
				state.listMap.mu.Unlock()
			}

			switch job.op {

			case rpush:
				state.listMap.mu.Lock()
				list.PushBack(job.args[1:]...)
				state.listMap.mu.Unlock()
				result = []string{strconv.FormatUint(list.Len(), 10)}
				pushed = true

			case lpush:
				state.listMap.mu.Lock()
				list.PushFront(job.args[1:]...)
				state.listMap.mu.Unlock()
				result = []string{strconv.FormatUint(list.Len(), 10)}
				pushed = true

			case lpop:
				var popCount uint64
				if len(job.args) == 1 {
					popCount = 1
				} else {
					// TODO: handle parse error.
					popCount, _ = strconv.ParseUint(job.args[1], 10, 64)
				}
				popCount = min(popCount, list.Len())
				result = make([]string, 0, popCount)
				state.listMap.mu.Lock()
				for range popCount {
					// Disregard `ok`, since we already checked `Len()`.
					popped, _ := list.PopFront()
					result = append(result, popped)
				}
				state.listMap.mu.Unlock()

			case blpop:
				// Fast path: no need to block.
				if !list.Empty() {
					state.stringMap.mu.Lock()
					popped, _ := list.PopFront()
					state.listMap.mu.Unlock()
					result = []string{popped}
					break
				}
				// TODO: handle parse error.
				duration, _ := strconv.ParseFloat(job.args[1], 64)
				key, val := job.args[0], &blpopStruct{response: job.written}
				// Has expiry.
				if duration != 0 {
					d := time.Duration(duration * float64(time.Second))
					expiry := time.Now().Add(d).UnixMilli()
					heap.Push(expirables, &priorityQueueEntry[*blpopStruct]{data: val, expiry: expiry})
					// We just pushed, so bypass `Peek`.
					if (*expirables)[0].expiry == expiry {
						timer.Reset(d)
						expired = timer.C
					}
				}
				// Queue already exists.
				if queue, ok := blockedChans[key]; ok {
					queue.PushBack(val)
				} else {
					// Queue doesn't exists.
					queue := MakeDeque[*blpopStruct](1)
					queue.PushBack(val)
					blockedChans[key] = queue
				}
			}

			if pushed {
				if queue, ok := blockedChans[key]; ok {
					for blpop, ok := queue.PopFront(); ok; blpop, ok = queue.PopFront() {
						// Disregard entries already flagged as expired.
						// Their channels have already received a response.
						if !blpop.timedOut {
							state.listMap.mu.Lock()
							e, _ := list.PopFront()
							state.listMap.mu.Unlock()
							blpop.response <- []string{key, e}
							break
						}
					}
					if queue.Empty() {
						delete(blockedChans, key)
					}
				}
			}

			job.written <- result

		case <-expired:
			now := time.Now().UnixMilli()
			for expired, ok := expirables.Peek(); ok; expired, ok = expirables.Peek() {
				if expired.expiry > now {
					break
				}
				expirables.Pop()
				// Invalidate timed out requests by modifying
				// the same struct pointed by the map's pointer.
				expired.data.timedOut = true
				// Tell the waiting goroutine it has timed out.
				expired.data.response <- nil
			}
		}
	}
}

func (pq priorityQueue[T]) Len() int           { return len(pq) }
func (pq priorityQueue[T]) Less(i, j int) bool { return pq[i].expiry < pq[j].expiry }
func (pq priorityQueue[T]) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }
func (pq *priorityQueue[T]) Push(x any)        { *pq = append(*pq, x.(*priorityQueueEntry[T])) }
func (pq *priorityQueue[T]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}
func (pq priorityQueue[T]) Peek() (e *priorityQueueEntry[T], ok bool) {
	if pq.Len() == 0 {
		return
	}
	e, ok = pq[0], true
	return
}
