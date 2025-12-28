package main

import (
	"container/heap"
	"strconv"
	"time"
)

// This background worker is the single consumer of `expirables`.
// It has a priority queue with all records expiring in the future.
// It waits for the next expiry time to arrive,
// or a new expiry value to arrive in `expirables`.
func stringWriter() {
	expirables := &priorityQueue{}
	heap.Init(expirables)

	var timer *time.Timer
	var nextExpired <-chan time.Time
	for {
		nextExpired = nil
		for expirables.Len() > 0 {
			entry := (*expirables)[0]
			now := time.Now().UnixMilli()
			d := time.Duration(entry.expiry-now) * time.Millisecond
			if d <= 0 {
				heap.Pop(expirables)
				// Possibly remove from map.
				v, ok := stringMap.data[entry.key]
				if ok && v.expiry != 0 && v.expiry <= now {
					stringMap.mu.Lock()
					delete(stringMap.data, entry.key)
					stringMap.mu.Unlock()
				}
			} else {
				if timer == nil {
					timer = time.NewTimer(d)
				} else {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(d)
				}
				nextExpired = timer.C
				break
			}
		}
		select {
		case job := <-stringChan:
			if job.expiry != 0 {
				heap.Push(expirables, &priorityQueueEntry{key: job.key, expiry: job.expiry})
			}
			stringMap.mu.Lock()
			stringMap.data[job.key] = mapEntry{val: job.val, expiry: job.expiry}
			stringMap.mu.Unlock()
			job.written <- []string{"0"}
		case <-nextExpired:
		}
	}
}

func listWriter() {
	for job := range listChan {
		var result []string
		list, ok := listMap.data[job.args[0]]
		if !ok {
			// TODO: should POP create the list?
			// Init list.
			list = MakeDeque[string](1)
			listMap.mu.Lock()
			listMap.data[job.args[0]] = list
			listMap.mu.Unlock()
		}
		switch job.op {
		case rpush:
			listMap.mu.Lock()
			list.PushBack(job.args[1:]...)
			listMap.mu.Unlock()
			result = []string{strconv.FormatUint(list.Len(), 10)}
		case lpush:
			listMap.mu.Lock()
			list.PushFront(job.args[1:]...)
			listMap.mu.Unlock()
			result = []string{strconv.FormatUint(list.Len(), 10)}
		case lpop:
			var popCount uint64
			if len(job.args) == 1 {
				popCount = 1
			} else {
				// TODO: handle pasrse error.
				popCount, _ = strconv.ParseUint(job.args[1], 10, 64)
			}
			popCount = min(popCount, list.Len())
			result = make([]string, 0, popCount)
			listMap.mu.Lock()
			for range popCount {
				// Disregard `ok`, since we already checked `Len()`.
				popped, _ := list.PopFront()
				result = append(result, popped)
			}
		}
		// No need to store the list back, as it is a pointer.
		job.written <- result
	}
}

func (pq priorityQueue) Len() int           { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].expiry < pq[j].expiry }
func (pq priorityQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }
func (pq *priorityQueue) Push(x any)        { *pq = append(*pq, x.(*priorityQueueEntry)) }
func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}
