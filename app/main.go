package main

import (
	"container/heap"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//=============================================================================
//	STRUCTS
//=============================================================================

type request string

type parsed_request struct {
	// argc is redundant, as it is just len(args) - 1
	argc uint64
	command
	args []string
}

type setStruct struct {
	key, val string
	expiry   int64
	written  chan int
}

type rpushStruct struct {
	args    []string
	written chan int
}

type mapEntry struct {
	val    string
	expiry int64
}

type priorityQueueEntry struct {
	key    string
	expiry int64
}
type priorityQueue []*priorityQueueEntry

//=============================================================================
//	ENUMS
//=============================================================================

type command int

const (
	ping command = iota + 1
	echo
	set
	get
	rpush
	lrange
)

//=============================================================================
//	CONSTANTS
//=============================================================================

const nullBulkString = "$-1\r\n"
const simpleOK = "+OK\r\n"
const emptyArray = "*0\r\n"

var commandsMap = map[string]command{
	"PING":   ping,
	"ECHO":   echo,
	"SET":    set,
	"GET":    get,
	"RPUSH":  rpush,
	"LRANGE": lrange,
}

//=============================================================================
//	GLOBALS
//=============================================================================

var (
	// MPSC channels for a single writer thread.
	// Unbuffered because of single consumer and the producer needing to wait for it.
	stringChan chan setStruct   = make(chan setStruct)
	listChan   chan rpushStruct = make(chan rpushStruct)
	// Concurrent hashmaps. Redis actually does not split these. Could be `any` val.
	stringMap = struct {
		mu   sync.RWMutex
		data map[string]mapEntry
	}{data: make(map[string]mapEntry)}
	listMap = struct {
		mu   sync.RWMutex
		data map[string][]string
	}{data: make(map[string][]string)}
)

//=============================================================================
//	FUNCTIONS
//=============================================================================

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	// Redis does not have a writer per type.
	// Multi-key operations might require this to be rewritten.
	go stringWriter()
	go listWriter()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go func() {
			defer conn.Close()
			// Synchronize with writer thread before returning.
			// Buffered so writer doesn't block.
			written := make(chan int, 1)
			defer close(written)
			// What if 4KB doesn't fit? Read in a loop.
			buf := make([]byte, 4096)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					if err.Error() == "EOF" {
						break
					} else {
						fmt.Println("Error reading request: ", err.Error())
						os.Exit(1)
					}
				}
				n, err = conn.Write(request(buf[:n]).parse().dispatch(written))
				if err != nil {
					fmt.Println("Error writing response: ", err.Error())
					os.Exit(1)
				}
			}
		}()
	}
}

// This function could be defensive, double checking that every specified length matches the lenght,
// and it could also be efficient by parsing only the lengths and manualy slicing the `r` string
// instead of looking for CRLF. For now it is simple.
func (r request) parse() parsed_request {
	tokens := strings.Split(string(r), "\r\n")
	// TODO: check it starts with '*'. Handle ParseUint error.
	argc, _ := strconv.ParseUint(tokens[0][1:], 10, 64)
	if argc == 0 {
		// TODO: return "empty request"
		return parsed_request{}
	}
	com, ok := commandsMap[strings.ToUpper(tokens[2])]
	if !ok {
		fmt.Println("Error parsing command: command not found.")
		// TODO: return "command not supported"
		return parsed_request{}
	}
	args := make([]string, 0, argc-1)
	for i := 4; i < len(tokens); i += 2 {
		args = append(args, tokens[i])
	}
	return parsed_request{argc: argc, command: com, args: args}
}

func (r parsed_request) dispatch(written chan int) []byte {
	var response string
	switch r.command {
	case ping:
		response = toSimpleString("PONG")
	case echo:
		response = toBulkString(r.args[0])
	case set:
		entry := setStruct{key: r.args[0], val: r.args[1], written: written}
		if r.argc == 5 {
			var multiplier time.Duration
			switch strings.ToUpper(r.args[2]) {
			case "EX":
				multiplier = time.Second
			case "PX":
				multiplier = time.Millisecond
			default:
				fmt.Println("Unsupported SET argument.")
			}
			// TODO: check parsing error.
			n, _ := strconv.ParseUint(r.args[3], 10, 64)
			entry.expiry = time.Now().Add(time.Duration(n) * multiplier).UnixMilli()
		}
		// Send job to writer thread.
		stringChan <- entry
		// Wait for acknowledgment.
		<-written
		response = simpleOK
	case get:
		stringMap.mu.RLock()
		v, ok := stringMap.data[r.args[0]]
		stringMap.mu.RUnlock()
		if ok && (v.expiry == 0 || time.Now().UnixMilli() < v.expiry) {
			response = toBulkString(v.val)
		} else {
			response = nullBulkString
		}
	case rpush:
		listChan <- rpushStruct{args: r.args, written: written}
		length := <-written
		response = toRESPInteger(length)
	case lrange:
		if r.argc == 4 {
			// TODO: handle parse error
			start, _ := strconv.ParseInt(r.args[1], 10, 64)
			// TODO: handle parse error
			stop, _ := strconv.ParseInt(r.args[2], 10, 64)
			stringMap.mu.RLock()
			// Don't bother with OK, as empty list is what we want.
			list, _ := listMap.data[r.args[0]]
			if start < 0 {
				start = max(int64(len(list))+start, 0)
			}
			if stop < 0 {
				stop = max(int64(len(list))+stop, 0)
			}
			if start < int64(len(list)) && start <= stop {
				stop = min(stop, int64(len(list))-1)
				// Copy the slice contents so they cannot be modified while returning
				result := make([]string, stop+1-start)
				copy(result, list[start:stop+1])
				stringMap.mu.RUnlock()
				response = toRESPArray(result)
			} else {
				stringMap.mu.RUnlock()
				response = emptyArray
			}
		} // TODO: handle error.
	default:
		response = toSimpleString("Command not supported.")
	}
	return []byte(response)
}

func toSimpleString(s string) string {
	return fmt.Sprintf("+%v\r\n", s)
}

func toBulkString(s string) string {
	return fmt.Sprintf("$%v\r\n%v\r\n", len(s), s)
}

func toRESPInteger(i int) string {
	return fmt.Sprintf(":%v\r\n", i)
}

func toRESPArray(ss []string) string {
	response := make([]string, 0, len(ss)+1)
	response = append(response, fmt.Sprintf("*%v\r\n", len(ss)))
	for _, s := range ss {
		response = append(response, fmt.Sprintf("$%v\r\n%v\r\n", len(s), s))
	}
	return strings.Join(response, "")
}

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
			job.written <- 0
		case <-nextExpired:
		}
	}
}

func listWriter() {
	for job := range listChan {
		// We don't care about ok, because an empty slice is what we want for absent keys.
		list, _ := listMap.data[job.args[0]]
		listMap.mu.Lock()
		list = append(list, job.args[1:]...)
		listMap.data[job.args[0]] = list
		listMap.mu.Unlock()
		job.written <- len(list)
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
