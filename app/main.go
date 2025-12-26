package main

import (
	"container/heap"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/alphadose/haxmap"
)

//=============================================================================
//	STRUCTS
//=============================================================================

type request struct {
	// argc is redundant, as it is just len(args) - 1
	argc uint64
	command
	args []string
}

type mapEntry struct {
	val    string
	expiry int64
}
type priorityQueueEntry mapEntry
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
)

//=============================================================================
//	CONSTANTS
//=============================================================================

const nullBulkString = "$-1\r\n"
const simpleOK = "+OK\r\n"

var commandsMap = map[string]command{
	"PING": ping,
	"ECHO": echo,
	"SET":  set,
	"GET":  get,
}

//=============================================================================
//	GLOBALS
//=============================================================================

// Concurrent hashmap.
var userMap *haxmap.Map[string, mapEntry] = haxmap.New[string, mapEntry]()

// MPSC channel to perform expired record cleanup.
var expirables chan string = make(chan string, 128)

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

	go expirationWorker()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go func() {
			defer conn.Close()
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

				n, err = conn.Write(dispatch(parseRequest(string(buf[:n]))))
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
func parseRequest(r string) request {
	tokens := strings.Split(r, "\r\n")
	// TODO: check it starts with '*'. Handle ParseUint error.
	argc, _ := strconv.ParseUint(tokens[0][1:], 10, 64)
	if argc == 0 {
		// TODO: return "empty request"
		return request{}
	}
	com, ok := parse_command(tokens[2])
	if !ok {
		fmt.Println("Error parsing command: command not found.")
		// TODO: return "command not supported"
		return request{}
	}
	args := make([]string, 0, argc-1)
	for i := 4; i < len(tokens); i += 2 {
		args = append(args, tokens[i])
	}
	return request{argc: argc, command: com, args: args}
}

func parse_command(s string) (command, bool) {
	c, ok := commandsMap[strings.ToUpper(s)]
	return c, ok
}

func dispatch(r request) []byte {
	var response string
	switch r.command {
	case ping:
		response = toSimpleString("PONG")
	case echo:
		response = toBulkString(r.args[0])
	case set:
		key := r.args[0]
		var expiry int64
		if len(r.args) == 4 {
			switch strings.ToUpper(r.args[2]) {
			case "EX":
				// TODO: check bounds and error.
				// TODO: check parsing error.
				n, _ := strconv.ParseUint(r.args[3], 10, 64)
				expiry = time.Now().Add(time.Duration(n) * time.Second).UnixMilli()
			case "PX":
				// TODO: check bounds and error.
				// TODO: check parsing error.
				n, _ := strconv.ParseUint(r.args[3], 10, 64)
				expiry = time.Now().Add(time.Duration(n) * time.Millisecond).UnixMilli()
			default:
				fmt.Println("Unsupported SET argument.")
			}
		}
		userMap.Set(key, mapEntry{r.args[1], expiry})
		// Only send on the channel after it has been set.
		if expiry != 0 {
			expirables <- key
		}
		response = simpleOK
	case get:
		if val, ok := userMap.Get(r.args[0]); ok && (val.expiry == 0 || time.Now().UnixMilli() < val.expiry) {
			response = toBulkString(val.val)
		} else {
			response = nullBulkString
		}
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

// This background worker is the single consumer of `expirables`.
// It has a priority queue with all records expiring in the future.
// It waits for the next expiry time to arrive,
// or a new expiry value to arrive in `expirables`.
func expirationWorker() {
	pq := &priorityQueue{}
	heap.Init(pq)

	var timer *time.Timer
	for {
		var nextExpired <-chan time.Time
		for pq.Len() > 0 {
			entry := (*pq)[0]
			now := time.Now().UnixMilli()
			d := time.Duration(entry.expiry-now) * time.Millisecond
			if d <= 0 {
				heap.Pop(pq)
				// TODO: possibly remove from map.
				val, ok := userMap.Get(entry.val)
				if ok && val.expiry == entry.expiry {
					// FIXME: race condition. Must use "CompareAndDel", but that doesn't exist.
					userMap.Del(entry.val)
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
		case newEntry := <-expirables:
			entry, ok := userMap.Get(newEntry)
			// False if someone already overwrote it.
			if ok {
				heap.Push(pq, &priorityQueueEntry{newEntry, entry.expiry})
			}
		case <-nextExpired:
		}

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
