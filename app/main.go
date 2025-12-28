package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
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
	lrange
	llen
	rpush
	lpush
	lpop
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
	"LPUSH":  lpush,
	"LLEN":   llen,
	"LPOP":   lpop,
}

var dispatchTable = [...]func(parsed_request, chan []string) string{
	ping:   pingFunc,
	echo:   echoFunc,
	set:    setFunc,
	get:    getFunc,
	lrange: lrangeFunc,
	llen:   llenFunc,
	rpush:  rpushFunc,
	lpush:  lpushFunc,
	lpop:   lpopFunc,
}

//=============================================================================
//	GLOBALS
//=============================================================================

var (
	// MPSC channels for a single writer thread.
	// Unbuffered because of single consumer and the producer needing to wait for it.
	stringChan chan setJob  = make(chan setJob)
	listChan   chan listJob = make(chan listJob)
	// Concurrent hashmaps. Redis actually does not split these. Could be `any` val.
	stringMap = struct {
		mu   sync.RWMutex
		data map[string]mapEntry
	}{data: make(map[string]mapEntry)}
	listMap = struct {
		mu   sync.RWMutex
		data map[string]*Deque[string]
	}{data: make(map[string]*Deque[string])}
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
			written := make(chan []string, 1)
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

func (r parsed_request) dispatch(written chan []string) []byte {
	return []byte(dispatchTable[r.command](r, written))
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
