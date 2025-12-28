package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func pingFunc(_ parsed_request, _ chan []string) string {
	return toSimpleString("PONG")
}

func echoFunc(r parsed_request, _ chan []string) string {
	return toBulkString(r.args[0])
}

func setFunc(r parsed_request, written chan []string) string {
	entry := setJob{key: r.args[0], val: r.args[1], written: written}
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
	return simpleOK
}

func getFunc(r parsed_request, _ chan []string) string {
	stringMap.mu.RLock()
	v, ok := stringMap.data[r.args[0]]
	stringMap.mu.RUnlock()
	if ok && (v.expiry == 0 || time.Now().UnixMilli() < v.expiry) {
		return toBulkString(v.val)
	} else {
		return nullBulkString
	}
}

func lrangeFunc(r parsed_request, _ chan []string) string {
	if r.argc == 4 {
		// TODO: handle parse error
		start, _ := strconv.ParseInt(r.args[1], 10, 64)
		// TODO: handle parse error
		stop, _ := strconv.ParseInt(r.args[2], 10, 64)
		stringMap.mu.RLock()
		list, ok := listMap.data[r.args[0]]
		if !ok {
			return emptyArray
		}
		L := int64(list.Len())
		if start < 0 {
			start = max(L+start, 0)
		}
		if stop < 0 {
			stop = max(L+stop, 0)
		}
		if start < L && start <= stop {
			stop = min(stop, L-1)
			// Copy the slice contents so they cannot be modified while returning
			result, _ := list.SliceCopy(uint64(start), uint64(stop+1))
			stringMap.mu.RUnlock()
			return toRESPArray(result)
		} else {
			stringMap.mu.RUnlock()
			return emptyArray
		}
	} // TODO: handle error.
	return ""
}

func llenFunc(r parsed_request, _ chan []string) string {
	listMap.mu.RLock()
	list, ok := listMap.data[r.args[0]]
	listMap.mu.RUnlock()
	if ok {
		return toRESPInteger(int(list.Len()))
	} else {
		return toRESPInteger(0)
	}
}

func sendToListWriter(r parsed_request, written chan []string) []string {
	listChan <- listJob{op: r.command, args: r.args, written: written}
	return <-written
}

func rpushFunc(r parsed_request, written chan []string) string {
	length := sendToListWriter(r, written)
	L, _ := strconv.Atoi(length[0])
	return toRESPInteger(L)
}

func lpushFunc(r parsed_request, written chan []string) string {
	length := sendToListWriter(r, written)
	L, _ := strconv.Atoi(length[0])
	return toRESPInteger(L)
}

func lpopFunc(r parsed_request, written chan []string) string {
	response := sendToListWriter(r, written)
	if len(response) == 0 {
		return nullBulkString
	} else if len(r.args) == 1 {
		fmt.Println(toBulkString(response[0]))
		return toBulkString(response[0])
	} else {
		return toRESPArray(response)
	}
}
