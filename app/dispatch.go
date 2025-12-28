package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const nullBulkString = "$-1\r\n"
const simpleOK = "+OK\r\n"
const emptyArray = "*0\r\n"
const nullArray = "*-1\r\n"

func pingFunc(s *sharedState, _ parsed_request, _ chan []string) string {
	return toSimpleString("PONG")
}

func echoFunc(s *sharedState, r parsed_request, _ chan []string) string {
	return toBulkString(r.args[0])
}

func setFunc(s *sharedState, r parsed_request, written chan []string) string {
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
	s.stringChan <- entry
	// Wait for acknowledgment.
	<-written
	return simpleOK
}

func getFunc(s *sharedState, r parsed_request, _ chan []string) string {
	s.stringMap.mu.RLock()
	v, ok := s.stringMap.data[r.args[0]]
	s.stringMap.mu.RUnlock()
	if ok && (v.expiry == 0 || time.Now().UnixMilli() < v.expiry) {
		return toBulkString(v.val)
	} else {
		return nullBulkString
	}
}

func lrangeFunc(s *sharedState, r parsed_request, _ chan []string) string {
	if r.argc == 4 {
		// TODO: handle parse error
		start, _ := strconv.ParseInt(r.args[1], 10, 64)
		// TODO: handle parse error
		stop, _ := strconv.ParseInt(r.args[2], 10, 64)
		s.stringMap.mu.RLock()
		list, ok := s.listMap.data[r.args[0]]
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
			s.stringMap.mu.RUnlock()
			return toRESPArray(result)
		} else {
			s.stringMap.mu.RUnlock()
			return emptyArray
		}
	} // TODO: handle error.
	return ""
}

func llenFunc(s *sharedState, r parsed_request, _ chan []string) string {
	s.listMap.mu.RLock()
	list, ok := s.listMap.data[r.args[0]]
	s.listMap.mu.RUnlock()
	if ok {
		return toRESPInteger(int(list.Len()))
	} else {
		return toRESPInteger(0)
	}
}

func sendToListWriter(s *sharedState, r parsed_request, written chan []string) []string {
	s.listChan <- listJob{op: r.command, args: r.args, written: written}
	return <-written
}

func rpushFunc(s *sharedState, r parsed_request, written chan []string) string {
	length := sendToListWriter(s, r, written)
	L, _ := strconv.Atoi(length[0])
	return toRESPInteger(L)
}

func lpushFunc(s *sharedState, r parsed_request, written chan []string) string {
	length := sendToListWriter(s, r, written)
	L, _ := strconv.Atoi(length[0])
	return toRESPInteger(L)
}

func lpopFunc(s *sharedState, r parsed_request, written chan []string) string {
	response := sendToListWriter(s, r, written)
	if len(response) == 0 {
		return nullBulkString
	} else if len(r.args) == 1 {
		fmt.Println(toBulkString(response[0]))
		return toBulkString(response[0])
	} else {
		return toRESPArray(response)
	}
}

func blpopFunc(s *sharedState, r parsed_request, written chan []string) string {
	response := sendToListWriter(s, r, written)
	if len(response) == 0 {
		return nullArray
	} else {
		return toRESPArray(response)
	}
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
