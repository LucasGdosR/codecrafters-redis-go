package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type request struct {
	// argc is redundant, as it is just len(args) - 1
	argc int
	command
	args []string
}

type command int

const (
	ping command = iota + 1
	echo
	set
	get
)

const nullBulkString = "$-1\r\n"
const simpleOK = "+OK\r\n"

var commandsMap = map[string]command{
	"PING": ping,
	"ECHO": echo,
	"SET":  set,
	"GET":  get,
}

var userMap map[string]string = make(map[string]string)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

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
	// TODO: check it starts with '*'. Handle Atoi error.
	argc, _ := strconv.Atoi(tokens[0][1:])
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
		userMap[r.args[0]] = r.args[1]
		response = simpleOK
	case get:
		val, ok := userMap[r.args[0]]
		if !ok {
			response = nullBulkString
		} else {
			response = toBulkString(val)
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
