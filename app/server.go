package main

import (
	"bufio"
	_ "errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)


func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379") // port 6379 is the default port for Redis
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	fmt.Println("Listening on 6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		fmt.Println("Connection accepted")

		go handleConnection(conn)
	}
}



func handleConnection(conn net.Conn) {
    defer conn.Close()

    scanner := bufio.NewScanner(conn)
    for scanner.Scan() {
        msg := scanner.Text()
        if strings.TrimSpace(msg) == "PING" {
            pingResponse(conn)
        } else if msg[:4] == "ECHO" {
			echoResponse(conn, msg)
		} 
    }
}

func pingResponse(conn net.Conn) {
	_, err := conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func echoResponse(conn net.Conn, msg string) {

}

type RespType int

const (
    SimpleString RespType = iota
    ErrorResponse
    Integer
    BulkString
    Array
)

type RespValue struct {
    Type   RespType
    String string
    Int    int64
    Array  []*RespValue
}

func parseRespValue(reader *bufio.Reader) (*RespValue, error) {
    line, err := reader.ReadString('\n')
    if err != nil {
        return nil, err
    }

    prefix := line[0]
    payload := line[1 : len(line)-2] // Trim \r\n

    switch prefix {
    case '+':
        return &RespValue{Type: SimpleString, String: payload}, nil
    case '-':
        return &RespValue{Type: ErrorResponse, String: payload}, nil
    case ':':
        intValue, err := strconv.ParseInt(payload, 10, 64)
        if err != nil {
            return nil, err
        }
        return &RespValue{Type: Integer, Int: intValue}, nil
    case '$':
        length, err := strconv.Atoi(payload)
        if err != nil {
            return nil, err
        }
        if length == -1 {
            return &RespValue{Type: BulkString, String: ""}, nil
        }
        data := make([]byte, length)
        _, err = io.ReadFull(reader, data)
        if err != nil {
            return nil, err
        }
        // Discard \r\n
        reader.Discard(2)
        return &RespValue{Type: BulkString, String: string(data)}, nil
    case '*':
        numElements, err := strconv.Atoi(payload)
        if err != nil {
            return nil, err
        }
        array := make([]*RespValue, numElements)
        for i := range array {
            array[i], err = parseRespValue(reader)
            if err != nil {
                return nil, err
            }
        }
        return &RespValue{Type: Array, Array: array}, nil
    default:
        return nil, fmt.Errorf("unknown RESP type: %c", prefix)
    }
}

// splitAtStr splits a string at the first occurrence of a given string.
// It returns two strings: the part before the character and the part after the character.
func splitAtStr(str string, strToSplitAt string) (string, string) {
    index := strings.Index(str, strToSplitAt)
    if index != -1 {
        return str[:index], str[index+len(strToSplitAt):]
    }
    return str, ""
}