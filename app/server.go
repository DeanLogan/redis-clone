package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	_"strconv"
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

func responseParser(msg string) (string, error) {
	response := ""
	switch msg[0] {
		case '+': // Simple string
			response := msg[1:]
			return response, nil
		case '-': // Error
			return "", errors.New(msg[1:])
		case ':': // Integer
			return response, nil
		case '$': // Bulk string
			response := msg[1:]
			return response, nil
		case '*': // Array
			_, msg := splitAtStr(msg, "\r\n")
			return responseParser(msg)
		}
	return response, nil
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