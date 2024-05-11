package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
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
