package main

import (
	"fmt"
	"net"
	"os"
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

    for {
        buf := make([]byte, 1024)
        textStart, err := conn.Read(buf)
        if err != nil {
            fmt.Println("Error reading:", err.Error())
            return
        }

        msg := string(buf[:textStart])
        respValue, err := parseRespValue(msg)
        if err != nil{
            fmt.Println("Error parsing resp value: ", err.Error())
            return
        }
        if inputArr, ok := respValue.Value.([]*RespValue); ok {
            if len(inputArr) == 0 {
                fmt.Println("Error: empty array")
                return
            }
            switch inputArr[0].Value {
            case "PING":
                fmt.Println("ping message")
                pingResponse(conn)
            case "ECHO":
                fmt.Println("echo message")
                if len(inputArr) != 2 {
                    fmt.Println("Error: ECHO command requires 1 argument")
                    return
                }
                echoResponse(conn, inputArr[1].Value.(string))
            default:
                fmt.Println("Error: unknown command")
                return
            }
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
    _, err := conn.Write([]byte("$" + fmt.Sprint(len(msg)) + "\r\n" + msg + "\r\n"))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}
