package main

import (
	"os"
	"fmt"
	"net"
    "flag"
    "time"
	"strconv"
	"strings"
    "math/rand"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var info = map[string]string {
    "role": "master",
    "connected_slaves": "0",
    "master_replid": "",
    "master_repl_offset": "0",
}

func randStringWithCharset(length int, charset string) string {
    seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
    b := make([]byte, length)
    for i := range b {
        b[i] = charset[seededRand.Intn(len(charset))]
    }
    return string(b)
}

func main() {
    port := flag.Int("port", 6379, "port to listen on")
    replicaof := flag.String("replicaof", "", "replication of master")
    flag.Parse()

    l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
    if err != nil {
        fmt.Printf("Failed to bind to port %d\n", *port)
        os.Exit(1)
    }

    if *replicaof != "" {
        info["role"] = "slave"
    } else {
        info["master_replid"] = randStringWithCharset(40, charset)
    }

    fmt.Printf("Role: %s\n", *replicaof)
    fmt.Printf("Listening on %d\n", *port)

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
            switch strings.ToUpper(inputArr[0].Value.(string)) { // convert the command to uppercase to make it case-insensitive
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
            case "SET":
                fmt.Println("set message")
                msgLen := len(inputArr)
                fmt.Println(msgLen)
                if msgLen < 3 {
                    fmt.Println("Error: SET command requires 2 arguments")
                    return
                }
                fmt.Println(inputArr)
                if msgLen > 3 {
                    timeType := strings.ToUpper(inputArr[3].Value.(string)) // convert the command to uppercase to make it case-insensitive
                    if timeType != "EX" && timeType != "PX" {
                        fmt.Println("Error: invalid time type")
                        return
                    }
                    if _, err := strconv.Atoi(inputArr[4].Value.(string)); err != nil {
                        fmt.Println("Error: invalid time")
                        return
                    }
                    setResponse(conn, inputArr[1].Value.(string), inputArr[2].Value.(string), timeType, inputArr[4].Value.(string))
                } else {
                    setResponse(conn, inputArr[1].Value.(string), inputArr[2].Value.(string), "", "")
                }
            case "GET":
                fmt.Println("get message")
                if len(inputArr) != 2 {
                    fmt.Println("Error: GET command requires 1 argument")
                    return
                }
                getResponse(conn, inputArr[1].Value.(string))
            case "INFO":
                fmt.Println("info message")
                infoResponse(conn)
            default:
                fmt.Println("Error: unknown command")
                return
            }
        }
    }
}
