package main

import (
    "encoding/base64"
    "errors"
    "flag"
    "fmt"
    "math/rand"
    "net"
    "os"
    "strconv"
    "strings"
    "time"
)

type Info struct {
    Role string
    ConnectedSlavesNum int
    MasterReplid string
    MasterReplOffset int
    MasterHost string
    MasterPort int
    SlavesConnected []net.Conn
    ListeningPort string
    Port int
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var info = Info {
    Role:               "master",
    ConnectedSlavesNum: 0,
    MasterReplid:       "",
    MasterReplOffset:   0,
    MasterHost:         "localhost",
    MasterPort:         6379,
    SlavesConnected:    []net.Conn{},
    ListeningPort:      "",
    Port:               6379,
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

    info.Port = *port

    if *replicaof != "" {
        info.Role = "slave"

        // reformats the repliconf string to be in the form of host:port
        *replicaof = strings.ReplaceAll(*replicaof, " ", ":")
        replicaofArr := strings.Split(*replicaof, ":")

        // adds the master host and port to the info map
        info.MasterHost = replicaofArr[0]
        info.MasterPort, _ = strconv.Atoi(replicaofArr[1])

        masterConn := connectToMaster(*replicaof)
        if masterConn == nil {
            fmt.Println("Error connecting to master")
            os.Exit(1)
        }
        
        handshake(masterConn, strconv.Itoa(*port))
    } else {
        info.MasterReplid = randStringWithCharset(40, charset)
    }

    fmt.Printf("Role: %s\n", info.Role)
    fmt.Printf("replica of: %s\n", *replicaof)
    fmt.Printf("Listening on %d\n", *port)

    for {
        conn, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting connection: ", err.Error())
            os.Exit(1)
        }

        fmt.Println("Connection accepted from: ", conn.RemoteAddr().String())
        cache["foo"] = "123"
        cache["bar"] = "456"
        cache["baz"] = "789"

        go handleConnection(conn)
    }
}

func handshake(conn net.Conn, port string) {
    pingToServer(conn)
    sendRepliconfToMaster(conn, "listening-port", port)
    time.Sleep(500 * time.Millisecond)
    sendRepliconfToMaster(conn, "capa", "pysync2")
    time.Sleep(500 * time.Millisecond)
    psyncToServer(conn)
}

func connectToMaster(serverAddr string) net.Conn {
    conn, err := net.Dial("tcp", serverAddr)
    if err != nil {
        fmt.Println("Error connecting to server: ", err.Error())
        return nil
    }
    return conn
}

func psyncToServer(conn net.Conn) {
    message := "*3\r\n" + createBulkString("PSYNC") + createBulkString("?") + createBulkString("-1")
    _, err := conn.Write([]byte(message))
    if err != nil {
        fmt.Println("error sending message: ")
        return
    }
}

func sendRepliconfToMaster(conn net.Conn, command string, port string) error {
    message := "*3\r\n" + createBulkString("REPLCONF") + createBulkString(command) + createBulkString(port)
    fmt.Println("Send Message To Master", conn.RemoteAddr().(*net.TCPAddr).Port)
    fmt.Println("Send Message To Master", conn.RemoteAddr().(*net.TCPAddr))
    fmt.Println("My port: ", info.Port)
    _, err := conn.Write([]byte(message))
    if err != nil {
        fmt.Println("error sending message: ")
        return errors.New("error sending message: " + err.Error())
    }

    buf := make([]byte, 1024)
    textStart, err := conn.Read(buf)
    if err != nil {
        fmt.Println("error reading: ")
        return errors.New("error reading: " + err.Error())
    }

    response, err := parseRespValue(string(buf[:textStart]))
    if err != nil {
        fmt.Println("error parsing resp value: ")
        return errors.New("error parsing resp value: " + err.Error())
    }
    if response.Value.(string) != "+OK\r\n" {
        fmt.Println("error response was not OK")
        return errors.New("error response was not OK")
    }

    return nil
}

func pingToServer(conn net.Conn) {
    _, err := conn.Write([]byte("*1\r\n"+createBulkString("PING")))
    if err != nil {
        fmt.Println("Error sending message: ", err.Error())
        return
    }
    time.Sleep(1 * time.Second)
}

func sendEmptyRDB(conn net.Conn) {
    fmt.Println("Sending empty RDB")
    base64String := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    data, err := base64ToBinary(base64String)
    if err != nil {
        fmt.Println("Error decoding base64 string:", err)
        return
    }

    _, err = conn.Write([]byte(fmt.Sprintf("$%s\r\n%v", strconv.Itoa(len(data)), data)))
    if err != nil {
        fmt.Println("Error sending message: ", err.Error())
        return
    }
}

func base64ToBinary(base64String string) (string, error) {
    data, err := base64.StdEncoding.DecodeString(base64String)
    if err != nil {
        return "", err
    }
    return string(data), nil
}

func handleConnection(conn net.Conn) {
    defer conn.Close()
    
    for {
        buf := make([]byte, 1024)
        textStart, err := conn.Read(buf)
        fmt.Println("textStart: ", textStart)
        if err != nil {
            fmt.Println("failed reading from connection")
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
                    setResponse(conn, inputArr[1].Value.(string), inputArr[2].Value.(string), timeType, inputArr[4].Value.(string), buf[:textStart])
                } else {
                    setResponse(conn, inputArr[1].Value.(string), inputArr[2].Value.(string), "", "", buf[:textStart])
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
            case "REPLCONF":
                fmt.Println("replconf message")
                if info.Role == "slave" {
                    fmt.Println("Error: slave cannot be a master")
                    return
                }
                if len(inputArr) != 3 {
                    fmt.Println("Error: REPLCONF command requires 2 arguments")
                    return
                }
                repliconfResponse(conn, inputArr[1].Value.(string), inputArr[2].Value.(string))
            case "PSYNC":
                fmt.Println("psync message")
                if info.Role == "slave" {
                    fmt.Println("Error: slave cannot be a master")
                    return
                }
                if len(inputArr) != 3 {
                    fmt.Println("Error: PSYNC command requires 2 arguments")
                    return
                }
                psyncResponse(conn)
                // handshake complete, slave has been connected add to the list of slaves and sends an empty RDB file
                info.SlavesConnected = append(info.SlavesConnected, conn)
                fmt.Println("Slaves connected: ", conn.RemoteAddr().String())
                sendEmptyRDB(conn)
            case "FULLRESYNC":
                fmt.Println("fullresync message")
                if info.Role == "slave" {
                    fmt.Println("Error: slave cannot be a master")
                    return
                }
                if len(inputArr) != 3 {
                    fmt.Println("Error: FULLRESYNC command requires 2 arguments")
                    return
                }
                sendEmptyRDB(conn)
            default:
                fmt.Println("Error: unknown command")
                return
            }
        }
    }
}
