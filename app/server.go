package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type serverConfig struct {
	Port          	 int
	Role          	 string
	Replid        	 string
	ReplOffset    	 int
	ReplicaofHost 	 string
	ReplicaofPort 	 int
	Replicas 	  	 []net.Conn
	ListeningPort 	 string
	MasterReplOffset int
    MasterReplid     string
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var store = make(map[string]string)
var ttl = make(map[string]time.Time)
var config serverConfig

func main() {
	flag.IntVar(&config.Port, "port", 6379, "listen on specified port")
	flag.StringVar(&config.ReplicaofHost, "replicaof", "", "start server in replica mode of given host and port")
	flag.Parse()

    handleReplicaConfig()
	setRole()

	config.ListeningPort = strconv.Itoa(config.Port)
	config.MasterReplOffset = 0
    config.MasterReplid = randStringWithCharset(40, charset)
    config.ReplOffset = 0

	if config.Role == "slave" {
		masterConn, reader := connectToMaster()
		handleMasterConnection(masterConn, reader)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Port))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", config.Port)
		os.Exit(1)
	}
	fmt.Println("Listening on: ", listener.Addr().String())

	for id := 1; ; id++ {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go manageClientConnection(id, conn)
	}
}

func randStringWithCharset(length int, charset string) string {
    seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
    b := make([]byte, length)
    for i := range b {
        b[i] = charset[seededRand.Intn(len(charset))]
    }
    return string(b)
}

func setRole() {
    if len(config.ReplicaofHost) == 0 {
        config.Role = "master"
        config.Replid = randReplid()
    } else {
        config.Role = "slave"
        switch flag.NArg() {
        case 0:
            config.ReplicaofPort = 6379
        case 1:
            config.ReplicaofPort, _ = strconv.Atoi(flag.Arg(0))
        default:
            flag.Usage()
        }
    }
}

func manageClientConnection(id int, conn net.Conn) {
    defer conn.Close()
    fmt.Printf("[#%d] Client connected: %v\n", id, conn.RemoteAddr().String())
    scanner := bufio.NewScanner(conn)

    for {
        cmd, err := readCommand(scanner)
        if err != nil {
            fmt.Printf("[#%d] Error reading command: %v\n", id, err.Error())
            break
        }

        if len(cmd) == 0 {
            break
        }

        fmt.Printf("[#%d] Command = %v\n", id, cmd)
        response, resynch := handleCommand(cmd)

        bytesSent, err := conn.Write([]byte(response))
        if err != nil {
            fmt.Printf("[#%d] Error writing response: %v\n", id, err.Error())
            break
        }
        fmt.Printf("[#%d] Bytes sent: %d %q\n", id, bytesSent, response)

        if resynch {
            sendEmptyRDB(id, conn)
        }
    }

    fmt.Printf("[#%d] Client closing\n", id)
}

func readCommand(scanner *bufio.Scanner) ([]string, error) {
    cmd := []string{}
    var arrSize, strSize int
    for scanner.Scan() {
        token := scanner.Text()
        switch token[0] {
        case '*':
            arrSize, _ = strconv.Atoi(token[1:])
        case '$':
            strSize, _ = strconv.Atoi(token[1:])
        default:
            if len(token) != strSize {
                return nil, fmt.Errorf("wrong string size - got: %d, want: %d", len(token), strSize)
            }
            arrSize--
            strSize = 0
            cmd = append(cmd, token)
        }
        if arrSize == 0 {
            break
        }
    }
    return cmd, nil
}

func encodeBulkString(s string) string {
	if len(s) == 0 {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func encodeStringArray(arr []string) string {
	result := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		result += encodeBulkString(s)
	}
	return result
}

func handleCommand(cmd []string) (response string, resynch bool) {
    isWrite := false
    switch strings.ToUpper(cmd[0]) {
    case "COMMAND":
        response = commandResponse()
    case "REPLCONF":
        response = replconfResponse(cmd)
    case "PSYNC":
        response, resynch = psyncResponse(cmd)
    case "PING":
        response = pingResponse()
    case "ECHO":
        response = echoResponse(cmd)
    case "INFO":
        response = infoResponse(cmd)
    case "SET":
        isWrite = true
        response = setResponse(cmd)
    case "GET":
        response = getResponse(cmd)
    }
    if isWrite {
        propagate(cmd)
    }
    return
}

func sendAndCheckResponse(conn net.Conn, reader *bufio.Reader, command []string, expectedResponse string) {
    conn.Write([]byte(encodeStringArray(command)))
    response, _ := reader.ReadString('\n')
    if !strings.Contains(response, expectedResponse) {
        fmt.Printf("Error: Expected %s but got %s\n", expectedResponse, response)
        os.Exit(1)
    }
}

func randReplid() string {
	chars := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	result := make([]byte, 40)
	for i := range result {
		c := rand.Intn(len(chars))
		result[i] = chars[c]
	}
	return string(result)
}