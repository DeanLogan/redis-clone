package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
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
    Dir              string
    Dbfilename       string
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var ttl = make(map[string]time.Time)
var keys = []string{}
var ackReceived chan bool
var config serverConfig
var streamTopMilisecondsTimeForStream int
var entryIds = make(map[int]int) // key is milisecondsTime and value is sequenceNumber

func main() {
	flag.IntVar(&config.Port, "port", 6379, "listen on specified port")
	flag.StringVar(&config.ReplicaofHost, "replicaof", "", "start server in replica mode of given host and port")
	flag.StringVar(&config.Dir, "dir", "", "the path to the directory where the RDB file is stored")
	flag.StringVar(&config.Dbfilename, "dbfilename", "", "the name of the RDB file")
	flag.Parse()

    handleReplicaConfig()
	setRole()

	config.ListeningPort = strconv.Itoa(config.Port)
	config.MasterReplOffset = 0
    config.MasterReplid = randStringWithCharset(40, charset)
    config.ReplOffset = 0
    ackReceived = make(chan bool)

    if len(config.Dir) > 0 && len(config.Dbfilename) > 0 {
		rdbPath := filepath.Join(config.Dir, config.Dbfilename)
		err := readRDB(rdbPath)
		if err != nil {
			fmt.Printf("Failed to load '%s': %v\n", rdbPath, err)
		}
	}

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

    handshakeCommands := [][]string{
        {"PING"},
        {"REPLCONF", "listening-port", strconv.Itoa(config.Port)},
        {"REPLCONF", "capa", "psync2"},
        {"PSYNC", "?", "-1"},
    }
    handshakeIndex := 0

    for {
        cmd, err := readCommand(scanner)
        if err != nil {
            fmt.Printf("[#%d] Error reading command: %v\n", id, err.Error())
            break
        }

        if len(cmd) == 0 {
            break
        }

        // Check if the command matches the current handshake command
        if handshakeIndex < len(handshakeCommands) && reflect.DeepEqual(cmd, handshakeCommands[handshakeIndex]) {
            handshakeIndex++
            // once handshake index reaches the same length as the commands, the handshake is complete and the replica is added to the list
            if handshakeIndex == len(handshakeCommands) {
                fmt.Printf("[#%d] Handshake completed\n", id)
                config.Replicas = append(config.Replicas, conn)
            }
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
    var arrSize, strSize, byteLength int
    for scanner.Scan() {
        token := scanner.Text()
        byteLength = len(token) + byteLength + 2
        switch token[0] {
        case '*':
            if len(token) != 1 {
                arrSize, _ = strconv.Atoi(token[1:])
            } else {
                arrSize--
                strSize = 0
                cmd = append(cmd, token)
            }
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
    fmt.Println(byteLength)
    config.ReplOffset += byteLength
    return cmd, nil
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
    case "WAIT":
        response = waitResponse(cmd)
    case "CONFIG":
        response = configResponse(cmd)
    case "KEYS":
        response = keysResponse(cmd)
    case "TYPE":
        response = typeResponse(cmd)
    case "XADD":
        response = xaddResponse(cmd)
    }
    if isWrite {
        propagate(cmd)
    }
    return
}

func sendAndCheckResponse(conn net.Conn, reader *bufio.Reader, command []string, expectedResponse string) (bool, error) {
    conn.Write([]byte(encodeStringArray(command)))
    response, err := reader.ReadString('\n')
    if err != nil {
        fmt.Printf("Error reading response: %v\n", err)
        return false, err
    }
    if !strings.Contains(response, expectedResponse) {
        fmt.Printf("Error: Expected %s but got %s\n", expectedResponse, response)
        return false, nil
    }
    return true, nil
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