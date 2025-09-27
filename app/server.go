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
    WriteOffset     int
    LastAckedOffset int
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var ttl = make(map[string]time.Time)
var keys = []string{}
var ackReceived chan bool
var config serverConfig
var streamTopMilisecondsTimeForStream int
var entryIds = make(map[int]int) // key is milisecondsTime and value is sequenceNumber
var replicaAckOffsets = make(map[string]int) // key: replica address, value: last acked offset
var queuedCommands = make(map[string][][]string)
var channelSubscribers = make(map[string]map[string]struct{})
var commandHandlers map[string]func([]string, string) (string, bool)
var subscriberCommandHandlers map[string]func([]string, string) (string, bool)

func init() {
    commandHandlers = map[string]func([]string, string) (string, bool){
        "COMMAND":   func(cmd []string, addr string) (string, bool) { return commandResponse(), false },
        "REPLCONF":  func(cmd []string, addr string) (string, bool) { return replconfResponse(cmd, addr), false },
        "PSYNC":     func(cmd []string, addr string) (string, bool) { return psyncResponse(cmd) },
        "PING":      func(cmd []string, addr string) (string, bool) { return pingResponse(false), false },
        "ECHO":      func(cmd []string, addr string) (string, bool) { return echoResponse(cmd), false },
        "INFO":      func(cmd []string, addr string) (string, bool) { return infoResponse(cmd), false },
        "SET":       func(cmd []string, addr string) (string, bool) { return setResponse(cmd), false },
        "GET":       func(cmd []string, addr string) (string, bool) { return getResponse(cmd), false },
        "WAIT":      func(cmd []string, addr string) (string, bool) { return waitResponse(cmd), false },
        "CONFIG":    func(cmd []string, addr string) (string, bool) { return configResponse(cmd), false },
        "KEYS":      func(cmd []string, addr string) (string, bool) { return keysResponse(cmd), false },
        "TYPE":      func(cmd []string, addr string) (string, bool) { return typeResponse(cmd), false },
        "XADD":      func(cmd []string, addr string) (string, bool) { return xaddResponse(cmd), false },
        "XRANGE":    func(cmd []string, addr string) (string, bool) { return xrangeResponse(cmd), false },
        "XREAD":     func(cmd []string, addr string) (string, bool) { return xreadResponse(cmd, addr), false },
        "RPUSH":     func(cmd []string, addr string) (string, bool) { return rPushResponse(cmd), false },
        "LRANGE":    func(cmd []string, addr string) (string, bool) { return lRangeResponse(cmd), false },
        "LPUSH":     func(cmd []string, addr string) (string, bool) { return lPushResponse(cmd), false },
        "LLEN":      func(cmd []string, addr string) (string, bool) { return lLenResponse(cmd), false },
        "LPOP":      func(cmd []string, addr string) (string, bool) { return lPopResponse(cmd), false },
        "BLPOP":     func(cmd []string, addr string) (string, bool) { return bLPopResponse(cmd, addr), false },
        "INCR":      func(cmd []string, addr string) (string, bool) { return incrResponse(cmd), false },
        "MULTI":     func(cmd []string, addr string) (string, bool) { return multiResponse(addr), false },
        "EXEC":      func(cmd []string, addr string) (string, bool) { return execResponse(addr), false },
        "DISCARD":   func(cmd []string, addr string) (string, bool) { return discardResponse(addr), false },
        "SUBSCRIBE": func(cmd []string, addr string) (string, bool) { return subscribeResponse(cmd, addr), false },
    }

    subscriberCommandHandlers = map[string]func([]string, string) (string, bool){
        "SUBSCRIBE":    func(cmd []string, addr string) (string, bool) { return subscribeResponse(cmd, addr), false },
        "UNSUBSCRIBE":  func(cmd []string, addr string) (string, bool) { return pingResponse(true), false },
        "PSUBSCRIBE":   func(cmd []string, addr string) (string, bool) { return pingResponse(true), false },
        "PUNSUBSCRIBE": func(cmd []string, addr string) (string, bool) { return pingResponse(true), false },
        "PING":         func(cmd []string, addr string) (string, bool) { return pingResponse(true), false },
        "QUIT":         func(cmd []string, addr string) (string, bool) { return pingResponse(true), false },
    }
}

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
        addr := conn.RemoteAddr().String()
        response, resynch := handleCommand(cmd, addr)

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
    var arrSize int
    for scanner.Scan() {
        token := scanner.Text()
        if len(token) == 0 {
            continue
        }
        switch token[0] {
        case '*':
            arrSize, _ = strconv.Atoi(token[1:])
        case '$':
            if !scanner.Scan() {
                return nil, fmt.Errorf("unexpected end of input after $")
            }
            strToken := scanner.Text()
            cmd = append(cmd, strToken)
            arrSize--
        default:
            cmd = append(cmd, token)
            arrSize--
        }
        if arrSize == 0 {
            break
        }
    }
    return cmd, nil
}

func handleCommand(cmd []string, addr string) (response string, resynch bool) {
    command := strings.ToUpper(strings.TrimSpace(cmd[0]))

    if isSubscriber(addr) {
        handler, ok := subscriberCommandHandlers[command]
        if !ok {
            return encodeSimpleErrorResponse(fmt.Sprintf("Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", command)), false
        }
        return handler(cmd, addr)
    }

    if isInMulti(addr) && command != "EXEC" && command != "MULTI" && command != "DISCARD" {
        queuedCommands[addr] = append(queuedCommands[addr], cmd)
        return encodeSimpleString("QUEUED"), false
    }

    handler, ok := commandHandlers[command]
    if !ok {
        return encodeSimpleErrorResponse("Unknown command"), false
    }
    response, resynch = handler(cmd, addr)

    rawCmd := encodeStringArray(cmd)
    config.ReplOffset += len(rawCmd)

    // If the command is a write, propagate
    if isWriteCommand(command) {
        config.WriteOffset++
        propagate(cmd)
    }
    return
}

func isWriteCommand(command string) bool {
    switch command {
    case "SET", "XADD", "RPUSH", "LPUSH", "LPOP", "INCR":
        return true
    default:
        return false
    }
}

func isSubscriber(addr string) bool {
    return subscriberCount(addr) > 0
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