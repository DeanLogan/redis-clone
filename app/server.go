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

// serverConfig is a struct that holds the configuration for the server.
type serverConfig struct {
    Port          	 int        // The port the server listens on
    Role          	 string     // The role of the server (master or slave)
    Replid        	 string     // The replication ID of the server
    ReplOffset    	 int        // The replication offset of the server
    ReplicaofHost 	 string     // The host of the master server if this server is a slave
    ReplicaofPort 	 int        // The port of the master server if this server is a slave
    Replicas 	  	 []net.Conn // The connections to the replicas of this server
    ListeningPort 	 string     // The port the server is listening on
    MasterReplOffset int        // The replication offset of the master server
    MasterReplid     string     // The replication ID of the master server
    Dir              string     // The directory where the RDB file is stored
    Dbfilename       string     // The name of the RDB file
}


const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // charset is a string of all possible characters that can be used to generate a random replication ID.
var store = make(map[string]string) // store is a map that holds the key-value pairs of the database.
var ttl = make(map[string]time.Time) // ttl is a map that holds the time-to-live (expiration time) for each key in the store.
var keys = []string{} // keys is a slice that holds all the keys in the store.
var ackReceived chan bool // ackReceived is a channel used to signal when an ACK has been received.
var config serverConfig // config is an instance of serverConfig that holds the current configuration of the server.

func main() {
    // Parse command line flags
    flag.IntVar(&config.Port, "port", 6379, "listen on specified port")
    flag.StringVar(&config.ReplicaofHost, "replicaof", "", "start server in replica mode of given host and port")
    flag.StringVar(&config.Dir, "dir", "", "the path to the directory where the RDB file is stored")
    flag.StringVar(&config.Dbfilename, "dbfilename", "", "the name of the RDB file")
    flag.Parse()

    // Handle replica configuration and set server role
    handleReplicaConfig()
    setRole()

    // Set additional configuration parameters
    config.ListeningPort = strconv.Itoa(config.Port)
    config.MasterReplOffset = 0
    config.MasterReplid = randStringWithCharset(40, charset)
    config.ReplOffset = 0
    ackReceived = make(chan bool)

    // If a directory and RDB file name are provided, attempt to read the RDB file
    if len(config.Dir) > 0 && len(config.Dbfilename) > 0 {
        rdbPath := filepath.Join(config.Dir, config.Dbfilename)
        err := readRDB(rdbPath)
        if err != nil {
            fmt.Printf("Failed to load '%s': %v\n", rdbPath, err)
        }
    }

    // If the server is a slave, connect to the master and handle the connection
    if config.Role == "slave" {
        masterConn, reader := connectToMaster()
        handleMasterConnection(masterConn, reader)
    }

    // Start listening for incoming connections
    listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Port))
    if err != nil {
        fmt.Printf("Failed to bind to port %d\n", config.Port)
        os.Exit(1)
    }
    fmt.Println("Listening on: ", listener.Addr().String())

    // Accept incoming connections and handle each one in a separate goroutine
    for id := 1; ; id++ {
        conn, err := listener.Accept()
        if err != nil {
            fmt.Println("Error accepting connection: ", err.Error())
            os.Exit(1)
        }
        go manageClientConnection(id, conn)
    }
}

// randStringWithCharset generates a random string of the given length using the provided charset.
func randStringWithCharset(length int, charset string) string {
    seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
    b := make([]byte, length)
    for i := range b {
        b[i] = charset[seededRand.Intn(len(charset))]
    }
    return string(b)
}

// setRole sets the role of the server based on the provided configuration.
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

// manageClientConnection manages a client connection, handling incoming commands and sending responses.
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

// readCommand reads a command from the provided scanner and returns it as a slice of strings.
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

// encodeBulkString encodes a string as a Redis bulk string.
func encodeBulkString(s string) string {
	if len(s) == 0 {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// encodeStringArray encodes a slice of strings as a Redis array.
func encodeStringArray(arr []string) string {
	result := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		result += encodeBulkString(s)
	}
	return result
}

// encodeInt encodes an integer as a Redis integer.
func encodeInt(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

// handleCommand handles a command, returning the appropriate response and a boolean indicating whether a resynchronization is required.
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
    }
    if isWrite {
        propagate(cmd)
    }
    return
}

// sendAndCheckResponse sends a command to the provided connection and checks the response against the expected response.
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

// randReplid generates a random replication ID.
func randReplid() string {
	chars := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	result := make([]byte, 40)
	for i := range result {
		c := rand.Intn(len(chars))
		result[i] = chars[c]
	}
	return string(result)
}