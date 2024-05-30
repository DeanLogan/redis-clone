package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func handleReplicaConfig() {
    if len(config.ReplicaofHost) > 0 {
        parts := strings.Split(config.ReplicaofHost, " ")
        config.ReplicaofHost = parts[0]
        if len(parts) > 1 {
            config.ReplicaofPort, _ = strconv.Atoi(parts[1])
        }
    }
}

func connectToMaster() (net.Conn, *bufio.Reader) {
    masterConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.ReplicaofHost, config.ReplicaofPort))
    if err != nil {
        fmt.Printf("Failed to connect to master %v\n", err)
        os.Exit(1)
    }

    reader := bufio.NewReader(masterConn)
    handshake(masterConn, reader)

    return masterConn, reader
}

func handleMasterConnection(masterConn net.Conn, reader *bufio.Reader) {
    // receiving RDB (ignoring it for now)
    response, _ := reader.ReadString('\n')
    if response[0] != '$' {
        fmt.Printf("Invalid response\n")
        os.Exit(1)
    }

    rdbSize, _ := strconv.Atoi(response[1 : len(response)-2])
    buffer := make([]byte, rdbSize)
    receivedSize, err := reader.Read(buffer)
    if err != nil {
        fmt.Printf("Invalid RDB received %v\n", err)
        os.Exit(1)
    }
    if rdbSize != receivedSize {
        fmt.Printf("Size mismatch - got: %d, want: %d\n", receivedSize, rdbSize)
    }

    go syncWithMaster(reader, masterConn)
}

func handshake(masterConn net.Conn, reader *bufio.Reader) {
	sendAndCheckResponse(masterConn, reader, []string{"PING"}, "PONG")
	sendAndCheckResponse(masterConn, reader, []string{"REPLCONF", "listening-port", strconv.Itoa(config.Port)}, "OK")
	sendAndCheckResponse(masterConn, reader, []string{"REPLCONF", "capa", "psync2"}, "OK")
	sendAndCheckResponse(masterConn, reader, []string{"PSYNC", "?", "-1"}, "FULLRESYNC")
}

func syncWithMaster(reader *bufio.Reader, masterConn net.Conn) {
	scanner := bufio.NewScanner(reader)
	for {
		cmd, err := readCommand(scanner)
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(cmd) == 0 {
			break
		}
		fmt.Printf("[from master] Command = %q\n", cmd)
		response, _ := handleCommand(cmd)
		fmt.Printf("response = %q\n", response)
		if strings.ToUpper(cmd[0]) == "REPLCONF" {
			fmt.Printf("ack = %q\n", cmd)
			_, err := masterConn.Write([]byte(response))
			if err != nil {
				fmt.Printf("Error responding to master: %v\n", err.Error())
				break
			}
		}
	}
}