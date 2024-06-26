package main

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// commandResponse returns a standard OK response.
func commandResponse() string {
    return "+OK\r\n"
}

// replconfResponse handles the REPLCONF command.
// It processes the command and returns the appropriate response.
func replconfResponse(cmd []string) string {
	switch strings.ToUpper(cmd[1]) {
	case "GETACK":
		if config.Role != "slave" && len(cmd) < 2 {
			return errorResponse(fmt.Errorf("invalid replconf command"))
		} 
        if cmd[2] == "*"{
            return encodeStringArray([]string{"REPLCONF", "ACK", strconv.Itoa(config.ReplOffset-37)}) // subtracted 37 as ReplOffset includes the current command which should not be included in the response
        } else {
            return "+OK\r\n"
        }
        case "ACK":
            ackReceived <- true
            return ""
	case "CAPA":
		if config.Role != "master" && config.ListeningPort == "" {
			return errorResponse(fmt.Errorf("invalid replconf command"))
		}
		return "+OK\r\n"
	case "LISTENING-PORT":
		if config.Role != "master" && len(cmd) < 2 {
			return errorResponse(fmt.Errorf("invalid replconf command"))
		}
		config.ListeningPort = cmd[2]
		return "+OK\r\n"
	}
    return errorResponse(fmt.Errorf("invalid replconf command"))
}

// psyncResponse handles the PSYNC command.
// It processes the command and returns the appropriate response and a boolean indicating if a full resync is required.
func psyncResponse(cmd []string) (string, bool) {
    if len(cmd) == 3 {
        // TODO: Implement synch
        return fmt.Sprintf("+FULLRESYNC %s 0\r\n", config.Replid), true
    }
    return "", false
}

// pingResponse returns a PONG response.
func pingResponse() string {
    return "+PONG\r\n"
}

// echoResponse handles the ECHO command.
// It returns the echoed message.
func echoResponse(cmd []string) string {
    return encodeBulkString(cmd[1])
}

// infoResponse handles the INFO command.
// It returns the server information.
func infoResponse(cmd []string) string {
    if len(cmd) == 2 && strings.ToUpper(cmd[1]) == "REPLICATION" {
        response := ""
        v := reflect.ValueOf(config)
        t := v.Type()

        for i := 0; i < v.NumField(); i++ {
            field := v.Field(i)
            fieldName := toSnakeCase(t.Field(i).Name)
            if field.Kind() == reflect.Slice {
                response += fmt.Sprintf("%s:%d", fieldName, field.Len()) + "\r\n"
            } else {
                response += fmt.Sprintf("%s:%v", fieldName, field.Interface()) + "\r\n"
            }
        }
        response = response[:len(response)-2] // remove the last \r\n
		fmt.Println(encodeBulkString(response))
        return encodeBulkString(response)
    }
    return ""
}

// setResponse handles the SET command.
// It sets the value of a key in the store and returns an OK response.
func setResponse(cmd []string) string {
    key, value := cmd[1], cmd[2]
    store[key] = value
    if len(cmd) == 5 && strings.ToUpper(cmd[3]) == "PX" {
        expiration, _ := strconv.Atoi(cmd[4])
        ttl[key] = time.Now().Add(time.Millisecond * time.Duration(expiration))
    }
    return "+OK\r\n"
}

// getResponse handles the GET command.
// It returns the value of a key in the store.
func getResponse(cmd []string) string {
    // Check if cmd has at least two elements
    if len(cmd) < 2 {
        return encodeBulkString("ERR wrong number of arguments for 'get' command")
    }

    key := cmd[1]
    value, ok := store[key]
    if ok {
        expiration, exists := ttl[key]
        if !exists || expiration.After(time.Now()) {
            return encodeBulkString(value)
        } else if exists {
            delete(ttl, key)
            delete(store, key)
            return encodeBulkString("")
        }
    } 
    return encodeBulkString("")
}

// waitResponse handles the WAIT command.
// It waits for a specified number of replicas to acknowledge receipt of the write.
func waitResponse(cmd []string) string {
    count, err := strconv.Atoi(cmd[1])
    if err != nil {
        return errorResponse(err)
    }
    timeout, err := strconv.Atoi(cmd[2])
    if err != nil {
        return errorResponse(err)
    }
	fmt.Printf("Wait count=%d timeout=%d\n", count, timeout)
	propagate([]string{"REPLCONF", "GETACK", "*"})

	for i := 0; i < len(config.Replicas); i++ {
		go func(conn net.Conn) {
			fmt.Println("waiting response from replica", conn.RemoteAddr().String())
			buffer := make([]byte, 1024)
			// TODO: Ignoring result, just "flushing" the response
			_, err := conn.Read(buffer)
			if err == nil {
				fmt.Println("got response from replica", conn.RemoteAddr().String())
			} else {
				fmt.Println("error from replica", conn.RemoteAddr().String(), " => ", err.Error())
			}
			ackReceived <- true
		}(config.Replicas[i])
	}

	timer := time.After(time.Duration(timeout) * time.Millisecond)

	acks := 0
    outer:
    for acks < count {
        select {
        case <-ackReceived:
            acks++
            fmt.Println("acks =", acks)
        case <-timer:
            fmt.Println("timeout! acks =", acks)
            break outer
        }
    }

	return encodeInt(acks)
}

// configResponse handles the CONFIG command.
// It processes the command and returns the appropriate response.
func configResponse(cmd []string) string {
    fmt.Println("configResponse", len(cmd))
    if len(cmd) >= 3 {
        switch strings.ToUpper(cmd[1]) {
        case "GET":
            switch strings.ToLower(cmd[2]) {
            case "dir":
                return encodeStringArray([]string{"dir", config.Dir})
            case "dbfilename":
                return encodeStringArray([]string{"dbfilename", config.Dbfilename})
            }
        case "SET":
            switch strings.ToUpper(cmd[2]) {
            case "REPL-ROLE":
                if len(cmd) < 4 {
                    return errorResponse(fmt.Errorf("invalid config set command, REPL-ROLE requires a value"))
                }
                config.Role = cmd[3]
                return "+OK\r\n"
            case "REPL-ID":
                if len(cmd) < 4 {
                    return errorResponse(fmt.Errorf("invalid config set command, REPL-ID requires a value"))
                }
                config.Replid = cmd[3]
                return "+OK\r\n"
            case "REPL-ACK":
                if len(cmd) < 4 {
                    return errorResponse(fmt.Errorf("invalid config set command, REPL-ACK requires a value"))
                }
                config.ReplOffset, _ = strconv.Atoi(cmd[3])
                return "+OK\r\n"
            }
        }
    }
    return errorResponse(fmt.Errorf("invalid config set command, invalid number of arguments"))
}

// keysResponse handles the KEYS command.
// It returns all keys in the store.
func keysResponse(cmd []string) string {
    if cmd[1] != "*" {
        return errorResponse(fmt.Errorf("invalid number of arguments"))
    }
    for key := range store {
        keys = append(keys, key)
    }
    return encodeStringArray(keys)
}

// task sends a command to a replica and waits for the ACK response. Currently not used but has been left here incase it is useful in the future
func task(wg *sync.WaitGroup, done chan<- struct{}, conn net.Conn, cmd []string) {
    defer wg.Done()

    _, err := conn.Write([]byte(encodeStringArray(cmd)))
    if err != nil {
        fmt.Println("Error sending message: ", err.Error())
        return
    }

    buf := make([]byte, 1024)
    for {
        n, err := conn.Read(buf)
        if err != nil {
            fmt.Println("failed reading from connection")
            fmt.Println("Error reading:", err.Error())
            return
        }

        response := string(buf[:n])
        if strings.Contains(response, "ACK") {
            done <- struct{}{}
            return
        }
    }
}

// errorResponse formats an error message for a response.
func errorResponse(err error) string {
	return fmt.Sprintf("-%s\r\n", err.Error())
}

// toSnakeCase converts a CamelCase string to snake_case.
func toSnakeCase(str string) string {
    runes := []rune(str)
    length := len(runes)
    var result []rune

    for i := 0; i < length; i++ {
        if i > 0 && unicode.IsUpper(runes[i]) && ((i+1 < length && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
            result = append(result, '_')
        }
        result = append(result, unicode.ToLower(runes[i]))
    }

    return string(result)
}