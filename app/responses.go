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

func commandResponse() string {
    return "+OK\r\n"
}

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

func psyncResponse(cmd []string) (string, bool) {
    if len(cmd) == 3 {
        // TODO: Implement synch
        return fmt.Sprintf("+FULLRESYNC %s 0\r\n", config.Replid), true
    }
    return "", false
}

func pingResponse() string {
    return "+PONG\r\n"
}

func echoResponse(cmd []string) string {
    return encodeBulkString(cmd[1])
}

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

func setResponse(cmd []string) string {
    key, value := cmd[1], cmd[2]
    store[key] = value
    if len(cmd) == 5 && strings.ToUpper(cmd[3]) == "PX" {
        expiration, _ := strconv.Atoi(cmd[4])
        ttl[key] = time.Now().Add(time.Millisecond * time.Duration(expiration))
    }
    return "+OK\r\n"
}

func getResponse(cmd []string) string {
    // TODO: check length
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

func errorResponse(err error) string {
	return fmt.Sprintf("-%s\r\n", err.Error())
}

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