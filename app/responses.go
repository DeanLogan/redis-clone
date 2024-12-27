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
    return encodeSimpleString("OK")
}

func xreadResponse(cmd []string) string {
    if len(cmd) < 4 || len(cmd)%2 != 0 {
        return encodeSimpleErrorResponse("wrong number of arguments for 'xread' command")
    }

    streamKey := cmd[2]
    startID := cmd[3]
    count := -1 

    for i := 4; i < len(cmd); i += 2 {
        if strings.ToUpper(cmd[i]) == "COUNT" {
            var err error
            count, err = strconv.Atoi(cmd[i+1])
            if err != nil {
                return encodeSimpleErrorResponse("invalid COUNT value")
            }
        }
    }

    stream, ok := getStream(streamKey)
    if !ok {
        return encodeSimpleErrorResponse("stream not found")
    }

    var result []StreamEntry
    for _, entry := range stream.Entries {
        if entry.ID >= startID {
            result = append(result, entry)
            if count > 0 && len(result) >= count {
                break
            }
        }
    }

    return encodeXReadResponse(streamKey, result)
}

func xrangeResponse(cmd []string) string {
    if len(cmd) != 4 {
        return encodeSimpleErrorResponse("wrong number of arguments for 'xrange' command")
    }

    streamKey := cmd[1]
    startID := cmd[2]
    endID := cmd[3]

    stream, ok := getStream(streamKey)
    if !ok {
        return encodeSimpleErrorResponse("stream not found")
    }

    var result []StreamEntry
    for _, entry := range stream.Entries {
        if isInRange(entry.ID, startID, endID) {
            result = append(result, entry)
        }
    }

    return encodeStream(RedisStream{Entries: result})
}

func isInRange(entryID, startID, endID string) bool {
    return (startID == "-" || entryID >= startID) && (endID == "+" || entryID <= endID)
}

func xaddResponse(cmd []string) string {
    if len(cmd) < 5 || len(cmd)%2 != 1 {
        return encodeSimpleErrorResponse("wrong number of arguments for 'xadd' command")
    }
    
    streamKey := cmd[1]
    entryId := cmd[2]

    if len(entryId) == 1 && entryId[0] == '*' {
        generateMilisecondTime(&entryId)
    } 

    if strings.HasSuffix(entryId, "*") {
        err := generateSequenceNumber(&entryId)
        if err != nil {
            return encodeSimpleErrorResponse(err.Error())
        }
    }

    msg := verifyStreamId(entryId)
    if msg != "" {
        return msg
    }

    fields := make(map[string]string)

    for i := 3; i < len(cmd); i += 2 {
        fields[cmd[i]] = cmd[i+1]
    }

    setStreamEntry(streamKey, entryId, fields)
    return encodeBulkString(entryId)
}

func generateMilisecondTime(entryId *string) {
    millis := time.Now().UnixNano() / int64(time.Millisecond)
    *entryId = strconv.FormatInt(millis, 10) + "-*"
}

func generateSequenceNumber(entryId *string) error {
    if len(*entryId) > 0 {
        *entryId = (*entryId)[:len(*entryId)-1]
    }

    milisecondTime, _, err := seperateStreamId(*entryId)
    if err != nil && err.Error() != "sequenceNumber failed" {
        return fmt.Errorf("invalid stream ID given for 'xadd' command, additional info: %s", err.Error())
    }

    currentSequenceNumber, exists := entryIds[milisecondTime]
    if !exists {
        if milisecondTime == 0 {
            *entryId += "1"
        } else {
            *entryId += "0"
        }
        return nil
    }

    *entryId += strconv.Itoa(currentSequenceNumber + 1)
    return nil
}

func verifyStreamId(entryId string) string {
    milisecondTime, sequenceNumber, err := seperateStreamId(entryId)
    if err != nil {
        return encodeSimpleErrorResponse("invalid stream ID given for 'xadd' command, additional info: "+err.Error())
    }

    if milisecondTime <= 0 && sequenceNumber <= 0 {
        return encodeSimpleErrorResponse("The ID specified in XADD must be greater than 0-0")
    }

    if milisecondTime < streamTopMilisecondsTimeForStream {
        return encodeSimpleErrorResponse("The ID specified in XADD is equal or smaller than the target stream top item")
    }

    currentSequenceNumber, exists := entryIds[milisecondTime]
    if exists && sequenceNumber <= currentSequenceNumber && milisecondTime == streamTopMilisecondsTimeForStream {
        return encodeSimpleErrorResponse("The ID specified in XADD is equal or smaller than the target stream top item")
    }

    // update stream top values
    streamTopMilisecondsTimeForStream = milisecondTime
    entryIds[milisecondTime] = sequenceNumber

    return ""
}

// seperate stream ID into milisecondTime and sequenceNumber
func seperateStreamId(id string) (int, int, error) {
    index := strings.LastIndex(id, string('-'))
    if index == -1 {
        return -1, -1, fmt.Errorf("invalid stream ID")
    }
    strTime, strSeqNum := id[:index], id[index+1:]

    milisecondTime, err := strconv.Atoi(strTime)
    if err != nil {
        return -1, -1, fmt.Errorf("milisecond failed")
    }
    sequenceNumber, err := strconv.Atoi(strSeqNum)
    if err != nil {
        return milisecondTime, -1, fmt.Errorf("sequenceNumber failed")
    }
    return milisecondTime, sequenceNumber, nil
}

func typeResponse(cmd []string) string {
    key := cmd[1]
    value, ok := store[key]
    if ok && !isExpired(key){
        return encodeSimpleString(getRedisValueType(value))
    }
    return encodeSimpleString("none")
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
            return encodeSimpleString("OK")
        }
        case "ACK":
            ackReceived <- true
            return ""
	case "CAPA":
		if config.Role != "master" && config.ListeningPort == "" {
			return errorResponse(fmt.Errorf("invalid replconf command"))
		}
		return encodeSimpleString("OK")
	case "LISTENING-PORT":
		if config.Role != "master" && len(cmd) < 2 {
			return errorResponse(fmt.Errorf("invalid replconf command"))
		}
		config.ListeningPort = cmd[2]
		return encodeSimpleString("OK")
	}
    return errorResponse(fmt.Errorf("invalid replconf command"))
}

func psyncResponse(cmd []string) (string, bool) {
    if len(cmd) == 3 {
        // TODO: Implement synch
        return encodeSimpleString(fmt.Sprintf("FULLRESYNC %s 0", config.Replid)), true 
    }
    return "", false
}

func pingResponse() string {
    return encodeSimpleString("PONG")
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
    setString(key, value)
    if len(cmd) == 5 && strings.ToUpper(cmd[3]) == "PX" {
        expiration, _ := strconv.Atoi(cmd[4])
        ttl[key] = time.Now().Add(time.Millisecond * time.Duration(expiration))
    }
    return encodeSimpleString("OK")
}

func getResponse(cmd []string) string {
    // TODO: check length
    key := cmd[1]
    value, ok := store[key]
    if ok && !isExpired(key){
        return encodeRedisValue(value)
    }
    return encodeBulkString("")
}

func isExpired(key string) bool {
    expiration, exists := ttl[key]
    if !exists {
        return false
    }
    if expiration.Before(time.Now()) {
        delete(ttl, key)
        delete(store, key)
        return true
    }
    return false
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
                return encodeSimpleString("OK") 
            case "REPL-ID":
                if len(cmd) < 4 {
                    return errorResponse(fmt.Errorf("invalid config set command, REPL-ID requires a value"))
                }
                config.Replid = cmd[3]
                return encodeSimpleString("OK")
            case "REPL-ACK":
                if len(cmd) < 4 {
                    return errorResponse(fmt.Errorf("invalid config set command, REPL-ACK requires a value"))
                }
                config.ReplOffset, _ = strconv.Atoi(cmd[3])
                return encodeSimpleString("OK")
            }
        }
    }
    return errorResponse(fmt.Errorf("invalid config set command, invalid number of arguments"))
}

func keysResponse(cmd []string) string {
    if cmd[1] != "*" {
        return errorResponse(fmt.Errorf("invalid number of arguments"))
    }
    for key := range store {
        keys = append(keys, key)
    }
    return encodeStringArray(keys)
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