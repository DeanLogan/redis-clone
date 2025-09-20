package main

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func commandResponse() string {
    return encodeSimpleString("OK")
}

func xreadResponse(cmd []string, addr string) string {
    parsedCmd, count, blockTime, err := parseXreadArguments(cmd)
    if err != nil {
        return encodeSimpleErrorResponse(err.Error())
    }

    streamKeys, startIDs, err := extractStreamKeysAndIDs(parsedCmd)
    if err != nil {
        return encodeSimpleErrorResponse(err.Error())
    }

    normaliseStartIDs(streamKeys, startIDs)

    entries := fetchStreamEntries(streamKeys, startIDs, count)
    if len(entries) == 0 && blockTime >= 0 {
        entries = waitForNewStreamEntries(streamKeys, blockTime, startIDs, count, addr)
    }
    
    if entries == nil {
        return encodeBulkString("") // returns null as bulk string (-1)
    }
    return encodeStreamArray(entries)
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

    addStreamEntry(streamKey, entryId, fields)
    return encodeBulkString(entryId)
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

func typeResponse(cmd []string) string {
    key := cmd[1]
    value, ok := store[key]
    if ok && !isExpired(key){
        return encodeSimpleString(getRedisValueType(value))
    }
    return encodeSimpleString("none")
}

func replconfResponse(cmd []string, addr string) string {
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
        ackOffset, _ := strconv.Atoi(cmd[2])
        // Only update if this connection is a known replica
        if checkIfAddrIsReplica(addr) {
            replicaAckOffsets[addr] = ackOffset
            ackReceived <- true
        }
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
    setGenericValue(key, value)
    if len(cmd) == 5 && strings.ToUpper(cmd[3]) == "PX" {
        expiration, _ := strconv.Atoi(cmd[4])
        ttl[key] = time.Now().Add(time.Millisecond * time.Duration(expiration))
    }
    return encodeSimpleString("OK")
}

func rPushResponse(cmd []string) string {
    key := cmd[1]
    values := cmd[2:]
    arr := addToList(key, values, false)
    return encodeInt(len(arr))
}

func lRangeResponse(cmd []string) string {
    key := cmd[1]
    arr, ok := getList[string](key)

    if !ok {
        return encodeStringArray([]string{})
    }

    arrLen := len(arr)-1
    startIndx, stopIndx, valid := parseRangeIndices(cmd, arrLen)

    if !valid {
        return encodeStringArray([]string{})
    }
    
    return encodeStringArray(arr[startIndx:stopIndx+1])
}

func lPushResponse(cmd []string) string {
    key := cmd[1]
    values := cmd[2:]
    reverseSlice(values)
    arr := addToList(key, values, true)
    return encodeInt(len(arr))
}

func lLenResponse(cmd []string) string {
    key := cmd[1]
    val, ok := store[key]
    if !ok {
        return encodeInt(0)
    }
    v := reflect.ValueOf(val.value)
    if v.Kind() == reflect.Slice {
        return encodeInt(v.Len())
    }
    return encodeInt(0)
}

func lPopResponse(cmd []string) string {
    key := cmd[1]
    arr, ok := getList[string](key)
    if !ok {
        return encodeBulkString("") // null bulk string
    }

    // returns bulk string if no optional command is given
    if len(cmd) <= 2 {
        _, val := removeFromList(key, arr, 0)
        return encodeBulkString(val)
    }

    removeFromEndStr := cmd[2]

    removeFromEnd, err := strconv.Atoi(removeFromEndStr)
    if err != nil || removeFromEnd > len(arr) {
        return encodeStringArray(arr)
    }
    
    valsPopped := []string{}
    var val string
    for i := 0; i<removeFromEnd; i++ {
        arr, val = removeFromList(key, arr, 0)
        valsPopped = append(valsPopped, val)
    }
    return encodeStringArray(valsPopped)
}

func bLPopResponse(cmd []string, addr string) string {
    key := cmd[1]
    timoutStr := cmd[2]
    timeout, err := strconv.ParseFloat(timoutStr, 64)
    if err != nil {
        return "$-1\r\n"
    }
    
    blockClient := blockingClient{addr, make(chan struct{})}
    addBlockingClient(key, blockClient, blockingQueueForBlop)
    if timeout == 0 {
        <-blockClient.notify
        arr := handleBlockingPop(key, addr)
        return encodeStringArray(arr)
    }

    experationTime := time.Now().Add(time.Duration(timeout * float64(time.Second)))
    timeoutChan := time.After(time.Until(experationTime))
    
    select {
    case <-blockClient.notify:
        arr := handleBlockingPop(key, addr)
        return encodeStringArray(arr)
    case <-timeoutChan:
        removeBlockingClient(key, addr, blockingQueueForBlop)
        return "$-1\r\n"
    }
}

func getResponse(cmd []string) string {
    // TODO: check length
    key := cmd[1]
    value, ok := getString(key)
    if !ok || isExpired(key){
        value = ""
    }
    return encodeBulkString(value)
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
    // return num of connected replicas if ACK was never sent before WAIT
    if(acks == 0) {
        acks = len(config.Replicas)
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

func errorResponse(err error) string {
	return fmt.Sprintf("-%s\r\n", err.Error())
}

func incrResponse(cmd []string) string {
    key := cmd[1]
    intVal, ok := getInt(key)
    if !ok {
        return encodeSimpleErrorResponse("value is not an integer or out of range")
    }
    intVal++
    setGenericValue(key, intVal)
    return encodeInt(intVal)
}