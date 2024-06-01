package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
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
        }
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

func setResponse(cmd []string) string {
    // TODO: check length
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

func errorResponse(err error) string {
	return fmt.Sprintf("-%s\r\n", err.Error())
}