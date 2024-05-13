package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
	NULL   = '_'
	BOOLEANS = '#'
	DOUBLE = ','
	BIG_NUMBER = '('
	BULK_ERROR = '!'
	VERBATIM_STRING = '='
	MAPS = '%'
	SETS = '~'
	PUSH = '>'
)

type RespValue struct {
    Type  byte
    Value interface{}
}

func parseRespValue(msg string) (*RespValue, error) {
    var respValue RespValue
    var err error

    respValue.Type = msg[0]

    switch respValue.Type {
    case STRING, ERROR, NULL:
        respValue.Value, err = readLine(msg[1:])
    case INTEGER:
        respValue.Value, err = readInt(msg[1:])
    case BULK:
        respValue.Value, err = readBulkString(msg[1:])
    case ARRAY:
        respValue.Value, err = readArray(msg[1:])
    case BOOLEANS:
        respValue.Value, err = readBooleans(msg[1:])
    case DOUBLE:
        respValue.Value, err = readDouble(msg[1:])
    case BIG_NUMBER:
        respValue.Value, err = readBigNumber(msg[1:])
    case BULK_ERROR:
        respValue.Value, err = readBulkError(msg[1:])
    case VERBATIM_STRING:
        respValue.Value, err = readVerbatimString(msg[1:])
    case MAPS:
        respValue.Value, err = readMaps(msg[1:])
    case SETS:
        respValue.Value, err = readSets(msg[1:])
    case PUSH:
        respValue.Value, err = readPush(msg[1:])
    default:
        err = fmt.Errorf("Unknown type: %v", string(respValue.Type))
    }

    return &respValue, err
}

func readLine(msg string) (string, error) {
	idx := strings.Index(msg, "\r\n")
	if idx == -1 {
		return "", errors.New("invalid resp formatDD: "+msg)
	}

	return msg[:idx], nil
}

func readInt(msg string) (int, error) {
	intStr, err := readLine(msg)
	if err != nil {
		return 0, err
	}
	intVal, err := strconv.Atoi(intStr)
	if err != nil {
		return 0, err
	}
	return intVal, nil
}

func readBulkString(msg string) (string, error) {
	idx := strings.Index(msg, "\r\n")
	if idx == -1 {
		return "", errors.New("invalid resp formatHH")
	}
	size,  err := strconv.Atoi(msg[:idx])
	if err != nil {
		return "", err
	}
	newStartIdx := idx+2 // skip the first \r\n
	return msg[newStartIdx:size+newStartIdx], nil // this does mean if the size indicated is shorter than the actual size, we will return a substring of the actual size
}

func readArray(msg string) ([]*RespValue, error) {
	idx := strings.Index(msg, "\r\n")
	if idx == -1 {
		return nil, errors.New("invalid resp formatOO"+msg)
	}
	arrLen,  err := strconv.Atoi(msg[:idx])
	if err != nil {
		return nil, err
	}

    arr := []*RespValue{}
    msg = msg[idx+2:] // skip the first line
    // break msg into individual elements
    msgs := []string{}
    //msg = strings.ReplaceAll(msg, "\r\n", "\\r\\n") // uncomment to see the actual msg in the console for debugging
    str := string(msg[0])
    for i := 1; i < len(msg); i++ {
        if isRespType(msg[i]) {
            msgs = append(msgs, str)
            str = string(msg[i])
        } else {
            str += string(msg[i])
        }
    }
    msgs = append(msgs, str)

    for _, msg := range msgs {
        respValue, err := parseRespValue(msg)
        if err != nil {
            return nil, err
        }
        printRespValue(respValue)
        arr = append(arr, respValue)
    }
    if len(arr) != arrLen {
        return nil, errors.New("invalid resp formatFF")
    }
    return arr, nil
}

func readBooleans(msg string) (bool, error) {
	return false, errors.New("function not implemented")
}

func readDouble(msg string) (float64, error) {
	return 0.0, errors.New("function not implemented")
}

func readBigNumber(msg string) (int64, error) {
	return 0, errors.New("function not implemented")
}

func readBulkError(msg string) (string, error) {
	return "", errors.New("function not implemented")
}

func readVerbatimString(msg string) (string, error) {
	return "", errors.New("function not implemented")
}

func readMaps(msg string) (map[string]string, error) {
	return nil, errors.New("function not implemented")
}

func readSets(msg string) ([]string, error) {
	return nil, errors.New("function not implemented")
}

func readPush(msg string) ([]string, error) {
	return nil, errors.New("function not implemented")
}

func isRespType(val byte) bool {
    switch val {
    case STRING, ERROR, INTEGER, BULK, ARRAY, NULL, BOOLEANS, DOUBLE, BIG_NUMBER, BULK_ERROR, VERBATIM_STRING, MAPS, SETS, PUSH:
        return true
    default:
        return false
    }
}

func printRespValue(respValue *RespValue) {
    switch respValue.Type {
    case STRING, ERROR, NULL, BULK, BULK_ERROR, VERBATIM_STRING:
        if value, ok := respValue.Value.(string); ok {
            fmt.Println("String: ", value)
        }
    case INTEGER:
        if value, ok := respValue.Value.(int); ok {
            fmt.Println("Integer: ", value)
        }
	case BIG_NUMBER:
		if value, ok := respValue.Value.(int64); ok {
            fmt.Println("Integer 64: ", value)
        }
    case ARRAY, SETS, PUSH:
        if value, ok := respValue.Value.([]*RespValue); ok {
            for _, v := range value {
                printRespValue(v)
            }
            //fmt.Println("Array: ", value)
        }
    case BOOLEANS:
        if value, ok := respValue.Value.(bool); ok {
            fmt.Println("Booleans: ", value)
        }
    case DOUBLE:
        if value, ok := respValue.Value.(float64); ok {
            fmt.Println("Double: ", value)
        }
    case MAPS:
        if value, ok := respValue.Value.(map[string]string); ok {
            fmt.Println("Maps: ", value)
        }
    default:
        fmt.Println("Unknown type")
    }
}