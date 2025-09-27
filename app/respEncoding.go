package main

import (
	"fmt"
    "strings"
)

func encodeBulkString(s string) string {
	if len(s) == 0 {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func encodeSimpleString(s string) string {
	if len(s) == 0 {
		return "+\r\n"
	}
	return fmt.Sprintf("+%s\r\n", s)
}

func encodeStringArray(arr []string) string {
	result := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		result += encodeBulkString(s)
	}
	return result
}

func wrapRespFragmentsAsArray(arr []string) string {
	result := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		result += s
	}
	return result
}

func encodeInt(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

func encodeStream(stream RedisStream) string {
    result := fmt.Sprintf("*%d\r\n", len(stream.Entries))
    for _, entry := range stream.Entries {
        result += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.ID), entry.ID)
        result += encodeStringMap(entry.Fields)
    }
    return result
}

func encodeStreamWithKey(streamKey string, entries []StreamEntry) string {
    result := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(streamKey), streamKey)
    result += fmt.Sprintf("*%d\r\n", len(entries))
    for _, entry := range entries {
        result += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.ID), entry.ID)
        result += encodeStringMap(entry.Fields)
    }
    return result
}

func encodeStringMap(m map[string]string) string {
    result := fmt.Sprintf("*%d\r\n", len(m)*2)
    for k, v := range m {
        result += encodeBulkString(k)
        result += encodeBulkString(v)
    }
    return result
}

func encodeSimpleErrorResponse(s string) string{
	if len(s) == 0 {
		return "-\r\n"
	}
	return fmt.Sprintf("-ERR %s\r\n", s)
}

func encodeStreamArray(entries []string) string {
    if len(entries) == 0 {
        return "*-1\r\n" // returns a null array
    }
    return fmt.Sprintf("*%d\r\n%s", len(entries), strings.Join(entries, ""))
}

func encodeRedisValue(rv RedisValue) string {
    switch v := rv.value.(type) {
    case string:
        return encodeBulkString(v)
    case int:
        return encodeInt(v)
    case []string:
        return encodeStringArray(v)
    case map[string]struct{}:
        set := make([]string, 0, len(v))
        for k := range v {
            set = append(set, k)
        }
        return encodeStringArray(set)
    case RedisStream:
        return encodeStream(v)
    default:
        return encodeBulkString("")
    }
}

func encodeRespValue(val RespValue) string {
    switch val.Type {
    case '+':
        return encodeSimpleString(val.Value.(string))
    case '-':
        return encodeSimpleErrorResponse(val.Value.(string))
    case ':':
        return encodeInt(val.Value.(int))
    case '$':
        return encodeBulkString(val.Value.(string))
    case '*':
        return encodeRespValueArray(val.Value.([]RespValue))
    default:
        return encodeBulkString("")
    }
}

func encodeRespValueArray(arr []RespValue) string {
    var sb strings.Builder
    sb.WriteString(fmt.Sprintf("*%d\r\n", len(arr)))
    for _, v := range arr {
        sb.WriteString(encodeRespValue(v))
    }
    return sb.String()
}