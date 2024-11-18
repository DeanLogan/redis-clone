package main

import (
	"fmt"
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

func encodeStringMap(m map[string]string) string {
    result := fmt.Sprintf("*%d\r\n", len(m)*2)
    for k, v := range m {
        result += encodeBulkString(k)
        result += encodeBulkString(v)
    }
    return result
}