package main

import (
    "strconv"
)

type StreamEntry struct {
    ID     string
    Fields map[string]string
}

type RedisStream struct {
    Entries []StreamEntry
}

var store = make(map[string]RedisValue)

type RedisValue struct {
    value interface{}
}

func getRedisValueType(rv RedisValue) string {
    switch rv.value.(type) {
    case string:
        return "string"
    case int:
        return "int"
    case []string:
        return "array"
    case map[string]struct{}:
        return "set"
    case RedisStream:
        return "stream"
    default:
        return "string"
    }
}

func addStreamEntry(key, id string, fields map[string]string) {
    stream, ok := store[key]
    if !ok {
        stream = RedisValue{value: RedisStream{Entries: []StreamEntry{}}}
    }
    redisStream, ok := stream.value.(RedisStream)
    if !ok {
        redisStream = RedisStream{Entries: []StreamEntry{}}
    }
    redisStream.Entries = append(redisStream.Entries, StreamEntry{ID: id, Fields: fields})

    client, ok := popBlockingClient(key, blockingQueueForXread)
    if ok {
        client.notify <- struct{}{}
    }

    store[key] = RedisValue{value: redisStream}
}

func setGenericValue[T any](key string, value T) {
    store[key] = RedisValue{value: value}
}

func addToList(key string, value []string, prepend bool) []string {
    listVal, ok := store[key]
    if !ok {
        listVal = RedisValue{value: []string{}}
    }
    arr, ok := listVal.value.([]string)
    if !ok {
        arr = []string{}
    }
    if prepend {
        arr = append(value, arr...)
    } else {
        arr = append(arr, value...)
    }
    
    store[key] = RedisValue{value: arr}
    
    client, ok := popBlockingClient(key, blockingQueueForBlop)
    if ok {
        client.notify <- struct{}{}
    }
    
    return arr
}

func removeFromList(key string, list []string, index int) ([]string, string) {
    var removedVal string
    if index < 0 || index >= len(list) {
        return list, removedVal
    }
    removedVal = list[index]
    newSlice := make([]string, len(list))
    copy(newSlice, list)
    arr := append(newSlice[:index], newSlice[index+1:]...)
    store[key] = RedisValue{value: arr}
    return arr, removedVal
}

func setSet(key string, value []string) {
    set := make(map[string]struct{})
    for _, v := range value {
        set[v] = struct{}{}
    }
    store[key] = RedisValue{value: set}
}

func getString(key string) (string, bool) {
    val, ok := store[key]
    if !ok {
        return "", false
    }
    switch v := val.value.(type) {
    case string:
        return v, true
    case int:
        return strconv.Itoa(v), true
    case float64:
        return strconv.FormatFloat(v, 'f', -1, 64), true
    default:
        return "", false
    }
}

func getInt(key string) (int, bool) {
    redisVal, exists := store[key]
    if !exists {
        return 0, true
    }
    switch value := redisVal.value.(type) {
    case int:
        return value, true
    case string:
        intValue, err := strconv.Atoi(value)
        if err == nil {
            return intValue, true
        }
    }
    return 0, false
}

func getList[T any](key string) ([]T, bool) {
    val, ok := store[key]
    if !ok {
        return nil, false
    }
    listVal, ok := val.value.([]T)
    return listVal, ok
}

func getSet(key string) ([]string, bool) {
    val, ok := store[key]
    if !ok {
        return nil, false
    }
    setVal, ok := val.value.(map[string]struct{})
    if !ok {
        return nil, false
    }
    set := make([]string, 0, len(setVal))
    for k := range setVal {
        set = append(set, k)
    }
    return set, true
}

func getStream(key string) (RedisStream, bool) {
    val, ok := store[key]
    if !ok {
        return RedisStream{}, false
    }
    streamVal, ok := val.value.(RedisStream)
    return streamVal, ok
}