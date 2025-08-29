package main

type StreamEntry struct {
    ID     string
    Fields map[string]string
    TimeReceivedAt int64
}

type RedisStream struct {
    Entries []StreamEntry
}

var store = make(map[string]RedisValue)

type RedisValue struct {
    value interface{}
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

func setStreamEntry(key, id string, fields map[string]string, timeReceivedAt int64) {
    stream, ok := store[key]
    if !ok {
        stream = RedisValue{value: RedisStream{Entries: []StreamEntry{}}}
    }
    redisStream, ok := stream.value.(RedisStream)
    if !ok {
        redisStream = RedisStream{Entries: []StreamEntry{}}
    }
    redisStream.Entries = append(redisStream.Entries, StreamEntry{ID: id, Fields: fields, TimeReceivedAt: timeReceivedAt})
    store[key] = RedisValue{value: redisStream}
}

func setString(key, value string) {
    store[key] = RedisValue{value: value}
}

func setInt(key string, value int) {
    store[key] = RedisValue{value: value}
}

func setList[T any](key string, value []T, prepend bool) []T {
    listVal, ok := store[key]
    if !ok {
        listVal = RedisValue{value: []T{}}
    }
    arr, ok := listVal.value.([]T)
    if !ok {
        arr = []T{}
    }
    if prepend {
        arr = append(value, arr...)
    } else {
        arr = append(arr, value...)
    }
    store[key] = RedisValue{value: arr}
    return arr
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
    strVal, ok := val.value.(string)
    return strVal, ok
}

func getInt(key string) (int, bool) {
    val, ok := store[key]
    if !ok {
        return 0, false
    }
    intVal, ok := val.value.(int)
    return intVal, ok
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