package main

type RedisValue struct {
    value interface{}
}

var store = make(map[string]RedisValue)

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
    default:
        return encodeBulkString("")
    }
}

func setString(key, value string) {
    store[key] = RedisValue{value: value}
}

func setInt(key string, value int) {
    store[key] = RedisValue{value: value}
}

func setList(key string, value []string) {
    store[key] = RedisValue{value: value}
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

func getList(key string) ([]string, bool) {
    val, ok := store[key]
    if !ok {
        return nil, false
    }
    listVal, ok := val.value.([]string)
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
