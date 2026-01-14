package main

import (
    "strconv"
    "sort"
)

type SortedSetEntry struct {
    Member string
    Score  float64
}

type SortedSet struct {
    Entries map[string]float64 // member: score
    Sorted  []SortedSetEntry   // keep sorted
}

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
    case SortedSet:
        return "sorted set"
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

func getOrCreateSortedSet(key string) SortedSet {
    sortedSet, ok := getSortedSet(key)
    if !ok {
        return SortedSet{
            Entries: make(map[string]float64),
            Sorted:  []SortedSetEntry{},
        }
    }
    return sortedSet
}

func getSortedSetMemberWithIndex(key, member string) (float64, int, bool) {
    sortedSet, ok := getSortedSet(key)
    if !ok {
        return 0, -1, false
    }
    for i, entry := range sortedSet.Sorted {
        if entry.Member == member {
            return entry.Score, i, true
        }
    }
    return 0, -1, false
}

func updateSortedSetMember(sortedSet *SortedSet, value SortedSetEntry) {
    // Update the score in the map
    sortedSet.Entries[value.Member] = value.Score

    // Update or add in the sorted slice
    for i, entry := range sortedSet.Sorted {
        if entry.Member == value.Member {
            sortedSet.Sorted[i].Score = value.Score
            return
        }
    }
    sortedSet.Sorted = append(sortedSet.Sorted, value)
}

func sortSortedSet(sortedSet *SortedSet) {
    sort.Slice(sortedSet.Sorted, func(i, j int) bool {
        if sortedSet.Sorted[i].Score == sortedSet.Sorted[j].Score {
            return sortedSet.Sorted[i].Member < sortedSet.Sorted[j].Member
        }
        return sortedSet.Sorted[i].Score < sortedSet.Sorted[j].Score
    })
}

func addToSortedSet(key string, value SortedSetEntry) SortedSet {
    sortedSet := getOrCreateSortedSet(key)
    updateSortedSetMember(&sortedSet, value)
    sortSortedSet(&sortedSet)
    store[key] = RedisValue{value: sortedSet}
    return sortedSet
}

func getSortedSet(key string) (SortedSet, bool) {
    val, ok := store[key]
    if !ok {
        return SortedSet{}, false
    }
    sortedSet, ok := val.value.(SortedSet)
    return sortedSet, ok
}

func removeFromSortedSet(key string, member string) (SortedSet, *SortedSetEntry) {
    sortedSet, ok := getSortedSet(key)
    if !ok {
        return sortedSet, nil
    }
    
    _, exists := sortedSet.Entries[member]
    if !exists {
        return sortedSet, nil
    }
    
    delete(sortedSet.Entries, member)
    
    // Remove from sorted slice and get the entry
    var removedEntry *SortedSetEntry
    for i, entry := range sortedSet.Sorted {
        if entry.Member == member {
            removedEntry = &sortedSet.Sorted[i]
            sortedSet.Sorted = append(sortedSet.Sorted[:i], sortedSet.Sorted[i+1:]...)
            break
        }
    }
    
    store[key] = RedisValue{value: sortedSet}
    return sortedSet, removedEntry
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