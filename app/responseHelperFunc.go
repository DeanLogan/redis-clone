package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func checkIfAddrIsReplica(addr string) bool {
    isReplica := false
    for _, r := range config.Replicas {
        if r.RemoteAddr().String() == addr {
            isReplica = true
            break
        }
    }
    return isReplica
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

func isInRange(entryID, startID, endID string) bool {
    return (startID == "-" || entryID >= startID) && (endID == "+" || entryID <= endID)
}

// Parses optional arguments like COUNT and BLOCK, returns cleaned command, count, blockTime, error
func parseXreadArguments(cmd []string) ([]string, int, int, error) {
    count := -1
    blockTime := -1
    cleaned := []string{}
    i := 0
    for i < len(cmd) {
        arg := strings.ToUpper(cmd[i])
        if arg == "COUNT" && i+1 < len(cmd) {
            c, err := strconv.Atoi(cmd[i+1])
            if err != nil {
                return nil, -1, -1, fmt.Errorf("invalid COUNT value")
            }
            count = c
            i += 2
        } else if arg == "BLOCK" && i+1 < len(cmd) {
            b, err := strconv.Atoi(cmd[i+1])
            if err != nil {
                return nil, -1, -1, fmt.Errorf("invalid BLOCK value")
            }
            blockTime = b
            i += 2
        } else {
            cleaned = append(cleaned, cmd[i])
            i++
        }
    }
    return cleaned, count, blockTime, nil
}

func findStreamsIndex(cmd []string) int {
    for i, arg := range cmd {
        if strings.ToUpper(arg) == "STREAMS" {
            return i
        }
    }
    return -1
}

// Extracts stream keys and start IDs from the command
func extractStreamKeysAndIDs(cmd []string) ([]string, []string, error) {
    streamsIndex := findStreamsIndex(cmd)
    if streamsIndex == -1 || streamsIndex+1 >= len(cmd) {
        return nil, nil, fmt.Errorf("wrong number of arguments for 'xread' command")
    }
    n := (len(cmd) - streamsIndex - 1) / 2
    streamKeys := cmd[streamsIndex+1 : streamsIndex+1+n]
    startIDs := cmd[streamsIndex+1+n:]
    if len(streamKeys) != len(startIDs) {
        return nil, nil, fmt.Errorf("wrong number of arguments for 'xread' command")
    }
    return streamKeys, startIDs, nil
}

func normaliseStartIDs(streamKeys, startIDs []string) {
    for i, startID := range startIDs {
        if startID == "$" {
            streamKey := streamKeys[i]
            stream, ok := getStream(streamKey)
            if ok && len(stream.Entries) > 0 {
                startIDs[i] = stream.Entries[len(stream.Entries)-1].ID
            } else {
                startIDs[i] = "0-0"
            }
        }
    }
}

func fetchStreamEntries(streamKeys, startIDs []string, count int) []string {
    var result []string
    for i, streamKey := range streamKeys {
        startID := startIDs[i]
        stream, ok := getStream(streamKey)
        if !ok {
            continue
        }
        var entries []StreamEntry
        for _, entry := range stream.Entries {
            if entry.ID > startID {
                entries = append(entries, entry)
                if count > 0 && len(entries) >= count {
                    break
                }
            }
        }
        if len(entries) > 0 {
            result = append(result, encodeStreamWithKey(streamKey, entries))
        }
    }
    return result
}

func waitForNewStreamEntries(streamKeys []string, blockTime int, startIDs []string, count int, addr string) []string {
    key := streamKeys[0]
    var entries []string
    blockClient := blockingClient{addr, make(chan struct{})}
    addBlockingClient(key, blockClient, blockingQueueForXread)
    fmt.Println(streamKeys)
    fmt.Println(key)
    fmt.Println(blockTime)
    fmt.Println(startIDs)
    fmt.Println(count)
    fmt.Println(addr)
    if blockTime == 0 {
        // Block forever until notified
        <-blockClient.notify
        removeBlockingClient(key, addr, blockingQueueForXread)
        entries = fetchStreamEntries(streamKeys, startIDs, count)
    } else if blockTime > 0 {
        // waits depending on the block time which is measured in ms
        experationTime := time.Now().Add(time.Duration(blockTime * int(time.Millisecond)))
        timeoutChan := time.After(time.Until(experationTime))

        select {
        case <-blockClient.notify:
            removeBlockingClient(key, addr, blockingQueueForXread)
            entries = fetchStreamEntries(streamKeys, startIDs, count)
        case <-timeoutChan:
            removeBlockingClient(key, addr, blockingQueueForXread)
            return nil 
        }
    }
    return entries
}

func adjustIndex(indx int, arrLen int) int {
    if indx < 0 {
        indx = arrLen+1 + indx
    }
    return max(indx, 0)
}

func reverseSlice[T any](s []T) {
    for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
        s[i], s[j] = s[j], s[i]
    }
}

func parseRangeIndices(cmd []string, arrLen int) (int, int, bool) {
    startIndx, err1 := strconv.Atoi(cmd[2])
    stopIndx, err2 := strconv.Atoi(cmd[3])

    if err1 != nil || err2 != nil {
        return 0, 0, false
    }

    startIndx = adjustIndex(startIndx, arrLen)
    stopIndx = adjustIndex(stopIndx, arrLen)

    if startIndx > stopIndx {
        return 0, 0, false
    }
    if stopIndx > arrLen {
        stopIndx = arrLen
    }
    return startIndx, stopIndx, true
}

func handleBlockingPop(key string, addr string) []string {
    arr, _ := getList[string](key)
    removeBlockingClient(key, addr, blockingQueueForBlop)
    _, val := removeFromList(key, arr, 0)
    return []string{key, val}
}

func isInMulti(addr string) bool {
    _, ok := queuedCommands[addr]
    return ok
}

func subscribe(channel string, addr string) {
    if _, ok := channelSubscribers[channel]; !ok {
        channelSubscribers[channel] = make(map[string]struct{})
    }
    channelSubscribers[channel][addr] = struct{}{}
}

func unsubscribe(channel string, addr string) {
    if subs, ok := channelSubscribers[channel]; ok {
        delete(subs, addr)
        if len(subs) == 0 {
            delete(channelSubscribers, channel)
        }
    }
}

func subscriptionCount(addr string) int {
    count := 0
    for _, subs := range channelSubscribers {
        if _, ok := subs[addr]; ok {
            count++
        }
    }
    return count
}
