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

func waitForNewEntries(streamKeys, startIDs []string, count, blockTime int) []string {
    var result []string

    currentTime := time.Now().UnixNano() / int64(time.Millisecond)
    endTime := currentTime + int64(blockTime)
    if blockTime == 0 {
        // Block indefinitely until new entries are available
        for {
            result = checkForNewEntries(streamKeys, startIDs, count, currentTime, time.Now().UnixNano()+20/int64(time.Millisecond))
            if len(result) > 0 {
                break
            }
            time.Sleep(100 * time.Millisecond)
        }
    } else {        
        time.Sleep(time.Duration(blockTime) * time.Millisecond)
        
        result = checkForNewEntries(streamKeys, startIDs, count, currentTime, endTime)
    }

    return result
}

func checkForNewEntries(streamKeys, startIDs []string, count int, currentTime, endTime int64) []string {
    var result []string
    for i, streamKey := range streamKeys {
        startID := startIDs[i]
        stream, ok := getStream(streamKey)
        if !ok {
            continue
        }

        var entries []StreamEntry
        for _, entry := range stream.Entries {
            if entry.ID >= startID && entry.TimeReceivedAt > currentTime && entry.TimeReceivedAt <= endTime {
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

func parseOptionalArgumentsForXread(cmd []string) ([]string, int, int, error) {
    count := -1
    blockTime := -1

    for i := 1; i < len(cmd); i += 2 {
        var err error
        if strings.ToUpper(cmd[i]) == "COUNT" {
            count, err = strconv.Atoi(cmd[i+1])
            if err != nil {
                return cmd, count, blockTime, fmt.Errorf("invalid COUNT value")
            }
            cmd = append(cmd[:i], cmd[i+2:]...) // Remove COUNT argument from cmd
            break
        } else if strings.ToUpper(cmd[i]) == "BLOCK" {
            blockTime, err = strconv.Atoi(cmd[i+1])
            if err != nil {
                return cmd, count, blockTime, fmt.Errorf("invalid BLOCK value")
            }
            cmd = append(cmd[:i], cmd[i+2:]...) // Remove BLOCK argument from cmd
            break
        }
    }

    return cmd, count, blockTime, nil
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