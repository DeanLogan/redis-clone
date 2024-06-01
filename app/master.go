package main

import (
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
)

func propagate(cmd []string) {
	if len(config.Replicas) == 0 {
		return
	}
	for i := 0; i < len(config.Replicas); i++ {
		fmt.Printf("Replicating to: %s\n", config.Replicas[i].RemoteAddr().String())
		_, err := config.Replicas[i].Write([]byte(encodeStringArray(cmd)))
		// remove stale replicas
		if err != nil {
			fmt.Printf("Disconnected: %s\n", config.Replicas[i].RemoteAddr().String())
			if len(config.Replicas) > 1 {
				last := len(config.Replicas) - 1
				config.Replicas[i] = config.Replicas[last]
				config.Replicas = config.Replicas[:last]
				i--
			}
		}
	}
}

func sendEmptyRDB(id int, conn net.Conn) {
    base64String := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    data, err := base64ToBinary(base64String)
    if err != nil {
        fmt.Println("Error decoding base64 string:", err)
        return
    }
	dataLen := strconv.Itoa(len(data))
    _, err = conn.Write([]byte(fmt.Sprintf("$%s\r\n%v", dataLen, data)))
    if err != nil {
        fmt.Println("Error sending message: ", err.Error())
        return
    }
	fmt.Printf("[#%d] full resynch sent: %s\n", id, dataLen)
	config.Replicas = append(config.Replicas, conn)
}

func base64ToBinary(base64String string) (string, error) {
    data, err := base64.StdEncoding.DecodeString(base64String)
    if err != nil {
        return "", err
    }
    return string(data), nil
}
