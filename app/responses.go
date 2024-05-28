package main

import (
	"fmt"
	"net"
	"time"
)

var cache = make(map[string]string)

func pingResponse(conn net.Conn) {
	_, err := conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func echoResponse(conn net.Conn, msg string) {
    _, err := conn.Write([]byte(createBulkString(msg)))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func setResponse(conn net.Conn, key string, value string, timeType string, expiryTime string, messageSent []byte) {
	switch timeType {
	case "EX": // time is given in seconds
		err := expiryCache(expiryTime+"s", key)
		if err != nil {
			sendErrorResponse(conn)
		}
	case "PX": // time is given in miliseconds
		err := expiryCache(expiryTime+"ms", key)
		if err != nil {
			sendErrorResponse(conn)
		}
	}
	cache[key] = value
	if info["role"] == "master" {
		sendMessagesToSlaves(messageSent, slavesConnected)
	}
	_, err := conn.Write([]byte("+OK\r\n"))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func getResponse(conn net.Conn, key string) {
	value, ok := cache[key]
	if !ok {
		sendErrorResponse(conn)
		return
	}
	_, err := conn.Write([]byte(createBulkString(value)))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func infoResponse(conn net.Conn) {
	response := ""
	for key, value := range info {
		response += fmt.Sprintf("%s:%s", key, value) + "\r\n"
	}
	response = response[:len(response)-2] // remove the last \r\n
	_, err := conn.Write([]byte(createBulkString(response)))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func repliconfResponse(conn net.Conn, command string, port string) {
	switch command {
	case "listening-port":
		info["listening-port"] = port
		okResponse(conn)
	case "capa":
		if _, ok := info["listening-port"]; !ok {
			fmt.Println("Error: no listening port set")
			return 
		}
		okResponse(conn)
	}
}

func psyncResponse(conn net.Conn) {
	if _, ok := info["listening-port"]; !ok {
		fmt.Println("Error: no listening port set")
		return 
	}
	_, err := conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s %v\r\n", info["master_replid"], info["master_repl_offset"])))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func okResponse(conn net.Conn) {
	_, err := conn.Write([]byte("+OK\r\n"))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func expiryCache(expiryTime string, key string) (error) {
	duration, err := time.ParseDuration(expiryTime)
	if err != nil {
		return err
	}
	time.AfterFunc(duration, func() {
		delete(cache, key)
	})
	return nil
}

func sendErrorResponse(conn net.Conn) {
	_, err := conn.Write([]byte("$-1\r\n"))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func sendMessagesToSlaves(msg []byte, slaves []net.Conn) {
	for _, slave := range slaves {
		slave.Write(msg)
	}
}