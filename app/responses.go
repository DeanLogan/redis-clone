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
    _, err := conn.Write([]byte("$" + fmt.Sprint(len(msg)) + "\r\n" + msg + "\r\n"))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func setResponse(conn net.Conn, key string, value string, timeType string, expiryTime string) {
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

func getResponse(conn net.Conn, key string) {
	value, ok := cache[key]
	if !ok {
		sendErrorResponse(conn)
		return
	}
	_, err := conn.Write([]byte("$" + fmt.Sprint(len(value)) + "\r\n" + value + "\r\n"))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}

func sendErrorResponse(conn net.Conn) {
	_, err := conn.Write([]byte("$-1\r\n"))
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		return
	}
}