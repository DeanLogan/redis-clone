package main

import (
	"fmt"
	"net"
	"reflect"
	"time"
	"unicode"
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
	fmt.Println("cache>", cache)
	fmt.Println("key>", key)
	fmt.Println("value>", value)
	if info.Role == "master" {
		sendMessagesToSlaves(messageSent, info.SlavesConnected)
		// ok response is not sent if the message is sent to the slaves
		_, err := conn.Write([]byte("+OK\r\n"))
		if err != nil {
			fmt.Println("Error sending response: ", err.Error())
			return
		}
	}
}

func getResponse(conn net.Conn, key string) {
	value, ok := cache[key]
	fmt.Println(info.Role)
	fmt.Println(info.ListeningPort)
	fmt.Println("myPort>", info.Port)
	fmt.Println("connected to>", conn.RemoteAddr().String())
	fmt.Println("cache>", cache)
	fmt.Println("Key>", key)
	fmt.Println("Value>", value)
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
    v := reflect.ValueOf(info)
    t := v.Type()

    for i := 0; i < v.NumField(); i++ {
        field := v.Field(i)
        fieldName := toSnakeCase(t.Field(i).Name)
        if field.Kind() == reflect.Slice {
            response += fmt.Sprintf("%s:%d", fieldName, field.Len()) + "\r\n"
        } else {
            response += fmt.Sprintf("%s:%v", fieldName, field.Interface()) + "\r\n"
        }
    }
    response = response[:len(response)-2] // remove the last \r\n
    _, err := conn.Write([]byte(createBulkString(response)))
    if err != nil {
        fmt.Println("Error sending response: ", err.Error())
        return
    }
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

func repliconfResponse(conn net.Conn, command string, port string) {
	switch command {
	case "listening-port":
		fmt.Println("Listening port set to: ", port)
		info.ListeningPort = port
		okResponse(conn)
	case "capa":
		if info.ListeningPort == "" {
			fmt.Println("Error: no listening port set")
			return 
		}
		okResponse(conn)
	}
}

func psyncResponse(conn net.Conn) {
	if info.ListeningPort == "" {
		fmt.Println("Error: no listening port set")
		return 
	}
	_, err := conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s %v\r\n", info.MasterReplid, info.MasterReplOffset)))
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