package main

import (
    "net"
)

type blockingClient struct {
    conn   net.Conn       // address of the client that is being blocked
    notify chan struct{}  // channel to notify when the client is no longer blocked (string to send the value that was pushed)
}

// key: key for the value awaiting a response, value: queue of clients
var blockingQueueForBlop = make(map[string][]blockingClient)
var blockingQueueForXread = make(map[string][]blockingClient) 

func addBlockingClient(listKey string, client blockingClient, queueMap map[string][]blockingClient) {
    queueMap[listKey] = append(queueMap[listKey], client)
}

func popBlockingClient(listKey string, queueMap map[string][]blockingClient) (blockingClient, bool) {
    queue := queueMap[listKey]
    if len(queue) == 0 {
        return blockingClient{}, false
    }
    client := queue[0]
    queueMap[listKey] = queue[1:]
    return client, true
}

func removeBlockingClient(listKey string, conn net.Conn, queueMap map[string][]blockingClient) {
    queue := queueMap[listKey]
    for i, client := range queue {
        if client.conn == conn {
            queueMap[listKey] = append(queue[:i], queue[i+1:]...)
            break
        }
    }
}