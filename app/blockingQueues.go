package main

type blockingClient struct {
    addr   string       // address of the client that is being blocked
    notify chan struct{}  // channel to notify when the client is no longer blocked (string to send the value that was pushed)
}

var blockingQueueForBlop = make(map[string][]blockingClient) // key: list name, value: queue of clients

func addBlockingClient(listKey string, client blockingClient) {
    blockingQueueForBlop[listKey] = append(blockingQueueForBlop[listKey], client)
}

func popBlockingClient(listKey string) (blockingClient, bool) {
    queue := blockingQueueForBlop[listKey]
    if len(queue) == 0 {
        return blockingClient{}, false
    }
    client := queue[0]
    blockingQueueForBlop[listKey] = queue[1:]
    return client, true
}

func removeBlockingClient(listKey string, addr string) {
    queue := blockingQueueForBlop[listKey]
    for i, client := range queue {
        if client.addr == addr {
            blockingQueueForBlop[listKey] = append(queue[:i], queue[i+1:]...)
            break
        }
    }
}