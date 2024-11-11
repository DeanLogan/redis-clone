# Redis Clone 

This is a clone of the Redis database, implemented in Go. It aims to replicate some of the key features of Redis, including:

- Key-value storage: Like Redis, this clone allows you to store and retrieve data using keys.
- Master-replica replication: The `master.go` and `replica.go` files handle the implementation of master and replica nodes, allowing for data replication across multiple nodes.
- RDB persistence: The `rdbReading.go` file handles reading from RDB files, providing a form of data persistence.

## Structure

The project is structured as follows:

- `master.go`: Contains the implementation for the master node.
- `rdbReading.go`: Handles reading from RDB files.
- `replica.go`: Contains the implementation for the replica nodes.
- `responses.go`: Handles the responses sent by the server.
- `server.go`: Contains the server implementation.

## How to Run

To run this project, you need to have Go installed on your machine. Once you have Go installed, you can run the project using the following command:

```sh
go run app/server.go
```