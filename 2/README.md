## Assignment 2 - Raft Leader Election with Go
Raft leader election protocol in Go.
Membership and log replication are not implemented.
Running the system will start a local leader election between all active nodes.
The amount of nodes is controlled in [`memberList.txt`](./memberList.txt).
Default is 4 nodes.

## Run
1. Membership is statically held in [`memberList.txt`](./memberList.txt).
Each entry in [`memberList.txt`](./memberList.txt) is a unique TCP socket address a node will use to communicate with other nodes. To add more nodes, add as many valid socket as desired separated by a newline.

2. To start a node, start a new terminal session and run the following command.
To start multiple nodes, start another new terminal session and run the command again.
If there are free sockets in [`memberList.txt`](./memberList.txt), the node will automatically use that socket.
    - `go run node.go`

## Enviornment
Written and tested using `go version go1.10.4 linux/amd64` and `go version go1.12 windows/amd64`
- `sudo apt install golang-go` 
