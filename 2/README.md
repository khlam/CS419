## Assignment 2 - Raft Leader Election with Go
Raft leader election protocol in Go.
Membership and log replication are not implemented.
Running the system will start a local leader election between all active nodes.
If the leader node's session is terminated, all alive nodes will start a new election.
The amount of nodes is set in [`memberList.txt`](./memberList.txt).
Default there are 5 nodes.

## Run
1. Membership is statically held in [`memberList.txt`](./memberList.txt).
Each line in [`memberList.txt`](./memberList.txt) is a unique TCP socket address a node will use to communicate with other nodes. To add more nodes, add as many valid socket as desired separated by a newline.

2. To start a node, start a new terminal session and run the following command.
To start multiple nodes, start another new terminal session and run the command again.
The node will automatically use the next free socket listed in [`memberList.txt`](./memberList.txt).
    - `go run node.go`

## Enviornment
Written and tested using `go version go1.10.4 linux/amd64` and `go version go1.12 windows/amd64`
- `sudo apt install golang-go` 
