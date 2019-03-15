package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
	"math/rand"
)

func remove(s []string, i int) []string {
    s[len(s)-1], s[i] = s[i], s[len(s)-1]
    return s[:len(s)-1]
}

func determineMembership(path string) (string, []string) {
	file, _ := os.Open(path)

	defer file.Close()
  
	var members []string
	var address string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		members = append(members, scanner.Text())
	}
	for i, port := range members {
		address = "localhost:" + port
		listen, err := net.Listen("tcp", address)
		if err != nil {
			//fmt.Printf("\tINIT: %s is taken.\n", address)
			continue
		}
		members = remove(members, i)
		listen.Close()
		break
	}
	return address, members
}

func handleConnection(conn net.Conn) {
	//remoteAddr := conn.RemoteAddr().String()
	//fmt.Println("Client connected from " + remoteAddr)

	scanner := bufio.NewScanner(conn)

	for {
		ok := scanner.Scan()

		if !ok {
			break
		}

		handleMessage(scanner.Text(), conn)
	}

	//fmt.Println("Client at " + remoteAddr + " disconnected.")
}

func handleMessage(message string, conn net.Conn) {
	fmt.Println("> "+ time.Now().String() + "\t" + message)
}

func heartbeat(members []string, myAddress string) {
	for true{
		var sleep time.Duration = time.Duration(rand.Intn(1)+5) * time.Second // Heartbeat sleep between 1-5 seconds
		fmt.Println("\tTimeout ",sleep)
		time.Sleep(sleep)
		for _, port := range members {
			var addr = "localhost:" + port
			conn, err := net.DialTimeout("tcp", addr, 1 * time.Second)

			if err != nil {
				fmt.Printf("\t%s No Response\n", addr)
			}else {
				conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
				conn.Write([]byte("/heartbeat | " + myAddress))
				conn.Close()
			}
		}
	}
}

func main() {
	var myAddress, members = determineMembership("memberList.txt")

	listener, _ := net.Listen("tcp", myAddress)
	fmt.Printf("I am %s ", myAddress + "\n")


	go heartbeat(members, myAddress)

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Connection error: %s\n", err)
		}

		go handleConnection(conn)
	}
}