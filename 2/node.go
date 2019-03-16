package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
	"math/rand"
	"strconv"
	"strings"
)

var myPort string = ""
var memberList []string

var state string = "follower"
var term int = 0
var votesForMe = 0
var votedFor string = ""
var totalAlive = 1 // Init to 1 because we are alive

var timer *time.Timer = time.NewTimer(time.Duration(30 + rand.Intn(60-30+1)) * time.Second)

// Removes item at index i from an array and returns the array
func remove(s []string, i int) []string {
    s[len(s)-1], s[i] = s[i], s[len(s)-1]
    return s[:len(s)-1]
}

// Opens memberList.txt and reads the list of ports
func determineMembership() (string, []string) {
	file, _ := os.Open("memberList.txt")

	defer file.Close()
  
	var members []string
	var myPort string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		members = append(members, scanner.Text()) // Puts ports into an array
	}
	for i, port := range members {
		listen, err := net.Listen("tcp", "localhost:" + port) // Checks if a port is already in use
		if err != nil {
			//fmt.Printf("\tINIT: %s is taken.\n", address)
			continue							  // If the port is in use continue
		}
		members = remove(members, i)			  // If the port is not in use then remove it it from the list
		myPort = port							  // and set it as our port
		listen.Close()
		break
	}
	return myPort, members						  // Return the address we're using and the rest of the members
}

func handleConnection(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for {
		ok := scanner.Scan()
		if !ok {
			break
		}
		handleMessage(scanner.Text(), conn)
	}
}

func handleMessage(message string, conn net.Conn) {
	msg := strings.Split(message, "|")
	var directive string = msg[0]
	var msgTerm int
	msgTerm, _ = strconv.Atoi(msg[1])
	var id string = msg[2]
	
	fmt.Printf("> %s, TERM: %d, SENDER: %s\n", directive, term, id)
	if (term < msgTerm) {
		state = "follower"
		term = msgTerm
		votesForMe = 0
		votedFor = ""
		totalAlive = 1
		fmt.Printf("Port: %s|State: %s|Term: %d\n", myPort, state, term)
	}

	if (state == "follower") {

		if (directive == "leaderHeartbeat") {
			
			fmt.Printf("\tLeader: %s\tTerm: %d\n", id, term)
			if (msgTerm < term) {
				fmt.Printf("\tTerm correction. Leader is behind terms. %s\n", id)
				conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
				conn.Write([]byte("termCorrection|" + strconv.Itoa(term) + "|"+ myPort))
				conn.Close()
			}else {
				resetElectionTimeout()
			}
		}

		if ((directive == "RequestVote") && (votedFor == "") && (term <= msgTerm)) {
			resetElectionTimeout()
			votedFor = id
			conn, _ := net.Dial("tcp", "localhost:"+id)
			fmt.Printf("\tI voted for %s\n", id)
			conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
			conn.Write([]byte("VoteForX|" + strconv.Itoa(term) + "|"+ myPort))
			conn.Close()
		}
	}

	if (state == "candidate") {
		if ((directive == "VoteForX") && (msgTerm == term)) {
			fmt.Printf("\t%s Voted for me.\n", id)
			votesForMe = votesForMe + 1
			if (didMajorityVoteForMe()) {
				state = "leader"
			}
		}
		if ((directive == "RequestVote") && (msgTerm > term)) {
			fmt.Printf("\t%s I am not the latest term, cancelling my election.\n", id)
			state = "follower"
			term = msgTerm
			votedFor = id
			conn, _ := net.Dial("tcp", "localhost:"+id)
			fmt.Printf("\tI voted for %s\n", id)
			conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
			conn.Write([]byte("VoteForX|" + strconv.Itoa(term) + "|"+ myPort))
			conn.Close()
		}
		if ((directive == "leaderHeartbeat") && (term == msgTerm)){
			state = "follower"
			term = msgTerm
			votesForMe = 0
			votedFor = ""
			totalAlive = 1
			fmt.Printf("Port: %s|State: %s|Term: %d\n", myPort, state, term)
		}
	}

	if (state == "leader") {
		if ((term < msgTerm)){
			fmt.Printf("\t%s has a term correction. Starting new election.\n", id)
			state = "follower"
			term = msgTerm
			votesForMe = 0
			votedFor = ""
			totalAlive = 1
			timer = time.NewTimer(time.Duration(30 + rand.Intn(60-30+1)) * time.Second)
			resetElectionTimeout()
		}
	}
}

func resetElectionTimeout() (int){
	rand.Seed(time.Now().UnixNano())
	var electionTimeout int = 15 + rand.Intn(30-15+1)
	fmt.Println("\tElection timeout reset to ",electionTimeout, " seconds.")
	timer.Reset(time.Duration(electionTimeout) * time.Second)
	return electionTimeout
}

func followerRoutine() {
	for true {
		if (state == "follower"){
			var electionTimeout = resetElectionTimeout()
			select {
				case <-timer.C:
					fmt.Println("Did not hear heartbeat from leader for ", electionTimeout, ". Starting election.")
					state = "candidate"
			}
		}
	}
}

func candidateRoutine() {
	for true {
		if (state == "candidate") {
			resetElectionTimeout()
			totalAlive = 1 // I am alive
			votesForMe = 1 // I vote for myself
			votedFor = myPort // I vote for myself
			term = term + 1
			fmt.Printf("\tVoting for myself.\n")
			for (state == "candidate") {
				for _, port := range memberList {
					var addr = "localhost:" + port
					conn, err := net.DialTimeout("tcp", addr, 1 * time.Second)
					if err != nil {
						fmt.Printf("\t%s unreachable\n", addr)
					}else {
						totalAlive = totalAlive + 1
						fmt.Printf("\tSent vote request to %s\n", addr)
						conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
						conn.Write([]byte("RequestVote|" + strconv.Itoa(term) + "|"+ myPort))
						conn.Close()
					}
				}
				var votePause time.Duration = time.Duration(rand.Intn(1)+3) * time.Second
				time.Sleep(votePause)
				select {
					case <-timer.C:
						fmt.Println("Election timer expired.")
						if (didMajorityVoteForMe()) {
							state = "leader"
							break
						}else {
							resetElectionTimeout()
							term = term + 1
							state = "candidate"
							votesForMe = 0
							votedFor = ""
							totalAlive = 1
							break
						}
					default:
				}
			}
		}
	}
}

func didMajorityVoteForMe() bool{
	if ((state == "candidate")) {
		var majority int = ((len(memberList) + 1) / 2) + 1
		if (votesForMe == majority) {
			fmt.Printf("%d/%d nodes voted for me. I am the leader.\n", votesForMe, len(memberList) + 1)
			return true
		}
	}else {
		fmt.Printf("%d nodes alive.\n", totalAlive)
	}
	return false
}

func leaderHeartbeat(){
	for true {
		if (state == "leader") {
			timer.Stop()
			var heartbeatPause time.Duration = time.Duration(rand.Intn(1)+3) * time.Second
			time.Sleep(heartbeatPause)
			for _, port := range memberList {
				var addr = "localhost:" + port
				conn, err := net.DialTimeout("tcp", addr, 1 * time.Second)
	
				if err != nil {
					//fmt.Printf("\t%s not responding\n", port)
				}else {
					fmt.Printf("\tSending heartbeat to %s\n", port)
					conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
					conn.Write([]byte("leaderHeartbeat|" + strconv.Itoa(term) + "|"+ myPort))
					conn.Close()
				}
			}
		}
	}
}


func main() {
	myPort, memberList = determineMembership()

	if (myPort == "") {
		fmt.Printf("All ports in memberList.txt are taken, add more ports or close some sessions!\n")
		os.Exit(1)
	}

	listener, _ := net.Listen("tcp", "localhost:"+myPort)
	fmt.Printf("Port: %s|State: %s|Term: %d\n", myPort, state, term)

	go followerRoutine()
	go candidateRoutine()
	go leaderHeartbeat()


	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Connection error: %s\n", err)
		}

		go handleConnection(conn)
	}
}