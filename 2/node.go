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

var timer *time.Timer = time.NewTimer(time.Duration(15 + rand.Intn(30-15+1)) * time.Second)

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

// Resets the state of a node
func setState(newTerm int, newState string) {
	fmt.Printf("\nNAME: %s\tSTATE: %s\n", myPort, newState)
	fmt.Printf("Term\tAction\n")
	state = newState
	term = newTerm
	votesForMe = 0
	votedFor = ""
}

func resetElectionTimeout() (int){
	rand.Seed(time.Now().UnixNano())
	var electionTimeout int = 15 + rand.Intn(30-15+1)
	fmt.Printf("%d\tElection timeout reset to %s seconds.\t\n", term, strconv.Itoa(electionTimeout))
	timer.Reset(time.Duration(electionTimeout) * time.Second)
	return electionTimeout
}

func followerRoutine() {
	for true {
		if (state == "follower"){
			var electionTimeout = resetElectionTimeout()
			select {
				case <-timer.C:
					fmt.Printf("%d\tDid not hear heartbeat from leader for %s seconds. Becoming a candidate.\n", term, strconv.Itoa(electionTimeout))
					setState(term, "candidate")
			}
		}
	}
}

func candidateRoutine() {
	for true {
		if (state == "candidate") {
			votesForMe = 1 // I vote for myself
			votedFor = myPort // I vote for myself
			term = term + 1   // New election new term
			fmt.Printf("%d\tStarting new election. Voting for myself.\n", term)
			resetElectionTimeout()

			// While we are a candidate keep sending vote requests until the timer expires or we become the leader
			for (state == "candidate") {
				for _, port := range memberList {
					if (didMajorityVoteForMe()) {
						setState(term, "leader")
						break
					}
					var addr = "localhost:" + port
					conn, err := net.DialTimeout("tcp", addr, 1 * time.Second)
					if err != nil {
						//fmt.Printf("\t\t%s unreachable\n", addr)
					}else {
						fmt.Printf("%d\tTelling %s to vote for me.\n", term, addr)
						conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
						conn.Write([]byte("VoteForMe|" + strconv.Itoa(term) + "|"+ myPort))
						conn.Close()
					}
				}
				var votePause time.Duration = time.Duration(rand.Intn(1)+3) * time.Second
				time.Sleep(votePause)
				select {
					case <-timer.C:
						fmt.Printf("%d\tElection Timer expired.\n", term)
						if (didMajorityVoteForMe()) { // Majority voted for me at the end of the term
							setState(term, "leader")
							break
						}else { // At this point, no leader has been elected, start a new election
							fmt.Printf("%d\tNo leader was elected. Starting a new election.\n", term)
							resetElectionTimeout()
							setState(term + 1, "candidate")
							break
						}
					default:
				}
			}
		}
	}
}

func leaderHeartbeat(){
	for true {
		if (state == "leader") {
			timer.Stop()
			for _, port := range memberList {
				var addr = "localhost:" + port
				conn, err := net.DialTimeout("tcp", addr, 1 * time.Second)
	
				if err != nil {
					//fmt.Printf("\t%s not responding\n", port)
				}else {
					fmt.Printf("%d\tSending heartbeat to %s\n", term, port)
					conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
					conn.Write([]byte("leaderHeartbeat|" + strconv.Itoa(term) + "|"+ myPort))
					conn.Close()
				}
			}
			var heartbeatPause time.Duration = time.Duration(rand.Intn(1)+3) * time.Second
			time.Sleep(heartbeatPause)
		}
	}
}

func didMajorityVoteForMe() bool{
	if ((state == "candidate")) {
		var majority int = ((len(memberList) + 1) / 2) + 1
		if (votesForMe == majority) {
			fmt.Printf("%d\t%d/%d nodes voted for me. I am now the leader.\n", term, votesForMe, len(memberList) + 1)
			return true
		}
	}
	return false
}

func handleMessage(message string, conn net.Conn) {
	msg := strings.Split(message, "|")
	var directive string = msg[0]
	var msgTerm int
	msgTerm, _ = strconv.Atoi(msg[1])
	var id string = msg[2]
	
	fmt.Printf("> %s: %s, TERM: %d\n", id, directive, msgTerm)
	// If our term is less than the message term, then reset our state to a follower because we are behind
	if (term < msgTerm) {
		fmt.Printf("\tTerm correction. %s has a higher term. \n", id)
		setState(msgTerm, "follower")
	}

	if (term > msgTerm) { // If our term is newer than the message sender's, tell them they are behind.
		fmt.Printf("%d\t%s is behind terms. Telling them they are behind.\n", term, id)
		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		conn.Write([]byte("YouAreBehind|" + strconv.Itoa(term) + "|"+ myPort))
		conn.Close()
	}

	if (state == "follower") {
		if (directive == "leaderHeartbeat") { // If we hear the leader's heartbeat
			fmt.Printf("%d\tReceived Leader %s heartbeat. Current network term is %d.\n", term, id, msgTerm)
			if (term == msgTerm) {
				resetElectionTimeout() // Otherwise, reset our election timeout
			}
		}

		if ((directive == "VoteForMe") && (votedFor == "")) { // If someone tells us to vote for them and we haven't voted yet then vote for them
			resetElectionTimeout()
			term = msgTerm
			votedFor = id
			conn, _ := net.Dial("tcp", "localhost:"+id)
			fmt.Printf("%d\tReceived vote request. I voted for %s\n", term, id)
			conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
			conn.Write([]byte("IVoteForYou|" + strconv.Itoa(term) + "|"+ myPort))
			conn.Close()
		}
	}

	if (state == "candidate") {
		if ((directive == "IVoteForYou") && (msgTerm == term)) {
			fmt.Printf("%d\t%s Voted for me.\n", term, id)
			votesForMe = votesForMe + 1
			if (didMajorityVoteForMe()) {
				setState(msgTerm, "leader")
			}
		}
		if ((directive == "leaderHeartbeat")){
			setState(msgTerm, "follower")
			resetElectionTimeout()
		}
	}
}

func main() {
	myPort, memberList = determineMembership()

	if (myPort == "") {
		fmt.Printf("All ports in memberList.txt are in use, add more ports or close some sessions!\n")
		os.Exit(1)
	}

	listener, _ := net.Listen("tcp", "localhost:"+myPort)
	setState(term, "follower")
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
// Kin-Ho Lam | 3/16/19 | CS 419 | Raft leader election with Go