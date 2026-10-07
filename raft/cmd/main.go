package main

import (
	"flag"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"miniredis/raft"
)

func main() {
	id := flag.String("id", "", "this node's address")
	all := flag.String("peers", "", "every node's address, comma separated")
	write := flag.Bool("write", false, "propose a command every second")
	flag.Parse()

	peers := map[string]raft.Peer{}
	for _, addr := range strings.Split(*all, ",") {
		if addr != "" && addr != *id {
			peers[addr] = raft.Dial(addr)
		}
	}

	applied := 0
	node := raft.New(*id, peers, func(cmd []string) {
		applied++
		log.Printf("applied %d %v", applied, cmd)
	})

	ln, err := net.Listen("tcp", *id)
	if err != nil {
		log.Fatal(err)
	}
	go raft.Listen(node, ln)
	node.Start()

	was, n := "", 0
	for range time.Tick(100 * time.Millisecond) {
		role, term, leader := node.State()
		if role != was {
			log.Printf("%s term %d leader %s", role, term, leader)
			was = role
		}

		n++
		if *write && n%10 == 0 && node.Propose([]string{"SET", "k" + strconv.Itoa(n/10), "v"}) {
			log.Printf("proposed k%d", n/10)
		}
	}
}
