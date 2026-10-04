package main

import (
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"miniredis/raft"
)

func main() {
	id := flag.String("id", "", "this node's address")
	peers := flag.String("peers", "", "other node addresses, comma separated")
	flag.Parse()

	if *id == "" {
		log.Fatal("need -id")
	}

	others := map[string]raft.Peer{}
	for _, addr := range strings.Split(*peers, ",") {
		if addr != "" && addr != *id {
			others[addr] = raft.Dial(addr)
		}
	}

	node := raft.New(*id, others)

	ln, err := net.Listen("tcp", *id)
	if err != nil {
		log.Fatal(err)
	}
	go raft.Listen(node, ln)

	node.Start()
	log.Printf("%s up with %d peers", *id, len(others))

	was := ""
	for range time.Tick(100 * time.Millisecond) {
		now := node.Role()
		if now != was {
			log.Printf("%s term %d leader %s", now, node.Term(), node.Leader())
			was = now
		}
	}
}
