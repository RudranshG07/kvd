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
	peers := flag.String("peers", "", "all node addresses, comma separated")
	write := flag.Bool("write", false, "propose entries once elected")
	flag.Parse()

	others := map[string]raft.Peer{}
	for _, addr := range strings.Split(*peers, ",") {
		if addr != "" && addr != *id {
			others[addr] = raft.Dial(addr)
		}
	}

	applied := 0
	node := raft.New(*id, others, func(cmd []string) {
		applied++
		log.Printf("applied %d %v", applied, cmd)
	})

	ln, err := net.Listen("tcp", *id)
	if err != nil {
		log.Fatal(err)
	}
	go raft.Listen(node, ln)

	node.Start()
	log.Printf("up with %d peers", len(others))

	if *write {
		go propose(node)
	}

	was := ""
	for range time.Tick(100 * time.Millisecond) {
		if now := node.Role(); now != was {
			log.Printf("%s term %d leader %s", now, node.Term(), node.Leader())
			was = now
		}
	}
}

func propose(node *raft.Raft) {
	n := 0
	for range time.Tick(time.Second) {
		n++
		if _, ok := node.Propose([]string{"SET", "k" + itoa(n), "v"}); ok {
			log.Printf("proposed k%d", n)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var out []byte
	for n > 0 {
		out = append([]byte{byte('0' + n%10)}, out...)
		n /= 10
	}
	return string(out)
}
