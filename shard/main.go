package main

import (
	"flag"
	"log"
	"net"
	"strings"
)

func main() {
	addr := flag.String("addr", ":6390", "address to listen on")
	shards := flag.String("shards", "127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382", "shard addresses")
	replicas := flag.Int("replicas", 150, "points per shard on the ring")
	flag.Parse()

	addrs := strings.Split(*shards, ",")
	r := newRouter(addrs, *replicas)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("routing %s across %d shards", *addr, len(addrs))
	r.serve(ln)
}
