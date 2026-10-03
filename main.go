package main

import (
	"flag"
	"log"
	"net"
)

func main() {
	addr := flag.String("addr", ":6380", "address to listen on")
	path := flag.String("aof", "appendonly.aof", "append only file")
	fsync := flag.Bool("fsync", true, "fsync after every write")
	flag.Parse()

	f, err := openAOF(*path, *fsync)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	server := newServer()
	replayed, err := server.replay(f)
	if err != nil {
		log.Fatal(err)
	}
	if replayed > 0 {
		log.Printf("replayed %d commands from %s", replayed, *path)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("listening on %s", *addr)
	server.serve(ln)
}
