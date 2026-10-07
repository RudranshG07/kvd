package main

import (
	"flag"
	"log"
	"net"
)

func main() {
	addr := flag.String("addr", ":6380", "listen address")
	path := flag.String("aof", "appendonly.aof", "append only file")
	fsync := flag.Bool("fsync", true, "fsync every write")
	flag.Parse()

	a, cmds, err := openAOF(*path, *fsync)
	if err != nil {
		log.Fatal(err)
	}
	defer a.f.Close()

	s := &server{db: newStore()}
	for _, args := range cmds {
		s.run(args)
	}
	s.aof = a

	if len(cmds) > 0 {
		log.Printf("replayed %d commands", len(cmds))
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s", *addr)
	s.serve(ln)
}
