package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"miniredis/resp"
)

var (
	errSyntax     = errors.New("syntax error")
	errNotInteger = errors.New("value is not an integer or out of range")
)

type server struct {
	db  *store
	log *aof
}

func newServer() *server {
	return &server{db: newStore()}
}

func (s *server) replay(log *aof) (int, error) {
	if err := log.rewind(); err != nil {
		return 0, err
	}

	r := bufio.NewReader(log.f)
	n := 0

	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			break
		}
		s.run(args)
		n++
	}

	if err := log.end(); err != nil {
		return n, err
	}
	s.log = log
	return n, nil
}

func (s *server) record(args []string) {
	if s.log == nil {
		return
	}
	if err := s.log.write(args); err != nil {
		log.Printf("aof write failed: %v", err)
	}
}

func (s *server) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *server) handle(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			if err != io.EOF {
				io.WriteString(conn, resp.Fail("protocol error"))
			}
			return
		}
		if _, err := io.WriteString(conn, s.run(args)); err != nil {
			return
		}
	}
}

func (s *server) run(args []string) string {
	name := strings.ToUpper(args[0])
	argc := len(args)

	switch name {
	case "PING":
		if argc > 1 {
			return resp.Bulk(args[1])
		}
		return resp.Simple("PONG")

	case "ECHO":
		if argc != 2 {
			return badArgs(name)
		}
		return resp.Bulk(args[1])

	case "SET":
		if argc < 3 {
			return badArgs(name)
		}
		ttl, err := parseTTL(args[3:])
		if err != nil {
			return resp.Fail(err.Error())
		}
		s.db.set(args[1], args[2], ttl)
		s.record(args)
		return resp.Simple("OK")

	case "GET":
		if argc != 2 {
			return badArgs(name)
		}
		value, ok := s.db.get(args[1])
		if !ok {
			return resp.NilBulk()
		}
		return resp.Bulk(value)

	case "DEL":
		if argc < 2 {
			return badArgs(name)
		}
		removed := s.db.del(args[1:]...)
		s.record(args)
		return resp.Integer(removed)

	case "EXISTS":
		if argc != 2 {
			return badArgs(name)
		}
		if _, ok := s.db.get(args[1]); ok {
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "TTL":
		if argc != 2 {
			return badArgs(name)
		}
		expires, ok := s.db.expiry(args[1])
		switch {
		case !ok:
			return resp.Integer(-2)
		case expires.IsZero():
			return resp.Integer(-1)
		default:
			return resp.Integer(int(time.Until(expires).Round(time.Second).Seconds()))
		}

	case "KEYS":
		return resp.Array(s.db.keys())

	case "FLUSHALL":
		s.db.flush()
		s.record(args)
		return resp.Simple("OK")

	case "COMMAND":
		return resp.Array(nil)
	}

	return resp.Fail("unknown command '" + args[0] + "'")
}

func parseTTL(opts []string) (time.Duration, error) {
	var ttl time.Duration

	for i := 0; i < len(opts); i += 2 {
		if i+1 >= len(opts) {
			return 0, errSyntax
		}
		n, err := strconv.Atoi(opts[i+1])
		if err != nil {
			return 0, errNotInteger
		}

		switch strings.ToUpper(opts[i]) {
		case "EX":
			ttl = time.Duration(n) * time.Second
		case "PX":
			ttl = time.Duration(n) * time.Millisecond
		default:
			return 0, errSyntax
		}
	}
	return ttl, nil
}

func badArgs(name string) string {
	return resp.Fail("wrong number of arguments for '" + strings.ToLower(name) + "' command")
}
