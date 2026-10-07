package main

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sort"
	"strings"
	"sync"

	"miniredis/resp"
)

type node struct {
	addr string

	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
}

func (n *node) send(args []string) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.conn == nil {
		conn, err := net.Dial("tcp", n.addr)
		if err != nil {
			return "", err
		}
		n.conn, n.r = conn, bufio.NewReader(conn)
	}

	_, err := io.WriteString(n.conn, resp.Array(args))
	reply := ""
	if err == nil {
		reply, err = resp.ReadReply(n.r)
	}
	if err != nil {
		n.conn.Close()
		n.conn = nil
	}
	return reply, err
}

type router struct {
	ring  *ring
	nodes map[string]*node
}

func newRouter(addrs []string, replicas int) *router {
	rt := &router{ring: newRing(replicas), nodes: map[string]*node{}}
	for _, addr := range addrs {
		rt.ring.add(addr)
		rt.nodes[addr] = &node{addr: addr}
	}
	return rt
}

func (rt *router) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go rt.handle(conn)
	}
}

func (rt *router) handle(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			return
		}
		if _, err := io.WriteString(conn, rt.route(args)); err != nil {
			return
		}
	}
}

func (rt *router) route(args []string) string {
	switch strings.ToUpper(args[0]) {
	case "PING":
		return resp.Simple("PONG")
	case "COMMAND":
		return resp.Array(nil)
	case "FLUSHALL":
		if _, err := rt.all(args); err != nil {
			return resp.Fail(err.Error())
		}
		return resp.Simple("OK")
	case "KEYS":
		keys, err := rt.all(args)
		if err != nil {
			return resp.Fail(err.Error())
		}
		sort.Strings(keys)
		return resp.Array(keys)
	}

	if len(args) < 2 {
		return resp.Fail("wrong number of arguments")
	}

	owner := rt.ring.get(args[1])
	reply, err := rt.nodes[owner].send(args)
	if err != nil {
		return resp.Fail(owner + " unreachable")
	}
	return reply
}

func (rt *router) all(args []string) ([]string, error) {
	var out []string

	for addr, n := range rt.nodes {
		reply, err := n.send(args)
		if err != nil {
			return nil, errors.New(addr + " unreachable")
		}
		if reply[0] != '*' {
			continue
		}
		items, err := resp.ParseArray(reply)
		if err != nil {
			return nil, errors.New("bad reply from " + addr)
		}
		out = append(out, items...)
	}
	return out, nil
}
