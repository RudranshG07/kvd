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
	br   *bufio.Reader
}

func (n *node) send(args []string) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.conn == nil {
		conn, err := net.Dial("tcp", n.addr)
		if err != nil {
			return "", err
		}
		n.conn, n.br = conn, bufio.NewReader(conn)
	}

	if _, err := io.WriteString(n.conn, resp.Array(args)); err != nil {
		n.drop()
		return "", err
	}

	reply, err := resp.ReadReply(n.br)
	if err != nil {
		n.drop()
		return "", err
	}
	return reply, nil
}

func (n *node) drop() {
	n.conn.Close()
	n.conn, n.br = nil, nil
}

type router struct {
	ring  *ring
	nodes map[string]*node
}

func newRouter(addrs []string, replicas int) *router {
	r := &router{ring: newRing(replicas), nodes: map[string]*node{}}
	for _, addr := range addrs {
		r.ring.add(addr)
		r.nodes[addr] = &node{addr: addr}
	}
	return r
}

func (r *router) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go r.handle(conn)
	}
}

func (r *router) handle(conn net.Conn) {
	defer conn.Close()

	br := bufio.NewReader(conn)
	for {
		args, err := resp.ReadCommand(br)
		if err != nil {
			return
		}
		if _, err := io.WriteString(conn, r.route(args)); err != nil {
			return
		}
	}
}

func (r *router) route(args []string) string {
	switch strings.ToUpper(args[0]) {
	case "PING":
		return resp.Simple("PONG")
	case "COMMAND":
		return resp.Array(nil)
	case "FLUSHALL":
		_, err := r.fanout(args)
		if err != nil {
			return resp.Fail(err.Error())
		}
		return resp.Simple("OK")
	case "KEYS":
		keys, err := r.fanout(args)
		if err != nil {
			return resp.Fail(err.Error())
		}
		sort.Strings(keys)
		return resp.Array(keys)
	}

	if len(args) < 2 {
		return resp.Fail("wrong number of arguments")
	}

	owner := r.ring.get(args[1])
	reply, err := r.nodes[owner].send(args)
	if err != nil {
		return resp.Fail(owner + " unreachable")
	}
	return reply
}

func (r *router) fanout(args []string) ([]string, error) {
	var all []string

	for addr, n := range r.nodes {
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
		all = append(all, items...)
	}
	return all, nil
}
