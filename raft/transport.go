package raft

import (
	"net"
	"net/rpc"
	"sync"
)

type Service struct {
	raft *Raft
}

func (s *Service) RequestVote(v Vote, reply *Reply) error {
	*reply = s.raft.OnRequestVote(v)
	return nil
}

func (s *Service) AppendEntries(b Beat, reply *Reply) error {
	*reply = s.raft.OnAppendEntries(b)
	return nil
}

func Listen(r *Raft, ln net.Listener) {
	srv := rpc.NewServer()
	srv.RegisterName("Raft", &Service{raft: r})

	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go srv.ServeConn(conn)
	}
}

type remote struct {
	addr string

	mu     sync.Mutex
	client *rpc.Client
}

func Dial(addr string) Peer {
	return &remote{addr: addr}
}

func (p *remote) call(method string, args, reply any) error {
	p.mu.Lock()
	if p.client == nil {
		client, err := rpc.Dial("tcp", p.addr)
		if err != nil {
			p.mu.Unlock()
			return err
		}
		p.client = client
	}
	client := p.client
	p.mu.Unlock()

	if err := client.Call(method, args, reply); err != nil {
		p.mu.Lock()
		if p.client == client {
			p.client.Close()
			p.client = nil
		}
		p.mu.Unlock()
		return err
	}
	return nil
}

func (p *remote) RequestVote(v Vote) (Reply, error) {
	var reply Reply
	err := p.call("Raft.RequestVote", v, &reply)
	return reply, err
}

func (p *remote) AppendEntries(b Beat) (Reply, error) {
	var reply Reply
	err := p.call("Raft.AppendEntries", b, &reply)
	return reply, err
}
