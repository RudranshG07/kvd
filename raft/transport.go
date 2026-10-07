package raft

import (
	"net"
	"net/rpc"
	"sync"
)

type service struct{ r *Raft }

func (s *service) RequestVote(v Vote, reply *Reply) error {
	*reply = s.r.OnRequestVote(v)
	return nil
}

func (s *service) AppendEntries(a Append, reply *Reply) error {
	*reply = s.r.OnAppendEntries(a)
	return nil
}

func Listen(r *Raft, ln net.Listener) {
	srv := rpc.NewServer()
	srv.RegisterName("Raft", &service{r})
	srv.Accept(ln)
}

type remote struct {
	addr   string
	mu     sync.Mutex
	client *rpc.Client
}

func Dial(addr string) Peer {
	return &remote{addr: addr}
}

func (p *remote) call(method string, args, reply any) error {
	p.mu.Lock()
	if p.client == nil {
		c, err := rpc.Dial("tcp", p.addr)
		if err != nil {
			p.mu.Unlock()
			return err
		}
		p.client = c
	}
	c := p.client
	p.mu.Unlock()

	err := c.Call(method, args, reply)
	if err != nil {
		p.mu.Lock()
		if p.client == c {
			c.Close()
			p.client = nil
		}
		p.mu.Unlock()
	}
	return err
}

func (p *remote) RequestVote(v Vote) (Reply, error) {
	var reply Reply
	err := p.call("Raft.RequestVote", v, &reply)
	return reply, err
}

func (p *remote) AppendEntries(a Append) (Reply, error) {
	var reply Reply
	err := p.call("Raft.AppendEntries", a, &reply)
	return reply, err
}
