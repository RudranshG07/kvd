package raft

import (
	"math/rand"
	"sync"
	"time"
)

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	}
	return "follower"
}

const (
	heartbeat   = 50 * time.Millisecond
	minTimeout  = 300 * time.Millisecond
	jitterRange = 300 * time.Millisecond
)

type Peer interface {
	RequestVote(Vote) (Reply, error)
	AppendEntries(Beat) (Reply, error)
}

type Vote struct {
	Term      int
	Candidate string
}

type Beat struct {
	Term   int
	Leader string
}

type Reply struct {
	Term    int
	Granted bool
}

type Raft struct {
	mu sync.Mutex

	id    string
	peers map[string]Peer

	term     int
	votedFor string
	role     role
	leader   string

	heard   time.Time
	acked   time.Time
	timeout time.Duration

	stop chan struct{}
	done sync.WaitGroup
}

func New(id string, peers map[string]Peer) *Raft {
	r := &Raft{
		id:    id,
		peers: peers,
		heard: time.Now(),
		stop:  make(chan struct{}),
	}
	r.reset()
	return r
}

func (r *Raft) Start() {
	r.done.Add(1)
	go r.loop()
}

func (r *Raft) Stop() {
	close(r.stop)
	r.done.Wait()
}

func (r *Raft) Role() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role.String()
}

func (r *Raft) Term() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.term
}

func (r *Raft) Leader() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leader
}

func (r *Raft) loop() {
	defer r.done.Done()

	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-r.stop:
			return
		case <-tick.C:
			r.mu.Lock()
			switch r.role {
			case leader:
				lost := time.Since(r.acked) > r.timeout
				r.mu.Unlock()
				if lost {
					r.mu.Lock()
					r.stepDown(r.term)
					r.reset()
					r.mu.Unlock()
					continue
				}
				r.beat()
			default:
				expired := time.Since(r.heard) > r.timeout
				r.mu.Unlock()
				if expired {
					r.campaign()
				}
			}
		}
	}
}

func (r *Raft) reset() {
	r.heard = time.Now()
	r.timeout = minTimeout + time.Duration(rand.Int63n(int64(jitterRange)))
}

func (r *Raft) stepDown(term int) {
	r.term = term
	r.votedFor = ""
	r.role = follower
	r.leader = ""
}

func (r *Raft) campaign() {
	r.mu.Lock()
	r.term++
	r.role = candidate
	r.votedFor = r.id
	r.leader = ""
	r.reset()

	term := r.term
	ask := Vote{Term: term, Candidate: r.id}
	peers := r.snapshot()
	r.mu.Unlock()

	votes := make(chan bool, len(peers))
	for _, p := range peers {
		go func(p Peer) {
			reply, err := p.RequestVote(ask)
			if err != nil {
				votes <- false
				return
			}

			r.mu.Lock()
			if reply.Term > r.term {
				r.stepDown(reply.Term)
				r.reset()
			}
			r.mu.Unlock()

			votes <- reply.Granted
		}(p)
	}

	won := 1
	need := (len(peers)+1)/2 + 1

	for range peers {
		if <-votes {
			won++
		}
		if won >= need {
			break
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != candidate || r.term != term || won < need {
		return
	}
	r.role = leader
	r.leader = r.id
	r.acked = time.Now()
}

func (r *Raft) beat() {
	r.mu.Lock()
	term := r.term
	beat := Beat{Term: term, Leader: r.id}
	peers := r.snapshot()
	r.mu.Unlock()

	acks := make(chan bool, len(peers))
	for _, p := range peers {
		go func(p Peer) {
			reply, err := p.AppendEntries(beat)
			if err != nil {
				acks <- false
				return
			}

			r.mu.Lock()
			if reply.Term > r.term {
				r.stepDown(reply.Term)
				r.reset()
			}
			r.mu.Unlock()

			acks <- reply.Granted
		}(p)
	}

	alive := 1
	for range peers {
		select {
		case ok := <-acks:
			if ok {
				alive++
			}
		case <-time.After(heartbeat):
		}
	}

	r.mu.Lock()
	if r.role == leader && r.term == term && alive >= (len(peers)+1)/2+1 {
		r.acked = time.Now()
	}
	r.mu.Unlock()

	time.Sleep(heartbeat)
}

func (r *Raft) snapshot() []Peer {
	out := make([]Peer, 0, len(r.peers))
	for _, p := range r.peers {
		out = append(out, p)
	}
	return out
}

func (r *Raft) OnRequestVote(v Vote) Reply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if v.Term < r.term {
		return Reply{Term: r.term}
	}
	if v.Term > r.term {
		r.stepDown(v.Term)
	}
	if r.votedFor != "" && r.votedFor != v.Candidate {
		return Reply{Term: r.term}
	}

	r.votedFor = v.Candidate
	r.reset()
	return Reply{Term: r.term, Granted: true}
}

func (r *Raft) OnAppendEntries(b Beat) Reply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if b.Term < r.term {
		return Reply{Term: r.term}
	}
	if b.Term > r.term {
		r.stepDown(b.Term)
	}

	r.role = follower
	r.leader = b.Leader
	r.reset()
	return Reply{Term: r.term, Granted: true}
}
