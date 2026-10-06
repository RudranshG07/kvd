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
	heartbeat  = 50 * time.Millisecond
	minTimeout = 300 * time.Millisecond
	jitter     = 300 * time.Millisecond
)

type Entry struct {
	Term int
	Cmd  []string
}

type Vote struct {
	Term      int
	Candidate string
	LastIdx   int
	LastTerm  int
}

type Append struct {
	Term     int
	Leader   string
	PrevIdx  int
	PrevTerm int
	Entries  []Entry
	Commit   int
}

type Reply struct {
	Term int
	Ok   bool
	Hint int
}

type Peer interface {
	RequestVote(Vote) (Reply, error)
	AppendEntries(Append) (Reply, error)
}

type Raft struct {
	mu sync.Mutex

	id    string
	peers map[string]Peer
	apply func([]string)

	term     int
	votedFor string
	log      []Entry

	commit  int
	applied int

	role   role
	leader string

	next  map[string]int
	match map[string]int

	heard   time.Time
	acked   time.Time
	timeout time.Duration

	stop chan struct{}
	done sync.WaitGroup
}

func New(id string, peers map[string]Peer, apply func([]string)) *Raft {
	if apply == nil {
		apply = func([]string) {}
	}

	r := &Raft{
		id:    id,
		peers: peers,
		apply: apply,
		log:   []Entry{{}},
		next:  map[string]int{},
		match: map[string]int{},
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

func (r *Raft) Committed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commit
}

func (r *Raft) Propose(cmd []string) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != leader {
		return 0, false
	}
	r.log = append(r.log, Entry{Term: r.term, Cmd: cmd})
	return len(r.log) - 1, true
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
			role, lost := r.role, time.Since(r.acked) > r.timeout
			expired := time.Since(r.heard) > r.timeout
			r.mu.Unlock()

			switch {
			case role == leader && lost:
				r.mu.Lock()
				r.stepDown(r.term)
				r.mu.Unlock()
			case role == leader:
				r.replicate()
			case expired:
				r.campaign()
			}
			r.flush()
		}
	}
}

func (r *Raft) reset() {
	r.heard = time.Now()
	r.timeout = minTimeout + time.Duration(rand.Int63n(int64(jitter)))
}

func (r *Raft) stepDown(term int) {
	if term > r.term {
		r.term = term
		r.votedFor = ""
	}
	r.role = follower
	r.leader = ""
	r.reset()
}

func (r *Raft) last() (int, int) {
	i := len(r.log) - 1
	return i, r.log[i].Term
}

func (r *Raft) campaign() {
	r.mu.Lock()
	r.term++
	r.role = candidate
	r.votedFor = r.id
	r.leader = ""
	r.reset()

	term := r.term
	idx, lastTerm := r.last()
	ask := Vote{Term: term, Candidate: r.id, LastIdx: idx, LastTerm: lastTerm}
	peers := r.others()
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
			}
			r.mu.Unlock()
			votes <- reply.Ok
		}(p)
	}

	won, need := 1, (len(peers)+1)/2+1
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

	idx, _ = r.last()
	for id := range r.peers {
		r.next[id] = idx + 1
		r.match[id] = 0
	}
}

func (r *Raft) replicate() {
	r.mu.Lock()
	term := r.term
	ids := make([]string, 0, len(r.peers))
	for id := range r.peers {
		ids = append(ids, id)
	}
	r.mu.Unlock()

	acks := make(chan bool, len(ids))
	for _, id := range ids {
		go func(id string) {
			acks <- r.sendTo(id, term)
		}(id)
	}

	alive := 1
	for range ids {
		select {
		case ok := <-acks:
			if ok {
				alive++
			}
		case <-time.After(heartbeat):
		}
	}

	r.mu.Lock()
	if r.role == leader && r.term == term && alive >= (len(ids)+1)/2+1 {
		r.acked = time.Now()
		r.advance()
	}
	r.mu.Unlock()

	time.Sleep(heartbeat)
}

func (r *Raft) sendTo(id string, term int) bool {
	r.mu.Lock()
	if r.role != leader || r.term != term {
		r.mu.Unlock()
		return false
	}

	next := r.next[id]
	if next < 1 {
		next = 1
	}

	send := Append{
		Term:     term,
		Leader:   r.id,
		PrevIdx:  next - 1,
		PrevTerm: r.log[next-1].Term,
		Entries:  append([]Entry(nil), r.log[next:]...),
		Commit:   r.commit,
	}
	peer := r.peers[id]
	r.mu.Unlock()

	reply, err := peer.AppendEntries(send)
	if err != nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.term {
		r.stepDown(reply.Term)
		return false
	}
	if r.role != leader || r.term != term {
		return false
	}

	if reply.Ok {
		r.match[id] = send.PrevIdx + len(send.Entries)
		r.next[id] = r.match[id] + 1
		return true
	}

	r.next[id] = max(1, reply.Hint)
	return true
}

func (r *Raft) advance() {
	last, _ := r.last()

	for n := last; n > r.commit; n-- {
		if r.log[n].Term != r.term {
			continue
		}
		copies := 1
		for id := range r.peers {
			if r.match[id] >= n {
				copies++
			}
		}
		if copies >= (len(r.peers)+1)/2+1 {
			r.commit = n
			return
		}
	}
}

func (r *Raft) flush() {
	r.mu.Lock()
	var ready [][]string
	for r.applied < r.commit {
		r.applied++
		ready = append(ready, r.log[r.applied].Cmd)
	}
	r.mu.Unlock()

	for _, cmd := range ready {
		r.apply(cmd)
	}
}

func (r *Raft) others() []Peer {
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

	idx, term := r.last()
	if v.LastTerm < term || (v.LastTerm == term && v.LastIdx < idx) {
		return Reply{Term: r.term}
	}

	r.votedFor = v.Candidate
	r.reset()
	return Reply{Term: r.term, Ok: true}
}

func (r *Raft) OnAppendEntries(a Append) Reply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if a.Term < r.term {
		return Reply{Term: r.term}
	}
	if a.Term > r.term {
		r.stepDown(a.Term)
	}

	r.role = follower
	r.leader = a.Leader
	r.reset()

	if a.PrevIdx >= len(r.log) {
		return Reply{Term: r.term, Hint: len(r.log)}
	}
	if r.log[a.PrevIdx].Term != a.PrevTerm {
		bad := r.log[a.PrevIdx].Term
		hint := a.PrevIdx
		for hint > 1 && r.log[hint-1].Term == bad {
			hint--
		}
		return Reply{Term: r.term, Hint: hint}
	}

	for i, e := range a.Entries {
		at := a.PrevIdx + 1 + i
		if at < len(r.log) && r.log[at].Term == e.Term {
			continue
		}
		r.log = append(r.log[:at], a.Entries[i:]...)
		break
	}

	if a.Commit > r.commit {
		last, _ := r.last()
		r.commit = min(a.Commit, last)
	}
	return Reply{Term: r.term, Ok: true}
}
