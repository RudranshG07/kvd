package raft

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

type link struct {
	mu   sync.Mutex
	to   *Raft
	down bool
}

func (l *link) target() (*Raft, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.down {
		return nil, errors.New("down")
	}
	return l.to, nil
}

func (l *link) RequestVote(v Vote) (Reply, error) {
	to, err := l.target()
	if err != nil {
		return Reply{}, err
	}
	return to.OnRequestVote(v), nil
}

func (l *link) AppendEntries(a Append) (Reply, error) {
	to, err := l.target()
	if err != nil {
		return Reply{}, err
	}
	return to.OnAppendEntries(a), nil
}

type cluster struct {
	t     *testing.T
	nodes map[string]*Raft
	links map[string][]*link
	dead  map[string]bool

	mu      sync.Mutex
	applied map[string][]string
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{
		t:       t,
		nodes:   map[string]*Raft{},
		links:   map[string][]*link{},
		dead:    map[string]bool{},
		applied: map[string][]string{},
	}

	for i := 0; i < n; i++ {
		id := strconv.Itoa(i)
		c.nodes[id] = New(id, map[string]Peer{}, func(cmd []string) {
			c.mu.Lock()
			c.applied[id] = append(c.applied[id], cmd[1])
			c.mu.Unlock()
		})
	}
	for from, node := range c.nodes {
		for to := range c.nodes {
			if from != to {
				l := &link{to: c.nodes[to]}
				node.peers[to] = l
				c.links[to] = append(c.links[to], l)
			}
		}
	}

	for _, node := range c.nodes {
		node.Start()
	}
	t.Cleanup(func() {
		for id, node := range c.nodes {
			if !c.dead[id] {
				node.Stop()
			}
		}
	})
	return c
}

func (c *cluster) kill(id string) {
	for _, l := range c.links[id] {
		l.mu.Lock()
		l.down = true
		l.mu.Unlock()
	}
	c.nodes[id].Stop()
	c.dead[id] = true
}

func (c *cluster) leader() string {
	c.t.Helper()

	for start := time.Now(); time.Since(start) < 5*time.Second; time.Sleep(20 * time.Millisecond) {
		var found []string
		for id, node := range c.nodes {
			if role, _, _ := node.State(); !c.dead[id] && role == leader {
				found = append(found, id)
			}
		}
		if len(found) == 1 {
			return found[0]
		}
	}
	c.t.Fatal("no single leader")
	return ""
}

func (c *cluster) waitApplied(id string, n int) []string {
	c.t.Helper()

	for start := time.Now(); time.Since(start) < 5*time.Second; time.Sleep(20 * time.Millisecond) {
		c.mu.Lock()
		got := append([]string(nil), c.applied[id]...)
		c.mu.Unlock()
		if len(got) >= n {
			return got
		}
	}
	c.t.Fatalf("node %s never applied %d entries", id, n)
	return nil
}

func TestReelection(t *testing.T) {
	c := newCluster(t, 5)
	first := c.leader()

	start := time.Now()
	c.kill(first)
	second := c.leader()

	if second == first {
		t.Fatal("dead node still leader")
	}
	t.Logf("new leader %s after %v", second, time.Since(start))
}

func TestNoLeaderWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader()

	for id := range c.nodes {
		if id != lead {
			c.kill(id)
		}
	}
	time.Sleep(1500 * time.Millisecond)

	if role, _, _ := c.nodes[lead].State(); role == leader {
		t.Fatal("kept leading without a majority")
	}
}

func TestSameOrderEverywhere(t *testing.T) {
	c := newCluster(t, 5)
	lead := c.leader()

	for i := 0; i < 20; i++ {
		c.nodes[lead].Propose([]string{"SET", strconv.Itoa(i)})
	}

	for id := range c.nodes {
		got := c.waitApplied(id, 20)
		for i := range 20 {
			if got[i] != strconv.Itoa(i) {
				t.Fatalf("node %s applied %v", id, got)
			}
		}
	}
}

func TestSurvivesLeaderDying(t *testing.T) {
	c := newCluster(t, 5)
	first := c.leader()

	for i := 0; i < 5; i++ {
		c.nodes[first].Propose([]string{"SET", strconv.Itoa(i)})
	}
	for id := range c.nodes {
		c.waitApplied(id, 5)
	}

	c.kill(first)
	second := c.leader()

	for i := 5; i < 10; i++ {
		if !c.nodes[second].Propose([]string{"SET", strconv.Itoa(i)}) {
			t.Fatal("new leader refused a write")
		}
	}

	for id := range c.nodes {
		if c.dead[id] {
			continue
		}
		got := c.waitApplied(id, 10)
		for i := range 10 {
			if got[i] != strconv.Itoa(i) {
				t.Fatalf("node %s applied %v", id, got)
			}
		}
	}
}
