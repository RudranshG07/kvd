package raft

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type link struct {
	mu   sync.Mutex
	to   *Raft
	down bool
}

func (l *link) RequestVote(v Vote) (Reply, error) {
	l.mu.Lock()
	down, to := l.down, l.to
	l.mu.Unlock()

	if down {
		return Reply{}, errors.New("unreachable")
	}
	return to.OnRequestVote(v), nil
}

func (l *link) AppendEntries(b Beat) (Reply, error) {
	l.mu.Lock()
	down, to := l.down, l.to
	l.mu.Unlock()

	if down {
		return Reply{}, errors.New("unreachable")
	}
	return to.OnAppendEntries(b), nil
}

type cluster struct {
	nodes map[string]*Raft
	links map[string][]*link
	dead  map[string]bool
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = string(rune('a' + i))
	}

	c := &cluster{nodes: map[string]*Raft{}, links: map[string][]*link{}, dead: map[string]bool{}}
	for _, id := range ids {
		c.nodes[id] = New(id, map[string]Peer{})
	}

	for _, from := range ids {
		for _, to := range ids {
			if from == to {
				continue
			}
			l := &link{to: c.nodes[to]}
			c.nodes[from].peers[to] = l
			c.links[to] = append(c.links[to], l)
		}
	}

	for _, n := range c.nodes {
		n.Start()
	}
	t.Cleanup(func() {
		for id, n := range c.nodes {
			if !c.dead[id] {
				n.Stop()
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

func (c *cluster) leaders() []string {
	var out []string
	for id, n := range c.nodes {
		if !c.dead[id] && n.Role() == "leader" {
			out = append(out, id)
		}
	}
	return out
}

func (c *cluster) waitForLeader(t *testing.T, within time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if got := c.leaders(); len(got) == 1 {
			time.Sleep(200 * time.Millisecond)
			if got := c.leaders(); len(got) == 1 {
				return got[0]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("no single leader after %v, have %v", within, c.leaders())
	return ""
}

func TestElectsOneLeader(t *testing.T) {
	c := newCluster(t, 5)
	c.waitForLeader(t, 3*time.Second)
}

func TestLeaderIsStable(t *testing.T) {
	c := newCluster(t, 5)
	first := c.waitForLeader(t, 3*time.Second)

	time.Sleep(time.Second)

	if got := c.leaders(); len(got) != 1 || got[0] != first {
		t.Fatalf("leader changed from %s to %v with nothing wrong", first, got)
	}
}

func TestReelectsAfterLeaderDies(t *testing.T) {
	c := newCluster(t, 5)
	first := c.waitForLeader(t, 3*time.Second)

	start := time.Now()
	c.kill(first)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got := c.leaders()
		if len(got) == 1 && got[0] != first {
			t.Logf("re-elected %s in %v", got[0], time.Since(start))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("no new leader after killing %s, have %v", first, c.leaders())
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	c := newCluster(t, 3)
	first := c.waitForLeader(t, 3*time.Second)

	for id := range c.nodes {
		if id != first {
			c.kill(id)
		}
	}

	time.Sleep(1500 * time.Millisecond)

	if c.nodes[first].Role() == "leader" {
		t.Fatal("kept leadership without a majority")
	}
}

func TestTermsOnlyGoUp(t *testing.T) {
	c := newCluster(t, 3)
	c.waitForLeader(t, 3*time.Second)

	seen := map[string]int{}
	for i := 0; i < 50; i++ {
		for id, n := range c.nodes {
			term := n.Term()
			if term < seen[id] {
				t.Fatalf("%s went from term %d back to %d", id, seen[id], term)
			}
			seen[id] = term
		}
		time.Sleep(20 * time.Millisecond)
	}
}
