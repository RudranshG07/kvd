package raft

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type link struct {
	mu   sync.Mutex
	to   *Raft
	down bool
}

func (l *link) alive() (*Raft, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.to, !l.down
}

func (l *link) RequestVote(v Vote) (Reply, error) {
	to, ok := l.alive()
	if !ok {
		return Reply{}, errors.New("unreachable")
	}
	return to.OnRequestVote(v), nil
}

func (l *link) AppendEntries(a Append) (Reply, error) {
	to, ok := l.alive()
	if !ok {
		return Reply{}, errors.New("unreachable")
	}
	return to.OnAppendEntries(a), nil
}

type cluster struct {
	t     *testing.T
	nodes map[string]*Raft
	links map[string][]*link
	dead  map[string]bool

	mu      sync.Mutex
	applied map[string][][]string
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()

	c := &cluster{
		t:       t,
		nodes:   map[string]*Raft{},
		links:   map[string][]*link{},
		dead:    map[string]bool{},
		applied: map[string][][]string{},
	}

	ids := make([]string, n)
	for i := range ids {
		ids[i] = string(rune('a' + i))
	}

	for _, id := range ids {
		id := id
		c.nodes[id] = New(id, map[string]Peer{}, func(cmd []string) {
			c.mu.Lock()
			c.applied[id] = append(c.applied[id], cmd)
			c.mu.Unlock()
		})
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

func (c *cluster) waitLeader(within time.Duration) string {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if got := c.leaders(); len(got) == 1 {
			time.Sleep(150 * time.Millisecond)
			if again := c.leaders(); len(again) == 1 && again[0] == got[0] {
				return got[0]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	c.t.Fatalf("no single leader after %v, have %v", within, c.leaders())
	return ""
}

func (c *cluster) log(id string) [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]string(nil), c.applied[id]...)
}

func (c *cluster) waitApplied(id string, n int, within time.Duration) [][]string {
	c.t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if got := c.log(id); len(got) >= n {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}

	c.t.Fatalf("%s applied %d of %d within %v", id, len(c.log(id)), n, within)
	return nil
}

func TestElectsOneLeader(t *testing.T) {
	newCluster(t, 5).waitLeader(3 * time.Second)
}

func TestLeaderIsStable(t *testing.T) {
	c := newCluster(t, 5)
	first := c.waitLeader(3 * time.Second)

	time.Sleep(time.Second)

	if got := c.leaders(); len(got) != 1 || got[0] != first {
		t.Fatalf("leader changed from %s to %v with nothing wrong", first, got)
	}
}

func TestReelectsAfterLeaderDies(t *testing.T) {
	c := newCluster(t, 5)
	first := c.waitLeader(3 * time.Second)

	start := time.Now()
	c.kill(first)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := c.leaders(); len(got) == 1 && got[0] != first {
			t.Logf("re-elected %s in %v", got[0], time.Since(start))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("no new leader after killing %s, have %v", first, c.leaders())
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	c := newCluster(t, 3)
	first := c.waitLeader(3 * time.Second)

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

func TestReplicatesToEveryone(t *testing.T) {
	c := newCluster(t, 5)
	lead := c.waitLeader(3 * time.Second)

	for i := 0; i < 10; i++ {
		if _, ok := c.nodes[lead].Propose([]string{"SET", fmt.Sprint(i), "v"}); !ok {
			t.Fatalf("%s refused entry %d", lead, i)
		}
	}

	for id := range c.nodes {
		got := c.waitApplied(id, 10, 3*time.Second)
		for i := 0; i < 10; i++ {
			if got[i][1] != fmt.Sprint(i) {
				t.Fatalf("%s applied %v at %d", id, got[i], i)
			}
		}
	}
}

func TestFollowersRefuseWrites(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.waitLeader(3 * time.Second)

	for id, n := range c.nodes {
		if id == lead {
			continue
		}
		if _, ok := n.Propose([]string{"SET", "a", "1"}); ok {
			t.Fatalf("follower %s accepted a write", id)
		}
	}
}

func TestEveryoneAppliesTheSameOrder(t *testing.T) {
	c := newCluster(t, 5)
	lead := c.waitLeader(3 * time.Second)

	for i := 0; i < 20; i++ {
		c.nodes[lead].Propose([]string{"SET", fmt.Sprint(i), "v"})
	}

	want := c.waitApplied(lead, 20, 3*time.Second)
	for id := range c.nodes {
		got := c.waitApplied(id, 20, 3*time.Second)
		for i := range want {
			if got[i][1] != want[i][1] {
				t.Fatalf("%s applied %v at %d, leader had %v", id, got[i], i, want[i])
			}
		}
	}
}

func TestWritesSurviveALeaderDying(t *testing.T) {
	c := newCluster(t, 5)
	first := c.waitLeader(3 * time.Second)

	for i := 0; i < 5; i++ {
		c.nodes[first].Propose([]string{"SET", fmt.Sprint(i), "v"})
	}
	for id := range c.nodes {
		c.waitApplied(id, 5, 3*time.Second)
	}

	c.kill(first)
	second := c.waitLeader(5 * time.Second)

	for i := 5; i < 10; i++ {
		if _, ok := c.nodes[second].Propose([]string{"SET", fmt.Sprint(i), "v"}); !ok {
			t.Fatalf("%s refused entry %d", second, i)
		}
	}

	for id := range c.nodes {
		if c.dead[id] {
			continue
		}
		got := c.waitApplied(id, 10, 5*time.Second)
		for i := 0; i < 10; i++ {
			if got[i][1] != fmt.Sprint(i) {
				t.Fatalf("%s applied %v at %d", id, got[i], i)
			}
		}
	}
}

func TestTermsOnlyGoUp(t *testing.T) {
	c := newCluster(t, 3)
	c.waitLeader(3 * time.Second)

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
