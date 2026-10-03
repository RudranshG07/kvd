package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
)

type ring struct {
	replicas int
	points   []uint32
	owner    map[uint32]string
}

func newRing(replicas int) *ring {
	return &ring{replicas: replicas, owner: map[uint32]string{}}
}

func (r *ring) add(node string) {
	for i := 0; i < r.replicas; i++ {
		p := crc32.ChecksumIEEE([]byte(node + "#" + strconv.Itoa(i)))
		if _, taken := r.owner[p]; taken {
			continue
		}
		r.owner[p] = node
		r.points = append(r.points, p)
	}
	sort.Slice(r.points, func(a, b int) bool { return r.points[a] < r.points[b] })
}

func (r *ring) get(key string) string {
	h := crc32.ChecksumIEEE([]byte(key))
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i == len(r.points) {
		i = 0
	}
	return r.owner[r.points[i]]
}

func nodes(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "127.0.0.1:" + strconv.Itoa(6380+i)
	}
	return out
}

func main() {
	const total = 100000

	keys := make([]string, total)
	for i := range keys {
		keys[i] = "key" + strconv.Itoa(i)
	}

	three, four := newRing(150), newRing(150)
	for _, n := range nodes(3) {
		three.add(n)
	}
	for _, n := range nodes(4) {
		four.add(n)
	}

	before := make([]string, total)
	after := make([]string, total)
	counts := map[string]int{}

	moved, modMoved := 0, 0
	for i, k := range keys {
		before[i], after[i] = three.get(k), four.get(k)
		counts[before[i]]++
		if before[i] != after[i] {
			moved++
		}
		h := crc32.ChecksumIEEE([]byte(k))
		if h%3 != h%4 {
			modMoved++
		}
	}

	fmt.Printf("%d keys across 3 shards\n\n", total)
	for _, n := range nodes(3) {
		fmt.Printf("  %-16s %6d\n", n, counts[n])
	}

	fmt.Printf("\nadding a 4th shard\n\n")
	fmt.Printf("  hash ring       %6d moved\n", moved)
	fmt.Printf("  hash %% n        %6d moved\n", modMoved)
}
