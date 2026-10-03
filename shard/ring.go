package main

import (
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
	if len(r.points) == 0 {
		return ""
	}

	h := crc32.ChecksumIEEE([]byte(key))
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i == len(r.points) {
		i = 0
	}
	return r.owner[r.points[i]]
}
