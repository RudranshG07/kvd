package main

import (
	"bufio"
	"hash/crc32"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"miniredis/resp"
)

func fakeShard(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	var mu sync.Mutex
	data := map[string]string{}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				r := bufio.NewReader(conn)
				for {
					args, err := resp.ReadCommand(r)
					if err != nil {
						return
					}
					mu.Lock()
					switch strings.ToUpper(args[0]) {
					case "SET":
						data[args[1]] = args[2]
						io.WriteString(conn, resp.Simple("OK"))
					case "GET":
						io.WriteString(conn, resp.Bulk(data[args[1]]))
					case "KEYS":
						var keys []string
						for k := range data {
							keys = append(keys, k)
						}
						io.WriteString(conn, resp.Array(keys))
					}
					mu.Unlock()
				}
			}()
		}
	}()

	return ln.Addr().String()
}

func cluster(t *testing.T, n int) *router {
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = fakeShard(t)
	}
	return newRouter(addrs, 150)
}

func TestRouting(t *testing.T) {
	rt := cluster(t, 3)
	used := map[string]bool{}

	for i := 0; i < 300; i++ {
		key := "k" + strconv.Itoa(i)
		rt.route([]string{"SET", key, key})
		used[rt.ring.get(key)] = true

		if got := rt.route([]string{"GET", key}); got != resp.Bulk(key) {
			t.Fatalf("GET %s = %q", key, got)
		}
	}
	if len(used) != 3 {
		t.Fatalf("keys landed on %d of 3 shards", len(used))
	}
}

func TestKeysWithAnEmptyShard(t *testing.T) {
	rt := cluster(t, 3)
	rt.route([]string{"SET", "only", "1"})

	keys, err := resp.ParseArray(rt.route([]string{"KEYS", "*"}))
	if err != nil || len(keys) != 1 {
		t.Fatalf("KEYS = %v, %v", keys, err)
	}
}

func TestDeadShard(t *testing.T) {
	rt := newRouter([]string{"127.0.0.1:1"}, 150)
	if got := rt.route([]string{"GET", "a"}); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("got %q from a dead shard", got)
	}
}

func TestRebalance(t *testing.T) {
	three, four := newRing(150), newRing(150)
	for i := 0; i < 4; i++ {
		addr := "node" + strconv.Itoa(i)
		if i < 3 {
			three.add(addr)
		}
		four.add(addr)
	}

	ringMoved, modMoved := 0, 0
	for i := 0; i < 100000; i++ {
		key := "k" + strconv.Itoa(i)
		if three.get(key) != four.get(key) {
			ringMoved++
		}
		h := crc32.ChecksumIEEE([]byte(key))
		if h%3 != h%4 {
			modMoved++
		}
	}

	t.Logf("adding a 4th node: ring moved %d, hash %% n moved %d", ringMoved, modMoved)
	if ringMoved*2 > modMoved {
		t.Fatalf("ring moved %d, hash %% n moved %d", ringMoved, modMoved)
	}
}
