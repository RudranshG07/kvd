package main

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"

	"miniredis/resp"
)

func fakeShard(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		data := map[string]string{}
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				br := bufio.NewReader(conn)
				for {
					args, err := resp.ReadCommand(br)
					if err != nil {
						return
					}
					switch strings.ToUpper(args[0]) {
					case "SET":
						data[args[1]] = args[2]
						io.WriteString(conn, resp.Simple("OK"))
					case "GET":
						v, ok := data[args[1]]
						if !ok {
							io.WriteString(conn, resp.NilBulk())
							continue
						}
						io.WriteString(conn, resp.Bulk(v))
					case "KEYS":
						keys := make([]string, 0, len(data))
						for k := range data {
							keys = append(keys, k)
						}
						io.WriteString(conn, resp.Array(keys))
					default:
						io.WriteString(conn, resp.Fail("unknown"))
					}
				}
			}()
		}
	}()

	return ln.Addr().String()
}

func dial(t *testing.T, shards int) (net.Conn, *bufio.Reader, *router) {
	t.Helper()

	addrs := make([]string, shards)
	for i := range addrs {
		addrs[i] = fakeShard(t)
	}
	r := newRouter(addrs, 150)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go r.serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn, bufio.NewReader(conn), r
}

func send(t *testing.T, conn net.Conn, br *bufio.Reader, args ...string) string {
	t.Helper()

	if _, err := conn.Write([]byte(resp.Array(args))); err != nil {
		t.Fatal(err)
	}
	reply, err := resp.ReadReply(br)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

func TestRoundTrip(t *testing.T) {
	conn, br, _ := dial(t, 3)

	for i := 0; i < 200; i++ {
		key := "key" + strconv.Itoa(i)
		if got := send(t, conn, br, "SET", key, "v"+strconv.Itoa(i)); got != resp.Simple("OK") {
			t.Fatalf("SET %s = %q", key, got)
		}
	}
	for i := 0; i < 200; i++ {
		key := "key" + strconv.Itoa(i)
		want := resp.Bulk("v" + strconv.Itoa(i))
		if got := send(t, conn, br, "GET", key); got != want {
			t.Fatalf("GET %s = %q, want %q", key, got, want)
		}
	}
}

func TestKeysAreSpread(t *testing.T) {
	_, _, r := dial(t, 3)

	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		counts[r.ring.get("key"+strconv.Itoa(i))]++
	}

	if len(counts) != 3 {
		t.Fatalf("keys landed on %d shards, want 3", len(counts))
	}
	for addr, n := range counts {
		if n < 2000 || n > 5000 {
			t.Errorf("%s holds %d of 10000", addr, n)
		}
	}
}

func TestSameKeySameShard(t *testing.T) {
	_, _, r := dial(t, 4)

	for i := 0; i < 1000; i++ {
		key := "key" + strconv.Itoa(i)
		if r.ring.get(key) != r.ring.get(key) {
			t.Fatalf("%s routed two different ways", key)
		}
	}
}

func TestKeysMerges(t *testing.T) {
	conn, br, _ := dial(t, 3)

	for i := 0; i < 50; i++ {
		send(t, conn, br, "SET", "key"+strconv.Itoa(i), "v")
	}

	keys, err := resp.ParseArray(send(t, conn, br, "KEYS", "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 50 {
		t.Fatalf("KEYS returned %d, want 50", len(keys))
	}
}

func TestDeadShard(t *testing.T) {
	r := newRouter([]string{"127.0.0.1:1"}, 150)
	if got := r.route([]string{"GET", "a"}); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("dead shard = %q", got)
	}
}
