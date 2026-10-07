package main

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"miniredis/resp"
)

func TestCommands(t *testing.T) {
	s := &server{db: newStore()}

	steps := []struct {
		args []string
		want string
	}{
		{[]string{"SET", "a", "1"}, "+OK\r\n"},
		{[]string{"GET", "a"}, "$1\r\n1\r\n"},
		{[]string{"GET", "b"}, resp.Nil},
		{[]string{"DEL", "a", "b"}, ":1\r\n"},
		{[]string{"GET", "a"}, resp.Nil},
		{[]string{"SET", "a", "1", "EX", "10"}, "+OK\r\n"},
		{[]string{"TTL", "a"}, ":10\r\n"},
		{[]string{"TTL", "b"}, ":-2\r\n"},
	}

	for _, step := range steps {
		if got := s.run(step.args); got != step.want {
			t.Fatalf("%v = %q, want %q", step.args, got, step.want)
		}
	}
}

func TestExpiry(t *testing.T) {
	s := &server{db: newStore()}
	s.run([]string{"SET", "a", "1", "PX", "30"})

	time.Sleep(60 * time.Millisecond)

	if got := s.run([]string{"GET", "a"}); got != resp.Nil {
		t.Fatalf("GET after expiry = %q", got)
	}
}

func TestPipelined(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go (&server{db: newStore()}).serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write([]byte(resp.Array([]string{"SET", "a", "1"}) + resp.Array([]string{"GET", "a"})))

	r := bufio.NewReader(conn)
	first, _ := resp.ReadReply(r)
	second, _ := resp.ReadReply(r)
	if first != "+OK\r\n" || second != "$1\r\n1\r\n" {
		t.Fatalf("got %q then %q", first, second)
	}
}

func TestReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")

	a, _, err := openAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	s := &server{db: newStore(), aof: a}
	s.run([]string{"SET", "a", "1"})
	s.run([]string{"SET", "b", "2"})
	s.run([]string{"GET", "a"})
	s.run([]string{"DEL", "b"})
	a.f.Close()

	_, cmds, err := openAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 3 {
		t.Fatalf("logged %d commands, want 3, reads should not be logged", len(cmds))
	}

	fresh := &server{db: newStore()}
	for _, args := range cmds {
		fresh.run(args)
	}
	if v, _ := fresh.db.get("a"); v != "1" {
		t.Fatalf("a = %q after replay", v)
	}
	if _, ok := fresh.db.get("b"); ok {
		t.Fatal("b came back after a replayed DEL")
	}
}

func TestTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	whole := resp.Array([]string{"SET", "a", "1"})
	torn := "*3\r\n$3\r\nSET\r\n$1\r\nb"

	if err := os.WriteFile(path, []byte(whole+torn), 0o644); err != nil {
		t.Fatal(err)
	}

	_, cmds, err := openAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 1 || cmds[0][1] != "a" {
		t.Fatalf("got %v, want only the complete command", cmds)
	}
}
