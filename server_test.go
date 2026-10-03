package main

import (
	"bufio"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func dial(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go newServer().serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn, bufio.NewReader(conn)
}

func send(t *testing.T, conn net.Conn, r *bufio.Reader, parts ...string) string {
	t.Helper()

	cmd := "*" + itoa(len(parts)) + "\r\n"
	for _, p := range parts {
		cmd += "$" + itoa(len(p)) + "\r\n" + p + "\r\n"
	}
	if _, err := conn.Write([]byte(cmd)); err != nil {
		t.Fatal(err)
	}

	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}

	if strings.HasPrefix(line, "$") && !strings.HasPrefix(line, "$-1") {
		body, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimRight(body, "\r\n")
	}
	return strings.TrimRight(line, "\r\n")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var out []byte
	for n > 0 {
		out = append([]byte{byte('0' + n%10)}, out...)
		n /= 10
	}
	return string(out)
}

func TestPing(t *testing.T) {
	conn, r := dial(t)
	if got := send(t, conn, r, "PING"); got != "+PONG" {
		t.Fatalf("PING = %q", got)
	}
}

func TestSetGet(t *testing.T) {
	conn, r := dial(t)

	if got := send(t, conn, r, "SET", "a", "hello"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := send(t, conn, r, "GET", "a"); got != "hello" {
		t.Fatalf("GET = %q", got)
	}
	if got := send(t, conn, r, "GET", "missing"); got != "$-1" {
		t.Fatalf("GET missing = %q", got)
	}
}

func TestDel(t *testing.T) {
	conn, r := dial(t)

	send(t, conn, r, "SET", "a", "1")
	if got := send(t, conn, r, "DEL", "a"); got != ":1" {
		t.Fatalf("DEL = %q", got)
	}
	if got := send(t, conn, r, "DEL", "a"); got != ":0" {
		t.Fatalf("DEL again = %q", got)
	}
}

func TestExpiry(t *testing.T) {
	conn, r := dial(t)

	send(t, conn, r, "SET", "a", "1", "PX", "40")
	if got := send(t, conn, r, "GET", "a"); got != "1" {
		t.Fatalf("GET before expiry = %q", got)
	}

	time.Sleep(80 * time.Millisecond)

	if got := send(t, conn, r, "GET", "a"); got != "$-1" {
		t.Fatalf("GET after expiry = %q", got)
	}
	if got := send(t, conn, r, "TTL", "a"); got != ":-2" {
		t.Fatalf("TTL after expiry = %q", got)
	}
}

func TestTTLWithoutExpiry(t *testing.T) {
	conn, r := dial(t)

	send(t, conn, r, "SET", "a", "1")
	if got := send(t, conn, r, "TTL", "a"); got != ":-1" {
		t.Fatalf("TTL = %q", got)
	}
}

func TestEcho(t *testing.T) {
	conn, r := dial(t)
	if got := send(t, conn, r, "ECHO", "hi"); got != "hi" {
		t.Fatalf("ECHO = %q", got)
	}
}

func TestUnknownCommand(t *testing.T) {
	conn, r := dial(t)
	if got := send(t, conn, r, "NOPE"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("unknown = %q", got)
	}
}

func TestPipelined(t *testing.T) {
	conn, r := dial(t)

	cmds := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n" +
		"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"
	if _, err := conn.Write([]byte(cmds)); err != nil {
		t.Fatal(err)
	}

	ok, _ := r.ReadString('\n')
	if strings.TrimRight(ok, "\r\n") != "+OK" {
		t.Fatalf("first reply = %q", ok)
	}
	r.ReadString('\n')
	body, _ := r.ReadString('\n')
	if strings.TrimRight(body, "\r\n") != "1" {
		t.Fatalf("second reply = %q", body)
	}
}

func TestAOFReplay(t *testing.T) {
	path := t.TempDir() + "/test.aof"

	aof, err := openAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}

	first := newServer()
	if _, err := first.replay(aof); err != nil {
		t.Fatal(err)
	}
	first.run([]string{"SET", "a", "hello"})
	first.run([]string{"SET", "b", "world"})
	first.run([]string{"DEL", "b"})
	aof.Close()

	reopened, err := openAOF(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	second := newServer()
	n, err := second.replay(reopened)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("replayed %d commands, want 3", n)
	}

	if got, ok := second.db.get("a"); !ok || got != "hello" {
		t.Fatalf("a = %q %v after replay", got, ok)
	}
	if _, ok := second.db.get("b"); ok {
		t.Fatal("b survived a replayed DEL")
	}
}

func TestAOFSkipsReads(t *testing.T) {
	path := t.TempDir() + "/test.aof"

	aof, _ := openAOF(path, true)
	s := newServer()
	s.replay(aof)

	s.run([]string{"SET", "a", "1"})
	s.run([]string{"GET", "a"})
	s.run([]string{"KEYS"})
	s.run([]string{"EXISTS", "a"})
	aof.Close()

	reopened, _ := openAOF(path, true)
	defer reopened.Close()

	n, err := newServer().replay(reopened)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("logged %d commands, want 1", n)
	}
}

func TestAOFTruncatedTail(t *testing.T) {
	path := t.TempDir() + "/test.aof"

	aof, _ := openAOF(path, true)
	s := newServer()
	s.replay(aof)
	s.run([]string{"SET", "a", "hello"})
	aof.Close()

	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(full, []byte("*3\r\n$3\r\nSET\r\n$1\r\nb")...), 0o644); err != nil {
		t.Fatal(err)
	}

	reopened, _ := openAOF(path, true)
	defer reopened.Close()

	recovered := newServer()
	n, err := recovered.replay(reopened)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("replayed %d commands, want 1", n)
	}
	if got, _ := recovered.db.get("a"); got != "hello" {
		t.Fatalf("a = %q, the good prefix should survive", got)
	}
}
