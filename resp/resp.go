// Package resp reads and writes the redis wire protocol.
package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

var ErrProtocol = errors.New("protocol error")

// ReadCommand reads one array of bulk strings, which is how clients send
// everything.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	line, err := ReadLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[0] != '*' {
		return nil, ErrProtocol
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil || count < 1 {
		return nil, ErrProtocol
	}

	args := make([]string, count)
	for i := range args {
		if args[i], err = readBulk(r); err != nil {
			return nil, err
		}
	}
	return args, nil
}

// ReadReply copies one whole reply back, however many lines it spans.
func ReadReply(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}

	switch line[0] {
	case '+', '-', ':':
		return line, nil

	case '$':
		size, err := strconv.Atoi(trim(line[1:]))
		if err != nil {
			return "", ErrProtocol
		}
		if size < 0 {
			return line, nil
		}
		body := make([]byte, size+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return "", err
		}
		return line + string(body), nil

	case '*':
		count, err := strconv.Atoi(trim(line[1:]))
		if err != nil {
			return "", ErrProtocol
		}
		var b strings.Builder
		b.WriteString(line)
		for i := 0; i < count; i++ {
			part, err := ReadReply(r)
			if err != nil {
				return "", err
			}
			b.WriteString(part)
		}
		return b.String(), nil
	}

	return "", ErrProtocol
}

func ReadLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return trim(line), nil
}

func readBulk(r *bufio.Reader) (string, error) {
	line, err := ReadLine(r)
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[0] != '$' {
		return "", ErrProtocol
	}

	size, err := strconv.Atoi(line[1:])
	if err != nil || size < 0 {
		return "", ErrProtocol
	}

	buf := make([]byte, size+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf[:size]), nil
}

func trim(s string) string {
	return strings.TrimSuffix(strings.TrimSuffix(s, "\n"), "\r")
}

func Simple(s string) string { return "+" + s + "\r\n" }
func Fail(s string) string   { return "-ERR " + s + "\r\n" }
func Integer(n int) string   { return ":" + strconv.Itoa(n) + "\r\n" }
func Bulk(s string) string   { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }
func NilBulk() string        { return "$-1\r\n" }

func Array(items []string) string {
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(items)) + "\r\n")
	for _, item := range items {
		b.WriteString(Bulk(item))
	}
	return b.String()
}

// ParseArray pulls the bulk strings back out of an array reply.
func ParseArray(reply string) ([]string, error) {
	return ReadCommand(bufio.NewReader(strings.NewReader(reply)))
}
