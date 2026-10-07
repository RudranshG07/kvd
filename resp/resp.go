package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

var ErrProtocol = errors.New("protocol error")

const Nil = "$-1\r\n"

func Simple(s string) string { return "+" + s + "\r\n" }
func Fail(s string) string   { return "-ERR " + s + "\r\n" }
func Integer(n int) string   { return ":" + strconv.Itoa(n) + "\r\n" }
func Bulk(s string) string   { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }

func Array(items []string) string {
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(items)) + "\r\n")
	for _, item := range items {
		b.WriteString(Bulk(item))
	}
	return b.String()
}

func ReadCommand(r *bufio.Reader) ([]string, error) {
	args, err := readArray(r)
	if err == nil && len(args) == 0 {
		err = ErrProtocol
	}
	return args, err
}

func ParseArray(reply string) ([]string, error) {
	return readArray(bufio.NewReader(strings.NewReader(reply)))
}

func ReadReply(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 3 {
		return "", ErrProtocol
	}

	switch line[0] {
	case '+', '-', ':':
		return line, nil
	case '$':
		n, err := strconv.Atoi(strings.TrimRight(line[1:], "\r\n"))
		if err != nil {
			return "", ErrProtocol
		}
		if n < 0 {
			return line, nil
		}
		body := make([]byte, n+2)
		_, err = io.ReadFull(r, body)
		return line + string(body), err
	case '*':
		n, err := strconv.Atoi(strings.TrimRight(line[1:], "\r\n"))
		if err != nil {
			return "", ErrProtocol
		}
		for i := 0; i < n; i++ {
			part, err := ReadReply(r)
			if err != nil {
				return "", err
			}
			line += part
		}
		return line, nil
	}
	return "", ErrProtocol
}

func readArray(r *bufio.Reader) ([]string, error) {
	n, err := header(r, '*')
	if err != nil {
		return nil, err
	}

	out := make([]string, n)
	for i := range out {
		size, err := header(r, '$')
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out[i] = string(buf[:size])
	}
	return out, nil
}

func header(r *bufio.Reader, kind byte) (int, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) < 2 || line[0] != kind {
		return 0, ErrProtocol
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 {
		return 0, ErrProtocol
	}
	return n, nil
}
