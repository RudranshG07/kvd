package main

import (
	"bufio"
	"io"
	"os"
	"sync"

	"miniredis/resp"
)

type aof struct {
	mu    sync.Mutex
	f     *os.File
	fsync bool
}

func openAOF(path string, fsync bool) (*aof, [][]string, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}

	var cmds [][]string
	r := bufio.NewReader(f)
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			break
		}
		cmds = append(cmds, args)
	}
	return &aof{f: f, fsync: fsync}, cmds, nil
}

func (a *aof) write(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := io.WriteString(a.f, resp.Array(args)); err != nil {
		return err
	}
	if a.fsync {
		return a.f.Sync()
	}
	return nil
}
