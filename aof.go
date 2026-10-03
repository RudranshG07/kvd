package main

import (
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

func openAOF(path string, fsync bool) (*aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &aof{f: f, fsync: fsync}, nil
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

func (a *aof) rewind() error {
	_, err := a.f.Seek(0, io.SeekStart)
	return err
}

func (a *aof) end() error {
	_, err := a.f.Seek(0, io.SeekEnd)
	return err
}

func (a *aof) Close() error {
	return a.f.Close()
}
