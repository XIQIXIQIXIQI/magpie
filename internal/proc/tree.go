package proc

import (
	"os/exec"
	"sync"
	"time"
)

// A Tree is a started command and the processes it starts in turn: an
// agent's CLI with the MCP servers, shells and helpers it runs. Killing the
// CLI alone leaves those running, so they are ended with it.
type Tree struct {
	cmd *exec.Cmd

	mu     sync.Mutex
	done   bool // waited for: its pid may be another process's now
	exited chan struct{}
	stop   sync.Once
	group
}

// StartTree starts cmd in a group of its own: a session on Unix, a job on
// Windows. It has no terminal then, and a Ctrl+C there doesn't reach it.
func StartTree(cmd *exec.Cmd) (*Tree, error) {
	ownGroup(cmd)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	t := &Tree{cmd: cmd, exited: make(chan struct{})}
	t.join(cmd)
	return t, nil
}

// Wait waits for the command, then ends what it left running.
func (t *Tree) Wait() error {
	err := t.cmd.Wait()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done = true
	t.sweep(t.cmd)
	close(t.exited)
	return err
}

// Stop lets the command clean up before killing its tree after grace.
// Only the leader is signalled, so helpers remain available for cleanup.
// The caller must also Wait, on another goroutine. Concurrent calls share
// one grace period. Windows has no SIGTERM and kills the tree at once.
func (t *Tree) Stop(grace time.Duration) {
	t.stop.Do(func() {
		t.mu.Lock()
		if t.done {
			t.mu.Unlock()
			return
		}
		gentle := grace > 0 && t.terminate(t.cmd)
		t.mu.Unlock()
		if gentle {
			timer := time.NewTimer(grace)
			defer timer.Stop()
			select {
			case <-t.exited:
				return
			case <-timer.C:
			}
		}
		t.Kill()
	})
}

// Kill ends the command and every process of its tree.
func (t *Tree) Kill() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return
	}
	_ = t.kill(t.cmd)
	_ = t.cmd.Process.Kill()
}
