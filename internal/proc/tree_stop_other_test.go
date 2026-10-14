//go:build !windows

package proc

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func stopTree(t *testing.T, script string, args ...string) (*Tree, *bufio.Reader, <-chan struct{}) {
	t.Helper()
	cmd := Command("/bin/sh", append([]string{"-c", script, "sh"}, args...)...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	tree, err := StartTree(cmd)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = tree.Wait(); close(done) }()
	t.Cleanup(func() { tree.Kill(); <-done })
	reader := bufio.NewReader(out)
	stopLine(t, reader, "ready\n")
	return tree, reader, done
}

func stopLine(t *testing.T, reader *bufio.Reader, want string) {
	t.Helper()
	line := make(chan string, 1)
	go func() { s, _ := reader.ReadString('\n'); line <- s }()
	select {
	case got := <-line:
		if got != want {
			t.Fatalf("process said %q, want %q", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("process did not answer")
	}
}

func TestTreeStopReleasesLock(t *testing.T) {
	dir := t.TempDir()
	lock, pidfile := filepath.Join(dir, "refresh.lock"), filepath.Join(dir, "child")
	tree, _, done := stopTree(t, `
mkdir "$1" || exit 1
sleep 600 >/dev/null 2>&1 & child=$!
echo "$child" > "$2"
trap 'kill -0 "$child" || exit 2; rmdir "$1"; exit 0' TERM
echo ready
while :; do sleep 0.02; done
`, lock, pidfile)
	tree.Stop(5 * time.Second)
	<-done
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Fatalf("refresh lock left after stop: %v", err)
	}
	if !tree.cmd.ProcessState.Success() {
		t.Fatalf("cleanup failed: %s", tree.cmd.ProcessState)
	}
	pid, err := os.ReadFile(pidfile)
	if err != nil {
		t.Fatal(err)
	}
	child, err := strconv.Atoi(strings.TrimSpace(string(pid)))
	if err != nil {
		t.Fatal(err)
	}
	if !gone(child) {
		t.Fatalf("helper %d outlived the leader's cleanup", child)
	}
	tree.Stop(time.Hour) // a waited-for PID must never be signalled again
}

func TestTreeStopSharesDeadline(t *testing.T) {
	tree, reader, done := stopTree(t, `
trap 'echo stopping; trap "" TERM' TERM
echo ready
while :; do sleep 0.02; done
`)
	first := make(chan struct{})
	go func() { tree.Stop(time.Second); close(first) }()
	stopLine(t, reader, "stopping\n")
	var callers sync.WaitGroup
	for range 8 {
		callers.Go(func() { tree.Stop(time.Hour) })
	}
	all := make(chan struct{})
	go func() { callers.Wait(); close(all) }()
	// The first deadline must kill an unresponsive leader, and every later
	// caller must finish too. Ten seconds leaves room for a loaded runner;
	// the competing deadlines are one hour, not near that boundary.
	for _, ch := range []<-chan struct{}{first, all, done} {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent stops did not share the bounded grace period")
		}
	}
	if tree.cmd.ProcessState.Success() {
		t.Fatal("unresponsive leader was not forcibly ended")
	}
}
