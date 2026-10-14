//go:build !windows

package gateway

import (
	"bufio"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/proc"
	"github.com/yetone/magpie/internal/provider"
)

func cleanupRun(t *testing.T, script string, args ...string) *subscriptionRun {
	t.Helper()
	cmd := proc.Command("/bin/sh", append([]string{"-c", script, "sh"}, args...)...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	run := &subscriptionRun{cmd: cmd}
	if err := run.launch(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { run.tree.Kill(); <-run.exited })
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(out).ReadString('\n'); ready <- line }()
	select {
	case line := <-ready:
		if line != "ready\n" {
			t.Fatalf("run not ready: %q", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not start")
	}
	return run
}

func TestClaudeAbortReleasesRefreshLock(t *testing.T) {
	for _, how := range []string{"abort", "client left", "initial error", "failed reply", "concurrent aborts"} {
		t.Run(how, func(t *testing.T) {
			lock := filepath.Join(t.TempDir(), "refresh.lock")
			run := cleanupRun(t, `
mkdir "$1" || exit 1
trap 'rmdir "$1"; exit 0' TERM
echo ready
while :; do sleep 0.02; done
`, lock)
			switch how {
			case "abort":
				run.abort()
			case "client left":
				run.letGo() // a fresh turn cannot be rewound
			case "initial error":
				events := make(chan Event, 1)
				events <- Event{Kind: KError, Text: "refresh failed"}
				close(events)
				code, _ := relay(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/messages", nil),
					provider.Anthropic, "Claude", &Request{Stream: true}, events, &Usage{}, run.abort, nil, nil)
				if code != 502 {
					t.Fatalf("initial error returned %d", code)
				}
			case "failed reply":
				run.ended(&Request{}, "", "", false)
			case "concurrent aborts":
				var callers sync.WaitGroup
				for range 8 {
					callers.Go(run.abort)
				}
				callers.Wait()
			}
			<-run.exited
			if _, err := os.Stat(lock); !os.IsNotExist(err) {
				t.Fatalf("Claude abort left refresh lock: %v", err)
			}
			if !run.cmd.ProcessState.Success() {
				t.Fatalf("Claude cleanup failed: %s", run.cmd.ProcessState)
			}
			run.mu.Lock()
			closed, killed := run.closed, run.killed
			run.mu.Unlock()
			if !closed || !killed {
				t.Fatalf("aborted run: closed=%t killed=%t", closed, killed)
			}
		})
	}
}

func TestClaudeAbortAllStartsCleanupTogether(t *testing.T) {
	dir := t.TempDir()
	locks := []string{filepath.Join(dir, "first.lock"), filepath.Join(dir, "second.lock")}
	b := idleTestBridge()
	for i, lock := range locks {
		run := cleanupRun(t, `
mkdir "$1" || exit 1
trap 'touch "$1.stopping"; while [ ! -f "$2.stopping" ]; do sleep 0.02; done; rmdir "$1"; exit 0' TERM
echo ready
while :; do sleep 0.02; done
`, lock, locks[1-i])
		run.bridge, run.token = b, lock
		b.runs[lock] = run
	}
	b.abortAll()
	for _, lock := range locks {
		if _, err := os.Stat(lock); !os.IsNotExist(err) {
			t.Fatalf("shutdown did not let both runs clean up: %s: %v", lock, err)
		}
	}
}

func TestClaudeAbortingRunCannotBeReused(t *testing.T) {
	dir := t.TempDir()
	lock := filepath.Join(dir, "refresh.lock")
	stopping, release := filepath.Join(dir, "stopping"), filepath.Join(dir, "release")
	run := cleanupRun(t, `
mkdir "$1" || exit 1
trap 'touch "$2"; while [ ! -f "$3" ]; do sleep 0.02; done; rmdir "$1"; exit 0' TERM
echo ready
while :; do sleep 0.02; done
`, lock, stopping, release)
	aborted := make(chan struct{})
	go func() { run.abort(); close(aborted) }()
	t.Cleanup(func() { run.tree.Kill(); <-aborted })
	eventually(t, "Claude began cleanup", func() bool { _, err := os.Stat(stopping); return err == nil })
	if run.claimResume() {
		t.Error("a run still cleaning up accepted another tool-result request")
	}
	b := idleTestBridge()
	if b.keepIdle(run, "next", "conversation") {
		t.Error("a run still cleaning up was kept for another turn")
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	<-aborted
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Fatalf("Claude abort left refresh lock: %v", err)
	}
}
