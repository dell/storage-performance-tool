/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

type recordedTUIMessages struct {
	mu       sync.Mutex
	messages []tea.Msg
	quit     chan struct{}
	quitOnce sync.Once
}

func newRecordedTUIMessages() *recordedTUIMessages {
	return &recordedTUIMessages{quit: make(chan struct{})}
}

func (r *recordedTUIMessages) send(msg tea.Msg) {
	r.mu.Lock()
	r.messages = append(r.messages, msg)
	r.mu.Unlock()
	if _, ok := msg.(autoTerminateMsg); ok {
		r.quitOnce.Do(func() { close(r.quit) })
	}
}

func (r *recordedTUIMessages) text() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var lines []string
	for _, msg := range r.messages {
		if line, ok := msg.(sptMessageMsg); ok {
			lines = append(lines, string(line))
		}
	}
	return strings.Join(lines, "\n")
}

// At the deadline the TUI has the run session stop the step and closes only once the session
// reports the run stopped. Before the fix the TUI closed at the deadline and the step ran on.
func TestTUIAutoTerminateStopsTheRunBeforeClosing(t *testing.T) {
	session := runcontrol.NewSession()
	hooks := NewSessionLaunchHooks(session, nil)
	hooks.NotifySubmitted()
	recorded := newRecordedTUIMessages()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runAutoTerminateTimer(ctx, 1, hooks, recorded.send)

	select {
	case <-session.WorkloadStopRequested():
	case <-recorded.quit:
		t.Fatal("the TUI closed at the deadline without stopping the run")
	case <-time.After(5 * time.Second):
		t.Fatal("the deadline did not ask the session to stop the run")
	}
	select {
	case <-recorded.quit:
		t.Fatal("the TUI closed before the session reported the run stopped")
	case <-time.After(200 * time.Millisecond):
	}
	session.MarkWorkloadTerminal()
	select {
	case <-recorded.quit:
	case <-time.After(5 * time.Second):
		t.Fatal("the TUI did not close after the run stopped")
	}
	if text := recorded.text(); !strings.Contains(text, "Auto-terminate deadline (1 s) reached; stopping the run") {
		t.Fatalf("TUI messages lack the deadline line:\n%s", text)
	}
}

// Without a run session the TUI closes at the deadline and its caller stops the run.
func TestTUIAutoTerminateWithoutSessionClosesAtDeadline(t *testing.T) {
	hooks := NewLaunchHooks(nil)
	hooks.NotifySubmitted()
	recorded := newRecordedTUIMessages()
	go runAutoTerminateTimer(context.Background(), 1, hooks, recorded.send)
	select {
	case <-recorded.quit:
	case <-time.After(5 * time.Second):
		t.Fatal("the TUI did not close at the deadline")
	}
}
