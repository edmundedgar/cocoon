package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
)

// fakeWSWriter is a minimal wsMessageWriter that records how many times
// NextWriter was called, without needing a real websocket handshake.
type fakeWSWriter struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeWSWriter) NextWriter(messageType int) (io.WriteCloser, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return nopWriteCloser{&bytes.Buffer{}}, nil
}

func (f *fakeWSWriter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestSendReposEventsReturnsOnContextCancel reproduces the relay-connection
// leak: once a connection's context is cancelled (which happens on every
// ordinary websocket read error — routine on any long-lived relay
// connection, not exotic), sendReposEvents must actually return so its
// caller's deferred cleanup (evtManCancel, the RelaysConnected metric, the
// requestCrawl retry) runs. Before the fix, the equivalent inline loop only
// exited a per-event closure on cancellation, not the loop itself, so it
// would sit forwarding (or silently discarding) events forever.
func TestSendReposEventsReturnsOnContextCancel(t *testing.T) {
	s := newTestServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled, mirroring a connection that already errored

	evts := make(chan *events.XRPCStreamEvent, 1)
	evts <- &events.XRPCStreamEvent{RepoAccount: &atproto.SyncSubscribeRepos_Account{}}

	conn := &fakeWSWriter{}

	done := make(chan struct{})
	go func() {
		s.sendReposEvents(ctx, conn, evts, "test-ident", discardLogger())
		close(done)
	}()

	select {
	case <-done:
		// returned promptly, as it should
	case <-time.After(2 * time.Second):
		t.Fatal("sendReposEvents did not return after context cancellation — " +
			"this is the leaked-goroutine bug: it should stop and return once " +
			"ctx is Done rather than looping forever")
	}
}

// TestSendReposEventsReturnsWhenChannelCloses covers the other exit path:
// the event manager closing evts (e.g. on its own cleanup) must also make
// sendReposEvents return, not just context cancellation.
func TestSendReposEventsReturnsWhenChannelCloses(t *testing.T) {
	s := newTestServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	evts := make(chan *events.XRPCStreamEvent)
	close(evts)

	conn := &fakeWSWriter{}

	done := make(chan struct{})
	go func() {
		s.sendReposEvents(ctx, conn, evts, "test-ident", discardLogger())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sendReposEvents did not return after evts closed")
	}
}

// TestSendReposEventsForwardsBeforeCancel confirms the extraction didn't
// change the happy path: events sent before cancellation are still written.
func TestSendReposEventsForwardsBeforeCancel(t *testing.T) {
	s := newTestServer(t)

	ctx, cancel := context.WithCancel(context.Background())

	evts := make(chan *events.XRPCStreamEvent, 3)
	for i := 0; i < 3; i++ {
		evts <- &events.XRPCStreamEvent{RepoAccount: &atproto.SyncSubscribeRepos_Account{}}
	}

	conn := &fakeWSWriter{}

	done := make(chan struct{})
	go func() {
		s.sendReposEvents(ctx, conn, evts, "test-ident", discardLogger())
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for conn.callCount() < 3 {
		select {
		case <-deadline:
			t.Fatalf("only %d of 3 events were forwarded before timeout", conn.callCount())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sendReposEvents did not return after context cancellation")
	}
}
