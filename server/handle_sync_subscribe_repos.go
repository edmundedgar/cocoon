package server

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/btcsuite/websocket"
	"github.com/haileyok/cocoon/metrics"
	"github.com/labstack/echo/v4"
)

// subscribeReposMsgType maps a stream event to its com.atproto.sync.subscribeRepos
// message frame type and the object to serialize. The bool is false for events
// that are not message frames (e.g. error frames, handled separately).
func subscribeReposMsgType(evt *events.XRPCStreamEvent) (string, util.CBOR, bool) {
	switch {
	case evt.RepoCommit != nil:
		return "#commit", evt.RepoCommit, true
	case evt.RepoSync != nil:
		return "#sync", evt.RepoSync, true
	case evt.RepoIdentity != nil:
		return "#identity", evt.RepoIdentity, true
	case evt.RepoAccount != nil:
		return "#account", evt.RepoAccount, true
	case evt.RepoInfo != nil:
		return "#info", evt.RepoInfo, true
	default:
		return "", nil, false
	}
}

func (s *Server) handleSyncSubscribeRepos(e echo.Context) error {
	ctx, cancel := context.WithCancel(e.Request().Context())
	defer cancel()

	logger := s.logger.With("component", "subscribe-repos-websocket")

	conn, err := websocket.Upgrade(e.Response().Writer, e.Request(), e.Response().Header(), 1<<10, 1<<10)
	if err != nil {
		logger.Error("unable to establish websocket with relay", "err", err)
		return err
	}

	ident := e.RealIP() + "-" + e.Request().UserAgent()
	logger = logger.With("ident", ident)
	logger.Info("new connection established")

	var since *int64
	if cursorStr := e.QueryParam("cursor"); cursorStr != "" {
		cursor, err := strconv.ParseInt(cursorStr, 10, 64)
		if err != nil {
			logger.Warn("invalid cursor parameter", "cursor", cursorStr, "err", err)
		} else {
			since = &cursor
			logger.Info("subscribing with cursor", "cursor", cursor)
		}
	}

	metrics.RelaysConnected.WithLabelValues(ident).Inc()
	defer func() {
		metrics.RelaysConnected.WithLabelValues(ident).Dec()
	}()

	evts, evtManCancel, err := s.evtman.Subscribe(ctx, ident, func(evt *events.XRPCStreamEvent) bool {
		return true
	}, since)
	if err != nil {
		return err
	}
	defer evtManCancel()

	// drop the connection whenever a subscriber disconnects from the socket, we should get errors
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if _, _, err := conn.ReadMessage(); err != nil {
					logger.Warn("websocket error", "err", err)
					cancel()
					return
				}
			}
		}
	}()

	s.sendReposEvents(ctx, conn, evts, ident, logger)

	// we should tell the relay to request a new crawl at this point if we got disconnected
	// use a new context since the old one might be cancelled at this point
	go func() {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer retryCancel()
		if err := s.requestCrawl(retryCtx); err != nil {
			logger.Error("error requesting crawls", "err", err)
		}
	}()

	return nil
}

// wsMessageWriter is the slice of *websocket.Conn that sendReposEvents needs.
// Narrowed to an interface so the event-forwarding loop is testable without a
// real websocket handshake.
type wsMessageWriter interface {
	NextWriter(messageType int) (io.WriteCloser, error)
}

// sendReposEvents forwards events from evts to conn until ctx is cancelled or
// evts is closed, then returns.
//
// This used to be `for evt := range evts { ...; if ctx.Err() != nil { return } }`,
// where that `return` only exited the per-event closure, not the loop — so
// once a connection's context was cancelled (which happens on every ordinary
// websocket read error, i.e. routinely on any long-lived relay connection),
// this would never actually return. It would sit blocked on the next receive
// from evts, and once a new event eventually arrived, log "context error"
// and go right back to waiting for another one, forever. The deferred
// evtManCancel() above (and the relay-metrics decrement, and the requestCrawl
// retry after this call) never ran, leaking a goroutine and an
// events.EventManager subscription per disconnect for the life of the
// process. Selecting on ctx.Done() alongside evts fixes that: cancellation
// is noticed immediately, whether or not another event ever arrives.
func (s *Server) sendReposEvents(ctx context.Context, conn wsMessageWriter, evts <-chan *events.XRPCStreamEvent, ident string, logger *slog.Logger) {
	header := events.EventHeader{Op: events.EvtKindMessage}
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-evts:
			if !ok {
				return
			}
			s.sendReposEvent(conn, ident, &header, evt, logger)
		}
	}
}

// sendReposEvent serializes and writes a single event frame to conn.
func (s *Server) sendReposEvent(conn wsMessageWriter, ident string, header *events.EventHeader, evt *events.XRPCStreamEvent, logger *slog.Logger) {
	defer func() {
		metrics.RelaySends.WithLabelValues(ident, header.MsgType).Inc()
	}()

	wc, err := conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		logger.Error("error writing message to relay", "err", err)
		return
	}

	var obj util.CBOR
	if evt.Error != nil {
		header.Op = events.EvtKindErrorFrame
		header.MsgType = ""
		obj = evt.Error
	} else if msgType, o, ok := subscribeReposMsgType(evt); ok {
		header.Op = events.EvtKindMessage
		header.MsgType = msgType
		obj = o
	} else {
		logger.Warn("unrecognized event kind")
		return
	}

	if err := header.MarshalCBOR(wc); err != nil {
		logger.Error("failed to write header to relay", "err", err)
		return
	}

	if err := obj.MarshalCBOR(wc); err != nil {
		logger.Error("failed to write event to relay", "err", err)
		return
	}

	if err := wc.Close(); err != nil {
		logger.Error("failed to flush-close our event write", "err", err)
		return
	}
}
