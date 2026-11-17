package main

// Ops for upstream package `broadcast`. The handler mirrors the upstream
// mocks.Handler usage: every HandleBroadcast/HandlePubSub call is recorded with
// its argument and returns nil. Broadcasters are the real Go implementations.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/anycable/anycable-go/broadcast"
)

type broadcastCall struct {
	Kind    string // "HandleBroadcast" or "HandlePubSub"
	Payload string
}

type broadcastHandler struct {
	mu     sync.Mutex
	calls  []broadcastCall
	notify chan struct{}
}

var _ broadcast.Handler = (*broadcastHandler)(nil)

func (h *broadcastHandler) record(kind string, data []byte) {
	h.mu.Lock()
	h.calls = append(h.calls, broadcastCall{kind, string(data)})
	h.mu.Unlock()
	select {
	case h.notify <- struct{}{}:
	default:
	}
}

func (h *broadcastHandler) HandleBroadcast(data []byte) error {
	h.record("HandleBroadcast", data)
	return nil
}

func (h *broadcastHandler) HandlePubSub(data []byte) { h.record("HandlePubSub", data) }

func (h *broadcastHandler) take() []broadcastCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.calls
	h.calls = nil
	if c == nil {
		c = []broadcastCall{}
	}
	return c
}

type broadcastBroadcaster struct {
	b    broadcast.Broadcaster
	http *broadcast.HTTPBroadcaster
}

func init() {
	handlerOf := func(id int64) (*broadcastHandler, error) { return msgGet[*broadcastHandler](id) }

	register("broadcast.NewHandler", func(json.RawMessage) (interface{}, error) {
		return msgPut(&broadcastHandler{notify: make(chan struct{}, 1)}), nil
	})
	// Calls recorded so far (and clears them).
	register("broadcast.HandlerCalls", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Handler int64 }](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		return h.take(), nil
	})
	// Upstream drainBroadcasts: collects calls until none arrived for `IdleMs`.
	register("broadcast.HandlerDrain", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Handler, IdleMs int64 }](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		out := []broadcastCall{}
		for {
			out = append(out, h.take()...)
			select {
			case <-h.notify:
			case <-time.After(msgTimeout(a.IdleMs)):
				return append(out, h.take()...), nil
			}
		}
	})

	register("broadcast.NewHTTPBroadcaster", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Handler int64
			Config  broadcast.HTTPConfig
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		config := a.Config
		b := broadcast.NewHTTPBroadcaster(h, &config, slog.Default())
		return msgPut(&broadcastBroadcaster{b: b, http: b}), nil
	})
	register("broadcast.NewRedisBroadcaster", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Handler int64
			Config  broadcast.RedisConfig
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		config := a.Config
		return msgPut(&broadcastBroadcaster{b: broadcast.NewRedisBroadcaster(h, &config, slog.Default())}), nil
	})
	register("broadcast.NewLegacyRedisBroadcaster", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Handler int64
			Config  broadcast.LegacyRedisConfig
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		config := a.Config
		return msgPut(&broadcastBroadcaster{b: broadcast.NewLegacyRedisBroadcaster(h, &config, slog.Default())}), nil
	})
	broadcasterOf := func(raw json.RawMessage) (*broadcastBroadcaster, error) {
		a, err := msgArgs[struct{ Broadcaster int64 }](raw)
		if err != nil {
			return nil, err
		}
		return msgGet[*broadcastBroadcaster](a.Broadcaster)
	}
	register("broadcast.Start", func(raw json.RawMessage) (interface{}, error) {
		b, err := broadcasterOf(raw)
		if err != nil {
			return nil, err
		}
		// Buffered: upstream tests use both `make(chan error)` and `make(chan error, 1)`
		// and never read it; a buffer keeps a late error from blocking a goroutine.
		return parsed{nil, errString(b.b.Start(make(chan error, 1)))}, nil
	})
	register("broadcast.Shutdown", func(raw json.RawMessage) (interface{}, error) {
		b, err := broadcasterOf(raw)
		if err != nil {
			return nil, err
		}
		return parsed{nil, errString(b.b.Shutdown(context.Background()))}, nil
	})
	register("broadcast.IsFanout", func(raw json.RawMessage) (interface{}, error) {
		b, err := broadcasterOf(raw)
		if err != nil {
			return nil, err
		}
		return b.b.IsFanout(), nil
	})
	register("broadcast.HTTPPrepare", func(raw json.RawMessage) (interface{}, error) {
		b, err := broadcasterOf(raw)
		if err != nil {
			return nil, err
		}
		return parsed{nil, errString(b.http.Prepare())}, nil
	})
	// Serves one request through HTTPBroadcaster.Handler with httptest (as upstream).
	register("broadcast.HTTPServe", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Broadcaster int64
			Method      string
			Headers     [][2]string
			Body        string
		}](raw)
		if err != nil {
			return nil, err
		}
		b, err := msgGet[*broadcastBroadcaster](a.Broadcaster)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest(a.Method, "/", bytes.NewReader([]byte(a.Body)))
		if err != nil {
			return nil, err
		}
		for _, h := range a.Headers {
			req.Header.Set(h[0], h[1])
		}
		rr := httptest.NewRecorder()
		http.HandlerFunc(b.http.Handler).ServeHTTP(rr, req)
		return rr.Code, nil
	})

	msgConfigOps("broadcast.HTTPConfig", broadcast.NewHTTPConfig, broadcast.HTTPConfig.ToToml)
	msgConfigOps("broadcast.RedisConfig", broadcast.NewRedisConfig, broadcast.RedisConfig.ToToml)
	msgConfigOps("broadcast.LegacyRedisConfig", broadcast.NewLegacyRedisConfig, broadcast.LegacyRedisConfig.ToToml)
	msgConfigOps("broadcast.LegacyNATSConfig", broadcast.NewLegacyNATSConfig, broadcast.LegacyNATSConfig.ToToml)
}
