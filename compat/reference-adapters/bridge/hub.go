package main

// Ops for upstream package `hub`. Sessions are test doubles mirroring the
// upstream MockSession (hub/hub_test.go): frames are encoded with encoders.JSON
// and queued; reads wait with a deadline. Hub and Gate behavior is the real Go
// implementation.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/encoders"
	"github.com/anycable/anycable-go/hub"
)

type hubSession struct {
	sid         string
	identifiers string
	encoder     encoders.JSON

	mu          sync.Mutex
	frames      [][]byte
	queueClosed bool
	notify      chan struct{}

	closeMu sync.Mutex
	closed  bool
}

var _ hub.HubSession = (*hubSession)(nil)

func newHubSession(sid string) *hubSession {
	return &hubSession{sid: sid, notify: make(chan struct{}, 1)}
}

func (s *hubSession) GetID() string { return s.sid }

// GetIdentifiers is upstream MockSession's: the identifiers when set, else the sid.
func (s *hubSession) GetIdentifiers() string {
	if s.identifiers != "" {
		return s.identifiers
	}
	return s.sid
}

func (s *hubSession) Send(msg encoders.EncodedMessage) {
	var payload []byte
	if cm, ok := msg.(*encoders.CachedEncodedMessage); ok {
		frame, _ := cm.Fetch(s.encoder.ID(), s.encoder.Encode)
		payload = frame.Payload
	} else {
		frame, _ := s.encoder.Encode(msg)
		payload = frame.Payload
	}
	s.mu.Lock()
	if s.queueClosed {
		s.mu.Unlock()
		return
	}
	s.frames = append(s.frames, payload)
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *hubSession) DisconnectWithMessage(msg encoders.EncodedMessage, code string) {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return
	}
	s.Send(msg)
	s.closed = true
}

func (s *hubSession) Closed() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return s.closed
}

// read pops the oldest frame, waiting up to timeout (upstream MockSession.Read).
func (s *hubSession) read(timeout time.Duration) ([]byte, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		s.mu.Lock()
		if len(s.frames) > 0 {
			f := s.frames[0]
			s.frames = s.frames[1:]
			s.mu.Unlock()
			return f, nil
		}
		s.mu.Unlock()
		select {
		case <-s.notify:
		case <-deadline.C:
			return nil, errors.New("Session hasn't received any messages")
		}
	}
}

// drain waits until at least n frames are queued (or the deadline passes), then
// removes and returns all queued frames.
func (s *hubSession) drain(n int, deadline time.Time) [][]byte {
	for {
		s.mu.Lock()
		if len(s.frames) >= n || !time.Now().Before(deadline) {
			f := s.frames
			s.frames = nil
			s.mu.Unlock()
			return f
		}
		s.mu.Unlock()
		select {
		case <-s.notify:
		case <-time.After(time.Until(deadline)):
		}
	}
}

// capturingSession records the EncodedMessage values passed to Send.
type capturingSession struct {
	sent chan encoders.EncodedMessage
}

func (s *capturingSession) GetID() string          { return "capture" }
func (s *capturingSession) GetIdentifiers() string { return "capture" }
func (s *capturingSession) Send(msg encoders.EncodedMessage) {
	select {
	case s.sent <- msg:
	default:
	}
}
func (s *capturingSession) DisconnectWithMessage(encoders.EncodedMessage, string) {}

type hubGate struct {
	gate   *hub.Gate
	cancel context.CancelFunc
}

func init() {
	hubOf := func(id int64) (*hub.Hub, error) { return msgGet[*hub.Hub](id) }
	sessOf := func(id int64) (*hubSession, error) { return msgGet[*hubSession](id) }

	register("hub.NewHub", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ PoolSize int }](raw)
		if err != nil {
			return nil, err
		}
		return msgPut(hub.NewHub(a.PoolSize, slog.Default())), nil
	})
	register("hub.Run", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Hub int64 }](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		go h.Run()
		return true, nil
	})
	register("hub.Shutdown", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Hub int64 }](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		h.Shutdown()
		return true, nil
	})
	register("hub.NewSession", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Sid, Identifiers string }](raw)
		if err != nil {
			return nil, err
		}
		s := newHubSession(a.Sid)
		s.identifiers = a.Identifiers
		return msgPut(s), nil
	})
	register("hub.NewSessions", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Sids []string }](raw)
		if err != nil {
			return nil, err
		}
		ids := make([]int64, 0, len(a.Sids))
		for _, sid := range a.Sids {
			ids = append(ids, msgPut(newHubSession(sid)))
		}
		return ids, nil
	})
	type hubSessionArgs struct {
		Hub, Session int64
		Stream       string
		Identifier   string
	}
	withHubSession := func(raw json.RawMessage, f func(*hub.Hub, *hubSession, hubSessionArgs)) (interface{}, error) {
		a, err := msgArgs[hubSessionArgs](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		s, err := sessOf(a.Session)
		if err != nil {
			return nil, err
		}
		f(h, s, a)
		return true, nil
	}
	register("hub.AddSession", func(raw json.RawMessage) (interface{}, error) {
		return withHubSession(raw, func(h *hub.Hub, s *hubSession, _ hubSessionArgs) { h.AddSession(s) })
	})
	register("hub.RemoveSession", func(raw json.RawMessage) (interface{}, error) {
		return withHubSession(raw, func(h *hub.Hub, s *hubSession, _ hubSessionArgs) { h.RemoveSession(s) })
	})
	register("hub.SubscribeSession", func(raw json.RawMessage) (interface{}, error) {
		return withHubSession(raw, func(h *hub.Hub, s *hubSession, a hubSessionArgs) {
			h.SubscribeSession(s, a.Stream, a.Identifier)
		})
	})
	register("hub.UnsubscribeSession", func(raw json.RawMessage) (interface{}, error) {
		return withHubSession(raw, func(h *hub.Hub, s *hubSession, a hubSessionArgs) {
			h.UnsubscribeSession(s, a.Stream, a.Identifier)
		})
	})
	register("hub.UnsubscribeSessionFromChannel", func(raw json.RawMessage) (interface{}, error) {
		return withHubSession(raw, func(h *hub.Hub, s *hubSession, a hubSessionArgs) {
			h.UnsubscribeSessionFromChannel(s, a.Identifier)
		})
	})
	// Batch forms of AddSession/SubscribeSession (one call per item, in order),
	// so large benchmark configurations do not need one bridge round trip each.
	register("hub.AddSessions", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Hub      int64
			Sessions []int64
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		for _, id := range a.Sessions {
			s, err := sessOf(id)
			if err != nil {
				return nil, err
			}
			h.AddSession(s)
		}
		return true, nil
	})
	register("hub.SubscribeSessions", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Hub  int64
			Subs []struct {
				Session    int64
				Stream     string
				Identifier string
			}
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		for _, sub := range a.Subs {
			s, err := sessOf(sub.Session)
			if err != nil {
				return nil, err
			}
			h.SubscribeSession(s, sub.Stream, sub.Identifier)
		}
		return true, nil
	})
	register("hub.Broadcast", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Hub          int64
			Stream, Data string
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		h.Broadcast(a.Stream, a.Data)
		return true, nil
	})
	// Calls Broadcast(stream, data) for each item, in order.
	register("hub.BroadcastMany", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Hub   int64
			Items []struct{ Stream, Data string }
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		for _, it := range a.Items {
			h.Broadcast(it.Stream, it.Data)
		}
		return true, nil
	})
	register("hub.BroadcastMessage", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Hub int64
			Msg common.StreamMessage
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		msg := a.Msg
		h.BroadcastMessage(&msg)
		return true, nil
	})
	register("hub.RemoteDisconnect", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Hub int64
			Msg common.RemoteDisconnectMessage
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		msg := a.Msg
		h.RemoteDisconnect(&msg)
		return true, nil
	})
	register("hub.Size", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Hub int64 }](raw)
		if err != nil {
			return nil, err
		}
		h, err := hubOf(a.Hub)
		if err != nil {
			return nil, err
		}
		return h.Size(), nil
	})
	register("hub.SessionRead", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Session, TimeoutMs int64 }](raw)
		if err != nil {
			return nil, err
		}
		s, err := sessOf(a.Session)
		if err != nil {
			return nil, err
		}
		frame, rerr := s.read(msgTimeout(a.TimeoutMs))
		if rerr != nil {
			return parsed{nil, errString(rerr)}, nil
		}
		return parsed{string(frame), nil}, nil
	})
	register("hub.SessionClosed", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Session int64 }](raw)
		if err != nil {
			return nil, err
		}
		s, err := sessOf(a.Session)
		if err != nil {
			return nil, err
		}
		return s.Closed(), nil
	})
	// For each session, waits (shared deadline) until it has queued at least the
	// expected number of frames, then drains it and returns frame -> count.
	register("hub.DrainSessions", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Sessions  []int64
			Expected  []int
			TimeoutMs int64
		}](raw)
		if err != nil {
			return nil, err
		}
		deadline := time.Now().Add(msgTimeout(a.TimeoutMs))
		out := make([]map[string]int, 0, len(a.Sessions))
		for i, id := range a.Sessions {
			s, err := sessOf(id)
			if err != nil {
				return nil, err
			}
			counts := map[string]int{}
			for _, f := range s.drain(a.Expected[i], deadline) {
				counts[string(f)]++
			}
			out = append(out, counts)
		}
		return out, nil
	})

	// Observes the unexported buildMessage(msg, identifier): Gate.performBroadcast
	// passes exactly that value to HubSession.Send, so a session subscribed to
	// (msg.Stream, identifier) captures it. Returns json.Marshal(&msg) of the
	// captured EncodedMessage, as upstream's toJSON helper.
	register("hub.BuildMessageJSON", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Msg        common.StreamMessage
			Identifier string
		}](raw)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gate := hub.NewGate(ctx, slog.Default())
		s := &capturingSession{sent: make(chan encoders.EncodedMessage, 1)}
		gate.Subscribe(s, a.Msg.Stream, a.Identifier)
		msg := a.Msg
		gate.Broadcast(&msg)
		select {
		case captured := <-s.sent:
			b, err := json.Marshal(&captured)
			if err != nil {
				return nil, err
			}
			return string(b), nil
		case <-time.After(5 * time.Second):
			return nil, errors.New("gate did not deliver the message within 5s")
		}
	})

	register("hub.NewGate", func(raw json.RawMessage) (interface{}, error) {
		ctx, cancel := context.WithCancel(context.Background())
		return msgPut(&hubGate{gate: hub.NewGate(ctx, slog.Default()), cancel: cancel}), nil
	})
	register("hub.CancelGate", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Gate int64 }](raw)
		if err != nil {
			return nil, err
		}
		g, err := msgGet[*hubGate](a.Gate)
		if err != nil {
			return nil, err
		}
		g.cancel()
		return true, nil
	})
	type gateArgs struct {
		Gate, Session      int64
		Stream, Identifier string
	}
	withGate := func(raw json.RawMessage, f func(*hub.Gate, *hubSession, gateArgs)) (interface{}, error) {
		a, err := msgArgs[gateArgs](raw)
		if err != nil {
			return nil, err
		}
		g, err := msgGet[*hubGate](a.Gate)
		if err != nil {
			return nil, err
		}
		s, err := sessOf(a.Session)
		if err != nil {
			return nil, err
		}
		f(g.gate, s, a)
		return true, nil
	}
	register("hub.GateSubscribe", func(raw json.RawMessage) (interface{}, error) {
		return withGate(raw, func(g *hub.Gate, s *hubSession, a gateArgs) { g.Subscribe(s, a.Stream, a.Identifier) })
	})
	register("hub.GateUnsubscribe", func(raw json.RawMessage) (interface{}, error) {
		return withGate(raw, func(g *hub.Gate, s *hubSession, a gateArgs) { g.Unsubscribe(s, a.Stream, a.Identifier) })
	})
	register("hub.GateSize", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Gate int64 }](raw)
		if err != nil {
			return nil, err
		}
		g, err := msgGet[*hubGate](a.Gate)
		if err != nil {
			return nil, err
		}
		return g.gate.Size(), nil
	})
	register("hub.GateBroadcast", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Gate int64
			Msg  common.StreamMessage
		}](raw)
		if err != nil {
			return nil, err
		}
		g, err := msgGet[*hubGate](a.Gate)
		if err != nil {
			return nil, err
		}
		msg := a.Msg
		g.gate.Broadcast(&msg)
		return true, nil
	})
}
