package main

// Package ds (durable streams). Pure functions are called directly; the
// handler tests run behind handles:
//
//   - ds.Harness.*: a node with a mocks.Controller and either the handler
//     test's buildNode() mocks.Broker or the integration/auth tests' memory
//     broker (setupIntegrationServer). Expectations are programmed with
//     On(...).Return(...) from JSON; testify panics on unexpected calls are
//     recovered (or observed on their way out of a served request) and reported.
//   - ds.Handler.*: DSHandler instances over a harness, served either with an
//     httptest recorder (handler_test.go) or on an httptest.Server
//     (auth_test.go, integration_test.go), whose URL the Rust side talks to.
//
// The stream resolvers the tests define inline are selected by kind; the one
// with assertions inside (TestDSHandler_HEAD "stream authorization") records
// what it received so the Rust side asserts on it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/anycable/anycable-go/broker"
	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/ds"
	"github.com/anycable/anycable-go/metrics"
	"github.com/anycable/anycable-go/mocks"
	"github.com/anycable/anycable-go/node"
	"github.com/anycable/anycable-go/pubsub"
	"github.com/anycable/anycable-go/server"
	"github.com/anycable/anycable-go/streams"
	"github.com/stretchr/testify/mock"
)

// pollConnectionStep is one call on a ds.PollConnection over an httptest recorder.
type pollConnectionStep struct {
	Op     string // preset_header | cache_control | etag | write | close | context_done | state
	Name   string
	Value  string
	Prefix string
	Suffix string
	Msg    []byte
	Code   int
	Reason string
}

type pollConnectionResult struct {
	Error     *string             `json:"error"`
	Code      int                 `json:"code"`
	Headers   map[string][]string `json:"headers"`
	Body      string              `json:"body"`
	Bool      bool                `json:"bool"`
	ETagCalls []string            `json:"etag_calls"`
}

type dsHarness struct {
	n          *node.Node
	controller *mocks.Controller
	mockBroker *mocks.Broker  // buildNode() (handler_test.go)
	memBroker  *broker.Memory // setupIntegrationServer() / auth_test.go
	mu         sync.Mutex
	panics     []string
}

func (h *dsHarness) recordPanic(r interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.panics = append(h.panics, fmt.Sprint(r))
}

func (h *dsHarness) broker() broker.Broker {
	if h.mockBroker != nil {
		return h.mockBroker
	}
	return h.memBroker
}

type dsHandlerObj struct {
	harness  *dsHarness
	h        http.Handler
	shutdown context.CancelFunc
	server   *httptest.Server
	mu       sync.Mutex
	resolved []dsResolved
}

// dsServeObj is one request served with a recorder.
type dsServeObj struct {
	w        *httptest.ResponseRecorder
	done     chan struct{}
	panicMsg *string
}

// dsResolved is one identifier the recording resolver received.
type dsResolved struct {
	Identifier string            `json:"identifier"`
	Decoded    map[string]string `json:"decoded"`
	Error      *string           `json:"error"`
}

type dsStreamsKind struct {
	Kind   string // echo | require_signed | reject_noauth | signed
	Secret string
	Public bool
}

type dsControllerExpectation struct {
	Method             string // Authenticate | Subscribe | Shutdown
	Sid                *string
	IdentifierContains *string
	Times              int // 0: unlimited, 1: Once, 2: Twice
	Maybe              bool
	ConnectResult      *common.ConnectResult
	CommandResult      *common.CommandResult
}

type dsBrokerExpectation struct {
	Method  string // Peak | HistorySince | HistoryFrom | Subscribe
	Stream  string
	Ts      int64
	Epoch   string
	Offset  uint64
	Message *common.StreamMessage   // Peak
	History *[]common.StreamMessage // HistorySince / HistoryFrom (null: nil slice)
	Error   *string
}

func dsHarnessGet(raw json.RawMessage) (*dsHarness, error) {
	a, err := msgArgs[struct{ Harness int64 }](raw)
	if err != nil {
		return nil, err
	}
	return msgGet[*dsHarness](a.Harness)
}

func dsHandlerGet(raw json.RawMessage) (*dsHandlerObj, error) {
	a, err := msgArgs[struct{ Handler int64 }](raw)
	if err != nil {
		return nil, err
	}
	return msgGet[*dsHandlerObj](a.Handler)
}

// dsResolver builds the streams controller of a test.
func (o *dsHandlerObj) streamsController(k dsStreamsKind) (*streams.Controller, error) {
	switch k.Kind {
	case "echo": // buildNode()
		return streams.NewController("", func(identifier string) (*streams.SubscribeRequest, error) {
			var request streams.SubscribeRequest
			if err := json.Unmarshal([]byte(identifier), &request); err != nil {
				return nil, err
			}
			return &streams.SubscribeRequest{StreamName: request.StreamName}, nil
		}, quietLog), nil
	case "require_signed": // TestDSHandler_HEAD "stream authorization"
		return streams.NewController("", func(identifier string) (*streams.SubscribeRequest, error) {
			var resolved map[string]string
			err := json.Unmarshal([]byte(identifier), &resolved)
			o.mu.Lock()
			o.resolved = append(o.resolved, dsResolved{identifier, resolved, errString(err)})
			o.mu.Unlock()
			if resolved["signed_stream_name"] == "" {
				return nil, errors.New("signed stream name is missing")
			}
			return &streams.SubscribeRequest{StreamName: resolved["stream_name"]}, nil
		}, quietLog), nil
	case "reject_noauth": // setupIntegrationServer()
		return streams.NewController("", func(identifier string) (*streams.SubscribeRequest, error) {
			if strings.Contains(identifier, "noauth") {
				return nil, errors.New("unauthenticated")
			}
			var request streams.SubscribeRequest
			if err := json.Unmarshal([]byte(identifier), &request); err != nil {
				return nil, err
			}
			return &streams.SubscribeRequest{StreamName: request.StreamName}, nil
		}, quietLog), nil
	case "signed": // auth_test.go: streams.NewStreamsController(&conf)
		conf := streams.NewConfig()
		conf.Secret = k.Secret
		conf.Public = k.Public
		return streams.NewStreamsController(&conf, quietLog), nil
	}
	return nil, fmt.Errorf("unknown streams controller kind %q", k.Kind)
}

func dsCalls(calls []mock.Call) [][]string {
	out := [][]string{}
	for _, c := range calls {
		entry := []string{c.Method}
		for _, arg := range c.Arguments {
			switch v := arg.(type) {
			case string:
				entry = append(entry, v)
			case int64, uint64:
				entry = append(entry, fmt.Sprint(v))
			}
		}
		out = append(out, entry)
	}
	return out
}

func init() {
	msgConfigOps[ds.Config]("ds.Config.", ds.NewConfig, func(c ds.Config) string { return c.ToToml() })

	register("ds.EncodeOffset", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Offset uint64
			Epoch  string
		}](raw)
		if err != nil {
			return nil, err
		}
		return ds.EncodeOffset(a.Offset, a.Epoch), nil
	})
	register("ds.DecodeOffset", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Offset string }](raw)
		if err != nil {
			return nil, err
		}
		offset, epoch, derr := ds.DecodeOffset(a.Offset)
		return map[string]interface{}{"offset": offset, "epoch": epoch, "error": errString(derr)}, nil
	})
	// (&ds.Stream{Params: &ds.StreamParams{Cursor: c}}).NextCursor(), Calls times on
	// the same stream.
	register("ds.Stream.NextCursor", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Cursor string
			Calls  int
		}](raw)
		if err != nil {
			return nil, err
		}
		stream := &ds.Stream{Params: &ds.StreamParams{Cursor: a.Cursor}}
		out := []string{}
		for i := 0; i < a.Calls || i == 0; i++ {
			out = append(out, stream.NextCursor())
		}
		return out, nil
	})
	register("ds.PollEncoder.ID", func(json.RawMessage) (interface{}, error) {
		return ds.PollEncoder{}.ID(), nil
	})
	register("ds.PollEncoder.Encode", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Msg encodedMessage }](raw)
		if err != nil {
			return nil, err
		}
		msg, err := a.Msg.message()
		if err != nil {
			return nil, err
		}
		return toFrameResult(ds.PollEncoder{}.Encode(msg)), nil
	})
	register("ds.SSEEncoder.Encode", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Encoder ds.SSEEncoder
			Msg     encodedMessage
		}](raw)
		if err != nil {
			return nil, err
		}
		msg, err := a.Msg.message()
		if err != nil {
			return nil, err
		}
		return toFrameResult(a.Encoder.Encode(msg)), nil
	})
	register("ds.SSEEncoder.EncodeTransmission", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Encoder ds.SSEEncoder
			Raw     string
		}](raw)
		if err != nil {
			return nil, err
		}
		return toFrameResult(a.Encoder.EncodeTransmission(a.Raw)), nil
	})
	// Replays the steps on a fresh recorder and connection; one result per step.
	register("ds.PollConnection", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Steps []pollConnectionStep }](raw)
		if err != nil {
			return nil, err
		}
		w := httptest.NewRecorder()
		var conn *ds.PollConnection
		etagCalls := []string{}
		get := func() *ds.PollConnection {
			if conn == nil {
				conn = ds.NewPollConnection(w)
			}
			return conn
		}
		results := []pollConnectionResult{}
		for _, s := range a.Steps {
			r := pollConnectionResult{}
			switch s.Op {
			case "preset_header":
				w.Header().Set(s.Name, s.Value)
			case "cache_control":
				get().CacheControl = s.Value
			case "etag":
				prefix, suffix := s.Prefix, s.Suffix
				get().ETag = func(nextOffset string) string {
					etagCalls = append(etagCalls, nextOffset)
					return prefix + nextOffset + suffix
				}
			case "write":
				r.Error = errString(get().Write(s.Msg, time.Time{}))
			case "close":
				get().Close(s.Code, s.Reason)
			case "context_done":
				select {
				case <-get().Context().Done():
					r.Bool = true
				default:
				}
			case "state":
				r.Code = w.Code
				r.Headers = w.Header().Clone()
				r.Body = w.Body.String()
				r.ETagCalls = append([]string{}, etagCalls...)
			default:
				return nil, fmt.Errorf("unknown poll connection op %q", s.Op)
			}
			results = append(results, r)
		}
		return results, nil
	})

	register("ds.Harness.New", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Broker       string // mock | memory
			Epoch        string
			HistoryTTL   int64
			HistoryLimit int
		}](raw)
		if err != nil {
			return nil, err
		}
		controller := &mocks.Controller{}
		controller.On("Shutdown").Return(nil)
		h := &dsHarness{controller: controller}
		config := node.NewConfig()
		config.HubGopoolSize = 2
		start := func() {
			go func() {
				defer func() {
					if r := recover(); r != nil {
						h.recordPanic(r)
					}
				}()
				h.n.Start() // nolint: errcheck
			}()
		}
		switch a.Broker {
		case "mock": // buildNode()
			h.n = node.NewNode(&config, node.WithController(controller), node.WithInstrumenter(metrics.NewMetrics(nil, 10, quietLog)))
			start()
			brk := &mocks.Broker{}
			brk.On("Peak", "test-stream").Return(nil, nil)
			h.mockBroker = brk
			h.n.SetBroker(brk)
		case "memory": // setupIntegrationServer() / auth_test.go
			config.DisconnectMode = node.DISCONNECT_MODE_NEVER
			h.n = node.NewNode(&config, node.WithController(controller), node.WithInstrumenter(metrics.NewMetrics(nil, 10, quietLog)))
			h.n.SetDisconnector(node.NewNoopDisconnector())
			bconf := broker.NewConfig()
			if a.HistoryTTL > 0 {
				bconf.HistoryTTL = a.HistoryTTL
			}
			if a.HistoryLimit > 0 {
				bconf.HistoryLimit = a.HistoryLimit
			}
			brk := broker.NewMemoryBroker(pubsub.NewLegacySubscriber(h.n), h.n, &bconf)
			brk.SetEpoch(a.Epoch)
			h.n.SetBroker(brk)
			if err := brk.Start(nil); err != nil {
				return nil, err
			}
			h.memBroker = brk
			start()
		default:
			return nil, fmt.Errorf("unknown broker kind %q", a.Broker)
		}
		return msgPut(h), nil
	})
	register("ds.Harness.ExpectController", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		a, err := msgArgs[struct{ Expectation dsControllerExpectation }](raw)
		if err != nil {
			return nil, err
		}
		e := a.Expectation
		var sid interface{} = mock.Anything
		if e.Sid != nil {
			sid = *e.Sid
		}
		var call *mock.Call
		switch e.Method {
		case "Shutdown":
			call = h.controller.On("Shutdown").Return(nil)
		case "Authenticate":
			call = h.controller.On("Authenticate", mock.Anything, sid, mock.Anything).Return(e.ConnectResult, nil)
		case "Subscribe":
			var identifier interface{} = mock.Anything
			if e.IdentifierContains != nil {
				part := *e.IdentifierContains
				identifier = mock.MatchedBy(func(channel string) bool { return strings.Contains(channel, part) })
			}
			call = h.controller.On("Subscribe", mock.Anything, sid, mock.Anything, mock.Anything, identifier).Return(e.CommandResult, nil)
		default:
			return nil, fmt.Errorf("unknown controller expectation %q", e.Method)
		}
		switch e.Times {
		case 0:
		case 1:
			call.Once()
		case 2:
			call.Twice()
		default:
			call.Times(e.Times)
		}
		if e.Maybe {
			call.Maybe()
		}
		return nil, nil
	})
	// controller.ExpectedCalls = []*mock.Call{}
	register("ds.Harness.ResetController", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		h.controller.ExpectedCalls = []*mock.Call{}
		return nil, nil
	})
	register("ds.Harness.ExpectBroker", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		if h.mockBroker == nil {
			return nil, errors.New("not a mock broker harness")
		}
		a, err := msgArgs[struct{ Expectation dsBrokerExpectation }](raw)
		if err != nil {
			return nil, err
		}
		e := a.Expectation
		var rerr error
		if e.Error != nil {
			rerr = errors.New(*e.Error)
		}
		var history interface{}
		if e.History != nil {
			history = *e.History
		}
		switch e.Method {
		case "Peak":
			var msg interface{}
			if e.Message != nil {
				msg = e.Message
			}
			h.mockBroker.On("Peak", e.Stream).Return(msg, rerr)
		case "HistorySince":
			h.mockBroker.On("HistorySince", e.Stream, e.Ts).Return(history, rerr)
		case "HistoryFrom":
			h.mockBroker.On("HistoryFrom", e.Stream, e.Epoch, e.Offset).Return(history, rerr)
		default:
			return nil, fmt.Errorf("unknown broker expectation %q", e.Method)
		}
		return nil, nil
	})
	// Controller and mock broker calls (method plus string/integer arguments) and
	// the recovered testify failures.
	register("ds.Harness.Calls", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		brokerCalls := [][]string{}
		if h.mockBroker != nil {
			brokerCalls = dsCalls(h.mockBroker.Calls)
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		return map[string]interface{}{
			"controller": dsCalls(h.controller.Calls),
			"broker":     brokerCalls,
			"unexpected": append([]string{}, h.panics...),
		}, nil
	})
	// brk.HandleBroadcast(&common.StreamMessage{Stream, Data}) on the memory broker.
	register("ds.Harness.Broadcast", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		a, err := msgArgs[struct{ Stream, Data string }](raw)
		if err != nil {
			return nil, err
		}
		if h.memBroker == nil {
			return nil, errors.New("not a memory broker harness")
		}
		return errString(h.memBroker.HandleBroadcast(&common.StreamMessage{Stream: a.Stream, Data: a.Data})), nil
	})
	register("ds.Harness.BrokerSubscribe", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		a, err := msgArgs[struct{ Stream string }](raw)
		if err != nil {
			return nil, err
		}
		return h.broker().Subscribe(a.Stream), nil
	})
	register("ds.Harness.Size", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		return h.n.Size(), nil
	})
	// Whether a session the controller was asked to subscribe is now subscribed to
	// Stream: n.LookupSession(identifiers).ToCacheEntry() lists the channel's
	// streams, which the node records right after subscribing the session in the
	// hub. The barrier replacing upstream's sleeps before broadcasting.
	register("ds.Harness.Subscribed", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		a, err := msgArgs[struct{ Stream string }](raw)
		if err != nil {
			return nil, err
		}
		for _, c := range h.controller.Calls {
			if c.Method != "Subscribe" || len(c.Arguments) < 4 {
				continue
			}
			ids, _ := c.Arguments[3].(string)
			s := h.n.LookupSession(ids)
			if s == nil {
				continue
			}
			cached, err := s.ToCacheEntry()
			if err != nil {
				continue
			}
			var entry struct {
				Subs map[string][]string `json:"subs"`
			}
			if err := json.Unmarshal(cached, &entry); err != nil {
				return nil, err
			}
			for _, streams := range entry.Subs {
				for _, st := range streams {
					if st == a.Stream {
						return true, nil
					}
				}
			}
		}
		return false, nil
	})
	register("ds.Harness.Shutdown", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		return errString(h.n.Shutdown(context.Background())), nil
	})

	// DSHandler(n, brk, streamCtrl, nil, shutdownCtx, &server.DefaultHeadersExtractor{}, &conf, logger)
	register("ds.Harness.Handler", func(raw json.RawMessage) (interface{}, error) {
		h, err := dsHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		a, err := msgArgs[struct {
			Config  ds.Config
			Streams *dsStreamsKind
		}](raw)
		if err != nil {
			return nil, err
		}
		o := &dsHandlerObj{harness: h, resolved: []dsResolved{}}
		var ctrl *streams.Controller
		if a.Streams != nil {
			if ctrl, err = o.streamsController(*a.Streams); err != nil {
				return nil, err
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		conf := a.Config
		handler, herr := ds.DSHandler(h.n, h.broker(), ctrl, nil, ctx, &server.DefaultHeadersExtractor{}, &conf, quietLog)
		if herr != nil {
			cancel()
			return map[string]interface{}{"handler": nil, "nil_handler": handler == nil, "error": errString(herr)}, nil
		}
		o.h = handler
		o.shutdown = cancel
		return map[string]interface{}{"handler": msgPut(o), "nil_handler": handler == nil, "error": nil}, nil
	})
	// handler.ServeHTTP(httptest.NewRecorder(), req) on its own goroutine; the
	// result is collected with ds.Serve.Wait (short, repeated waits).
	register("ds.Handler.Serve", func(raw json.RawMessage) (interface{}, error) {
		o, err := dsHandlerGet(raw)
		if err != nil {
			return nil, err
		}
		a, err := msgArgs[struct {
			Method, Target string
			Headers        [][2]string
		}](raw)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest(a.Method, a.Target, nil)
		if err != nil {
			return nil, err
		}
		for _, kv := range a.Headers {
			req.Header.Set(kv[0], kv[1])
		}
		s := &dsServeObj{w: httptest.NewRecorder(), done: make(chan struct{})}
		go func() {
			defer close(s.done)
			defer func() {
				if r := recover(); r != nil {
					msg := fmt.Sprint(r)
					s.panicMsg = &msg
					o.harness.recordPanic(r)
				}
			}()
			o.h.ServeHTTP(s.w, req)
		}()
		return msgPut(s), nil
	})
	register("ds.Serve.Wait", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Serve     int64
			TimeoutMs int64
		}](raw)
		if err != nil {
			return nil, err
		}
		s, err := msgGet[*dsServeObj](a.Serve)
		if err != nil {
			return nil, err
		}
		select {
		case <-s.done:
		case <-time.After(msgTimeout(a.TimeoutMs)):
			return map[string]interface{}{"done": false}, nil
		}
		return map[string]interface{}{
			"done":    true,
			"status":  s.w.Code,
			"headers": s.w.Header(),
			"body":    s.w.Body.String(),
			"panic":   s.panicMsg,
		}, nil
	})
	// mux.Handle(conf.Path+"/", handler); httptest.NewServer(mux). A panic in the
	// handler (a testify failure) is recorded and re-raised, so net/http aborts the
	// response as it does upstream.
	register("ds.Handler.Listen", func(raw json.RawMessage) (interface{}, error) {
		o, err := dsHandlerGet(raw)
		if err != nil {
			return nil, err
		}
		a, err := msgArgs[struct{ Pattern string }](raw)
		if err != nil {
			return nil, err
		}
		mux := http.NewServeMux()
		mux.Handle(a.Pattern, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if rec != http.ErrAbortHandler {
						o.harness.recordPanic(rec)
					}
					panic(rec)
				}
			}()
			o.h.ServeHTTP(w, r)
		}))
		ts := httptest.NewUnstartedServer(mux)
		ts.Config.ErrorLog = log.New(io.Discard, "", 0)
		ts.Start()
		o.server = ts
		return map[string]interface{}{"url": ts.URL}, nil
	})
	register("ds.Handler.Resolved", func(raw json.RawMessage) (interface{}, error) {
		o, err := dsHandlerGet(raw)
		if err != nil {
			return nil, err
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		return append([]dsResolved{}, o.resolved...), nil
	})
	// ts.Close() (CloseClientConnections first: Close waits for active requests).
	register("ds.Handler.Close", func(raw json.RawMessage) (interface{}, error) {
		o, err := dsHandlerGet(raw)
		if err != nil {
			return nil, err
		}
		if o.server != nil {
			o.server.CloseClientConnections()
			go o.server.Close()
		}
		return nil, nil
	})
}
