package main

// The TestSSEHandler harness (sse/handler_test.go) behind handles: buildNode()
// (mocks.Controller, legacy broker, immediate disconnector) with the test's
// disconnect queue (rate 1), the node started; SSEHandler instances; requests
// served on their own goroutine through a copy of the test's streamingWriter.
// Expectations are programmed with On(...).Return(...) from JSON; testify panics
// on unexpected calls are recovered and reported, never fatal to the bridge.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/anycable/anycable-go/broker"
	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/metrics"
	"github.com/anycable/anycable-go/mocks"
	"github.com/anycable/anycable-go/node"
	"github.com/anycable/anycable-go/pubsub"
	"github.com/anycable/anycable-go/server"
	"github.com/anycable/anycable-go/sse"
	"github.com/stretchr/testify/mock"
)

type sseHarness struct {
	n          *node.Node
	controller *mocks.Controller
	mu         sync.Mutex
	panics     []string // testify failures (unexpected calls) recovered anywhere
}

func (h *sseHarness) recordPanic(r interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.panics = append(h.panics, fmt.Sprint(r))
}

type sseHandlerObj struct {
	h        http.Handler
	harness  *sseHarness
	shutdown context.CancelFunc
}

// streamingWriter is the upstream test helper: every Write is split into events at
// blank lines. statusSeen reports WriteHeader (the barrier replacing a sleep).
type streamingWriter struct {
	httptest.ResponseRecorder

	stream     chan []byte
	statusSeen chan int
}

func (w *streamingWriter) Write(data []byte) (int, error) {
	events := bytes.Split(data, []byte("\n\n"))
	for _, event := range events {
		if len(event) > 0 {
			w.stream <- event
		}
	}
	return w.ResponseRecorder.Write(data)
}

func (w *streamingWriter) WriteHeader(code int) {
	select {
	case w.statusSeen <- code:
	default:
	}
	w.ResponseRecorder.WriteHeader(code)
}

type sseServeObj struct {
	// original is upstream's `w`: the recorder the streaming writer copied.
	original *httptest.ResponseRecorder
	sw       *streamingWriter
	cancel   context.CancelFunc
	done     chan struct{}
	panicMsg *string
}

type sseExpectation struct {
	Method        string // Authenticate | Subscribe
	Sid           string
	Ids           string
	Identifier    string
	ConnectResult *common.ConnectResult
	CommandResult *common.CommandResult
}

func sseHarnessGet(raw json.RawMessage) (*sseHarness, json.RawMessage, error) {
	var a struct{ Harness int64 }
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, nil, err
	}
	h, err := msgGet[*sseHarness](a.Harness)
	return h, raw, err
}

func init() {
	register("sse.Harness.New", func(json.RawMessage) (interface{}, error) {
		controller := &mocks.Controller{}
		config := node.NewConfig()
		config.HubGopoolSize = 2
		n := node.NewNode(&config, node.WithController(controller), node.WithInstrumenter(metrics.NewMetrics(nil, 10, quietLog)))
		n.SetBroker(broker.NewLegacyBroker(pubsub.NewLegacySubscriber(n)))
		dconfig := node.NewDisconnectQueueConfig()
		dconfig.Rate = 1
		n.SetDisconnector(node.NewDisconnectQueue(n, &dconfig, quietLog))
		h := &sseHarness{n: n, controller: controller}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					h.recordPanic(r)
				}
			}()
			n.Start() // nolint: errcheck
		}()
		controller.On("Shutdown").Return(nil)
		controller.On("Disconnect", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		return msgPut(h), nil
	})
	register("sse.Harness.Expect", func(raw json.RawMessage) (interface{}, error) {
		h, raw, err := sseHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		var a struct{ Expectation sseExpectation }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		e := a.Expectation
		switch e.Method {
		case "Authenticate":
			h.controller.On("Authenticate", mock.Anything, e.Sid, mock.Anything).Return(e.ConnectResult, nil)
		case "Subscribe":
			h.controller.On("Subscribe", mock.Anything, e.Sid, mock.Anything, e.Ids, e.Identifier).Return(e.CommandResult, nil)
		default:
			return nil, fmt.Errorf("unknown expectation %q", e.Method)
		}
		return nil, nil
	})
	// Calls: every controller call (method and its string arguments) plus the
	// recovered testify failures.
	register("sse.Harness.Calls", func(raw json.RawMessage) (interface{}, error) {
		h, _, err := sseHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		calls := [][]string{}
		for _, c := range h.controller.Calls {
			entry := []string{c.Method}
			for _, arg := range c.Arguments {
				if s, ok := arg.(string); ok {
					entry = append(entry, s)
				}
			}
			calls = append(calls, entry)
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		return map[string]interface{}{"calls": calls, "unexpected": append([]string{}, h.panics...)}, nil
	})
	register("sse.Harness.Broadcast", func(raw json.RawMessage) (interface{}, error) {
		h, raw, err := sseHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		var a struct{ Stream, Data string }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		h.n.Broadcast(&common.StreamMessage{Stream: a.Stream, Data: a.Data})
		return nil, nil
	})
	register("sse.Harness.Size", func(raw json.RawMessage) (interface{}, error) {
		h, _, err := sseHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		return h.n.Size(), nil
	})
	register("sse.Harness.Shutdown", func(raw json.RawMessage) (interface{}, error) {
		h, _, err := sseHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		return nil, h.n.Shutdown(context.Background())
	})
	// SSEHandler(appNode, nil, shutdownCtx, &server.DefaultHeadersExtractor{}, &conf, logger);
	// the shutdown context is cancellable through sse.Handler.Shutdown.
	register("sse.Harness.Handler", func(raw json.RawMessage) (interface{}, error) {
		h, raw, err := sseHarnessGet(raw)
		if err != nil {
			return nil, err
		}
		var a struct{ Config sse.Config }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		conf := a.Config
		handler := sse.SSEHandler(h.n, nil, ctx, &server.DefaultHeadersExtractor{}, &conf, quietLog)
		return msgPut(&sseHandlerObj{h: handler, harness: h, shutdown: cancel}), nil
	})
	register("sse.Handler.Shutdown", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Handler int64 }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		o, err := msgGet[*sseHandlerObj](a.Handler)
		if err != nil {
			return nil, err
		}
		o.shutdown()
		return nil, nil
	})
	// go handler.ServeHTTP(sw, req) with the request context timing out after
	// TimeoutMs (upstream context.WithTimeout).
	register("sse.Handler.Serve", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Handler        int64
			Method, Target string
			Headers        map[string]string
			Body           *string
			TimeoutMs      int64
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		o, err := msgGet[*sseHandlerObj](a.Handler)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest(a.Method, a.Target, nil)
		if err != nil {
			return nil, err
		}
		for k, v := range a.Headers {
			req.Header.Set(k, v)
		}
		if a.Body != nil {
			req.Body = io.NopCloser(strings.NewReader(*a.Body))
		}
		ctx, cancel := context.WithTimeout(context.Background(), msgTimeout(a.TimeoutMs))
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()
		s := &sseServeObj{
			original: w,
			sw:       &streamingWriter{ResponseRecorder: *w, stream: make(chan []byte, 100), statusSeen: make(chan int, 1)},
			cancel:   cancel,
			done:     make(chan struct{}),
		}
		go func() {
			defer close(s.done)
			defer func() {
				if r := recover(); r != nil {
					msg := fmt.Sprint(r)
					s.panicMsg = &msg
					o.harness.recordPanic(r)
				}
			}()
			o.h.ServeHTTP(s.sw, req)
		}()
		return msgPut(s), nil
	})
	register("sse.Serve", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Serve     int64
			Op        string // read_event | wait_status | cancel | wait
			TimeoutMs int64
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		s, err := msgGet[*sseServeObj](a.Serve)
		if err != nil {
			return nil, err
		}
		timeout := time.After(msgTimeout(a.TimeoutMs))
		switch a.Op {
		case "read_event":
			// Buffered events first: select picks randomly among ready cases. The
			// facade calls with short timeouts so the shared bridge is never held.
			select {
			case e := <-s.sw.stream:
				return map[string]interface{}{"event": string(e)}, nil
			default:
			}
			select {
			case e := <-s.sw.stream:
				return map[string]interface{}{"event": string(e)}, nil
			case <-s.done:
				select {
				case e := <-s.sw.stream:
					return map[string]interface{}{"event": string(e)}, nil
				default:
				}
				return map[string]interface{}{"error": "handler returned"}, nil
			case <-timeout:
				return map[string]interface{}{"error": "context deadline exceeded", "timeout": true}, nil
			}
		case "wait_status":
			select {
			case code := <-s.sw.statusSeen:
				return map[string]interface{}{"status": code}, nil
			case <-timeout:
				return map[string]interface{}{"status": nil}, nil
			}
		case "cancel":
			s.cancel()
			return nil, nil
		case "original_code":
			// Upstream's `w`: nothing writes to it once the streaming copy exists.
			return s.original.Code, nil
		case "wait":
			select {
			case <-s.done:
			case <-timeout:
				return map[string]interface{}{"done": false}, nil
			}
			body := s.sw.Body.String()
			return map[string]interface{}{
				"done":          true,
				"status":        s.sw.Code,
				"original_code": s.original.Code,
				"headers":       s.sw.Header(),
				"body":          body,
				"panic":         s.panicMsg,
			}, nil
		}
		return nil, fmt.Errorf("unknown serve op %q", a.Op)
	})
	register("sse.NewConfig", func(json.RawMessage) (interface{}, error) {
		return sse.NewConfig(), nil
	})
}
