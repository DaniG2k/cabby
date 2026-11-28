package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/api"
	"github.com/anycable/anycable-go/broker"
	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/server"
)

// scriptedBroker is a double of broker.Broker (upstream tests: mocks.Broker).
// Only SupportsPresence and PresenceInfo are scripted; any other call is
// recorded (and rejected by the Rust mock on replay).
type scriptedBroker struct{ s *scripted }

var _ broker.Broker = (*scriptedBroker)(nil)

func (b *scriptedBroker) other(method string) { b.s.answer(method, struct{}{}) }

func (b *scriptedBroker) Start(chan error) error         { b.other("Start"); return nil }
func (b *scriptedBroker) Shutdown(context.Context) error { b.other("Shutdown"); return nil }
func (b *scriptedBroker) Announce() string               { b.other("Announce"); return "" }
func (b *scriptedBroker) HandleBroadcast(*common.StreamMessage) error {
	b.other("HandleBroadcast")
	return nil
}
func (b *scriptedBroker) HandleCommand(*common.RemoteCommandMessage) error {
	b.other("HandleCommand")
	return nil
}
func (b *scriptedBroker) Subscribe(string) string   { b.other("Subscribe"); return "" }
func (b *scriptedBroker) Unsubscribe(string) string { b.other("Unsubscribe"); return "" }
func (b *scriptedBroker) HistoryFrom(string, string, uint64) ([]common.StreamMessage, error) {
	b.other("HistoryFrom")
	return nil, nil
}
func (b *scriptedBroker) HistorySince(string, int64) ([]common.StreamMessage, error) {
	b.other("HistorySince")
	return nil, nil
}
func (b *scriptedBroker) CommitSession(string, broker.Cacheable) error {
	b.other("CommitSession")
	return nil
}
func (b *scriptedBroker) RestoreSession(string) ([]byte, error) {
	b.other("RestoreSession")
	return nil, nil
}
func (b *scriptedBroker) TouchSession(string) error { b.other("TouchSession"); return nil }
func (b *scriptedBroker) DeleteSession(string) error { b.other("DeleteSession"); return nil }
func (b *scriptedBroker) SupportsPresence() bool {
	r := decodeScriptedReturn(b.s.answer("SupportsPresence", struct{}{}))
	var v bool
	if !r.isNil() {
		if err := json.Unmarshal(r.Result, &v); err != nil {
			panic(err)
		}
	}
	return v
}
func (b *scriptedBroker) PresenceAdd(string, string, string, interface{}) (*common.PresenceEvent, error) {
	b.other("PresenceAdd")
	return nil, nil
}
func (b *scriptedBroker) PresenceRemove(string, string) (*common.PresenceEvent, error) {
	b.other("PresenceRemove")
	return nil, nil
}

// PresenceInfo matches on the stream and the number of options (upstream:
// mockery unrolls the variadic, so `On("PresenceInfo", "a", mock.Anything)`
// matches exactly one option of any value).
func (b *scriptedBroker) PresenceInfo(stream string, opts ...broker.PresenceInfoOption) (*common.PresenceInfo, error) {
	r := decodeScriptedReturn(b.s.answer("PresenceInfo", map[string]interface{}{"stream": stream, "options": len(opts)}))
	if r.isNil() {
		return nil, r.err()
	}
	var info common.PresenceInfo
	if err := json.Unmarshal(r.Result, &info); err != nil {
		panic(err)
	}
	return &info, r.err()
}
func (b *scriptedBroker) TouchPresence(string) error { b.other("TouchPresence"); return nil }
func (b *scriptedBroker) Peak(string) (*common.StreamMessage, error) {
	b.other("Peak")
	return nil, nil
}

// scriptedHandler is a double of broadcast.Handler (upstream tests: mocks.Handler).
type scriptedHandler struct{ s *scripted }

func (h *scriptedHandler) HandleBroadcast(payload []byte) error {
	return decodeScriptedReturn(h.s.answer("HandleBroadcast", map[string]interface{}{"payload": string(payload)})).err()
}

func (h *scriptedHandler) HandlePubSub(payload []byte) {
	h.s.answer("HandlePubSub", map[string]interface{}{"payload": string(payload)})
}

type httpRequestArg struct {
	Method  string      `json:"method"`
	Path    string      `json:"path"`
	Headers [][2]string `json:"headers"`
	Body    string      `json:"body"`
}

func (a httpRequestArg) build() (*http.Request, error) {
	req, err := http.NewRequest(a.Method, a.Path, strings.NewReader(a.Body))
	if err != nil {
		return nil, err
	}
	for _, h := range a.Headers {
		req.Header.Add(h[0], h[1])
	}
	return req, nil
}

func flattenHeaders(h http.Header) [][2]string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := [][2]string{}
	for _, k := range keys {
		for _, v := range h[k] {
			out = append(out, [2]string{k, v})
		}
	}
	return out
}

func init() {
	register("api.NewConfig", func(json.RawMessage) (interface{}, error) {
		return api.NewConfig(), nil
	})
	register("api.Config.IsSecured", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config api.Config }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return a.Config.IsSecured(), nil
	})
	register("api.Config.DeriveSecret", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config api.Config }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config
		err := conf.DeriveSecret()
		return map[string]interface{}{"config": conf, "error": errString(err)}, nil
	})
	register("api.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config api.Config }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return a.Config.ToToml(), nil
	})
	register("api.Config.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config api.Config
			Toml   string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config
		_, err := toml.Decode(a.Toml, &conf)
		return map[string]interface{}{"config": conf, "error": errString(err)}, nil
	})
	register("api.NewAuthenticator", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Secret string }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		auth, err := api.NewAuthenticator(a.Secret)
		if err != nil {
			return map[string]interface{}{"error": errString(err)}, nil
		}
		return map[string]interface{}{"is_enabled": auth.IsEnabled(), "secret": auth.Secret(), "error": nil}, nil
	})
	register("api.Authenticator.Authenticate", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Secret  string
			Request httpRequestArg
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		auth, err := api.NewAuthenticator(a.Secret)
		if err != nil {
			return nil, err
		}
		req := httptest.NewRequest(a.Request.Method, a.Request.Path, strings.NewReader(a.Request.Body))
		for _, h := range a.Request.Headers {
			req.Header.Add(h[0], h[1])
		}
		return auth.Authenticate(req), nil
	})
	register("api.NewServer", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config api.Config }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config
		_, err := api.NewServer(&conf, &scriptedBroker{newScripted(nil)}, &scriptedHandler{newScripted(nil)}, slog.Default())
		return map[string]interface{}{"config": conf, "error": errString(err)}, nil
	})
	// api.Server: NewServer + Start, then (Action "serve") one request through
	// the HTTP server Start registered the handlers on, then Shutdown.
	register("api.Server", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config  api.Config
			Broker  []scriptedExpectation
			Handler []scriptedExpectation
			Action  string
			Request *httpRequestArg
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		brk := &scriptedBroker{newScripted(a.Broker)}
		handler := &scriptedHandler{newScripted(a.Handler)}
		reply := map[string]interface{}{"broker_calls": brk.s.calls, "handler_calls": handler.s.calls}

		conf := a.Config
		srv, err := api.NewServer(&conf, brk, handler, slog.Default())
		if err != nil {
			reply["error"] = errString(err)
			return reply, nil
		}
		if err := srv.Start(); err != nil {
			reply["error"] = errString(err)
			return reply, nil
		}
		defer srv.Shutdown(context.Background()) // nolint:errcheck

		if a.Action == "serve" {
			if a.Request == nil {
				return nil, fmt.Errorf("serve needs a request")
			}
			if conf.Host != "" && conf.Host != server.Host {
				return nil, fmt.Errorf("api.Server bridge: a separate API host is not supported")
			}
			// Start registers on server.ForPort(port) when no separate host is set;
			// this is the server upstream tests reach as `api.server`.
			httpSrv, err := server.ForPort(strconv.Itoa(conf.Port))
			if err != nil {
				return nil, err
			}
			req, err := a.Request.build()
			if err != nil {
				return nil, err
			}
			rr := httptest.NewRecorder()
			httpSrv.ServeHTTP(rr, req)
			reply["status"] = rr.Code
			reply["headers"] = flattenHeaders(rr.Header())
			reply["body"] = rr.Body.String()
		}
		reply["broker_calls"] = brk.s.calls
		reply["handler_calls"] = handler.s.calls
		return reply, nil
	})
}
