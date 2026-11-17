// Protocol of the Benchi reference adapters (SPEC.md §5 "Internal units").
//
// This file is shared: benchi-bridge's main() serves it, and the scenario shims
// (scenario-shims/*_test.go) get it overlaid into the scenarios' package main
// together with their own ops. Every identifier is prefixed with `br` so it cannot
// collide with upstream code in those packages.
//
// Unlike the shared bridge, Benchi works with long-lived objects (servers, clients,
// publishers) and blocking calls (Receive, Run). So:
//
//   - every request carries an id and runs in its own goroutine; responses carry
//     the same id and may arrive out of order;
//   - objects live in a handle table: constructors return a handle, methods take it;
//   - Go func-typed parameters (RPCServerConfig.SessionClosed, runOpts.setupHook)
//     are marshaled as callbacks into the caller: the adapter writes
//     {"cb":<fn>,"call":<n>,"args":...} and blocks until the caller answers
//     {"reply":<n>,"ok":...} or {"reply":<n>,"err":"..."}. A failed, null or late
//     reply is fatal: the adapter prints it on stderr and exits, so the caller's
//     pending calls fail loudly (a panic inside a net/http handler would otherwise
//     be swallowed).
//
// Request:  {"v":1,"id":<n>,"op":"<Name>","args":{...}}
// Response: {"v":1,"id":<n>,"ok":<result>} or {"v":1,"id":<n>,"err":"<message>"}
// Go error values are returned inside results, never as adapter errors.
//
// It must only marshal arguments and results: no behavior is reimplemented here.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const brVersion = 1

// brCallbackDeadline bounds how long Go waits for a caller-side callback.
const brCallbackDeadline = 30 * time.Second

type brRequest struct {
	V     int             `json:"v"`
	ID    *uint64         `json:"id"`
	Op    string          `json:"op"`
	Args  json.RawMessage `json:"args"`
	Reply *uint64         `json:"reply"`
	Ok    json.RawMessage `json:"ok"`
	Err   *string         `json:"err"`
}

type brResponse struct {
	V   int         `json:"v"`
	ID  uint64      `json:"id"`
	Ok  interface{} `json:"ok,omitempty"`
	Err string      `json:"err,omitempty"`
}

type brCallbackRequest struct {
	V    int         `json:"v"`
	Cb   uint64      `json:"cb"`
	Call uint64      `json:"call"`
	Args interface{} `json:"args"`
}

type brOp func(args json.RawMessage) (interface{}, error)

var brOps = map[string]brOp{
	"Version": func(json.RawMessage) (interface{}, error) { return brVersion, nil },
	// Stderr.Sentinel writes text to stderr so the caller can tell when everything
	// written before it has been drained from the pipe.
	"Stderr.Sentinel": func(raw json.RawMessage) (interface{}, error) {
		a, err := brDecode[struct{ Text string }](raw)
		if err != nil {
			return nil, err
		}
		_, err = fmt.Fprint(os.Stderr, a.Text)
		return nil, err
	},
}

func brRegister(name string, f brOp) {
	if _, dup := brOps[name]; dup {
		panic("duplicate adapter op " + name)
	}
	brOps[name] = f
}

func brDecode[T any](raw json.RawMessage) (T, error) {
	var a T
	err := json.Unmarshal(raw, &a)
	return a, err
}

// brFatal reports an adapter failure and exits.
func brFatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "benchi adapter: "+format+"\n", args...)
	os.Exit(3)
}

// --- output, shared by responses and callbacks ---

var (
	brOutMu sync.Mutex
	brOut   = json.NewEncoder(os.Stdout)
)

func brEmit(v interface{}) {
	brOutMu.Lock()
	defer brOutMu.Unlock()
	if err := brOut.Encode(v); err != nil {
		os.Exit(1)
	}
}

// --- handles ---

var (
	brHandlesMu  sync.Mutex
	brHandles    = map[uint64]interface{}{}
	brNextHandle uint64
)

func brPut(v interface{}) uint64 {
	brHandlesMu.Lock()
	defer brHandlesMu.Unlock()
	brNextHandle++
	brHandles[brNextHandle] = v
	return brNextHandle
}

func brGet[T any](h uint64) (T, error) {
	brHandlesMu.Lock()
	v, ok := brHandles[h]
	brHandlesMu.Unlock()
	t, isT := v.(T)
	if !ok || !isT {
		var zero T
		return zero, fmt.Errorf("handle %d is not a %T", h, zero)
	}
	return t, nil
}

// --- callbacks into the caller ---

type brReply struct {
	ok  json.RawMessage
	err *string
}

var (
	brCallsMu  sync.Mutex
	brCalls    = map[uint64]chan brReply{}
	brNextCall atomic.Uint64
)

// brCallback invokes caller-side function fn with args and returns its JSON result,
// which is never null. Failures are fatal (see the package comment).
func brCallback(fn uint64, args interface{}) json.RawMessage {
	n := brNextCall.Add(1)
	ch := make(chan brReply, 1)
	brCallsMu.Lock()
	brCalls[n] = ch
	brCallsMu.Unlock()
	brEmit(brCallbackRequest{V: brVersion, Cb: fn, Call: n, Args: args})
	select {
	case r := <-ch:
		if r.err != nil {
			brFatal("callback %d failed: %s", fn, *r.err)
		}
		if len(r.ok) == 0 || string(r.ok) == "null" {
			brFatal("callback %d returned null", fn)
		}
		return r.ok
	case <-time.After(brCallbackDeadline):
		brFatal("callback %d did not answer within %s", fn, brCallbackDeadline)
	}
	return nil
}

func brHandle(req brRequest) {
	resp := brResponse{V: brVersion, ID: *req.ID}
	defer func() {
		if r := recover(); r != nil {
			resp.Ok = nil
			resp.Err = fmt.Sprintf("panic: %v", r)
			brEmit(resp)
		}
	}()
	f, ok := brOps[req.Op]
	if !ok {
		resp.Err = fmt.Sprintf("unknown op %q", req.Op)
	} else if res, err := f(req.Args); err != nil {
		resp.Err = err.Error()
	} else {
		resp.Ok = res
	}
	brEmit(resp)
}

// brServe answers requests on stdin until EOF.
func brServe() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 64<<20)
	for in.Scan() {
		var req brRequest
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			brFatal("bad request: %v", err)
		}
		if req.V != brVersion {
			brFatal("unsupported version %d", req.V)
		}
		switch {
		case req.Reply != nil:
			brCallsMu.Lock()
			ch := brCalls[*req.Reply]
			delete(brCalls, *req.Reply)
			brCallsMu.Unlock()
			if ch != nil {
				ch <- brReply{ok: req.Ok, err: req.Err}
			}
		case req.ID != nil:
			go brHandle(req)
		default:
			brFatal("request without id")
		}
	}
}
