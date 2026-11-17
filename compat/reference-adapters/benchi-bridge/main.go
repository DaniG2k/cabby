// Command benchi-bridge is a test-only reference adapter (SPEC.md §5 "Internal units")
// for Benchi (etc/benchi), a separate Go module that the shared bridge cannot import.
// It exposes exported Benchi functions and methods over a versioned JSON-lines
// protocol. It must only marshal arguments and results: no behavior is
// reimplemented here.
//
// Unlike the shared bridge, Benchi works with long-lived objects (servers, clients,
// publishers) and blocking calls (Receive, Run). So:
//
//   - every request carries an id and runs in its own goroutine; responses carry
//     the same id and may arrive out of order;
//   - objects live in a handle table: constructors return a handle, methods take it;
//   - Go func-typed parameters (RPCServerConfig.SessionClosed) are marshaled as
//     callbacks into the caller: the bridge writes {"cb":<fn>,"call":<n>,"args":...}
//     and blocks until the caller answers {"reply":<n>,"ok":...}.
//
// Each Rust test starts its own bridge process, so Benchi's one-server-per-process
// limit (cli.NewRunner mutates package globals) holds per test.
//
// Request:  {"v":1,"id":<n>,"op":"<Name>","args":{...}}
// Response: {"v":1,"id":<n>,"ok":<result>} or {"v":1,"id":<n>,"err":"<message>"}
// Go error values are returned inside results, never as bridge errors.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

const version = 1

type request struct {
	V     int             `json:"v"`
	ID    *uint64         `json:"id"`
	Op    string          `json:"op"`
	Args  json.RawMessage `json:"args"`
	Reply *uint64         `json:"reply"`
	Ok    json.RawMessage `json:"ok"`
}

type response struct {
	V   int         `json:"v"`
	ID  uint64      `json:"id"`
	Ok  interface{} `json:"ok,omitempty"`
	Err string      `json:"err,omitempty"`
}

type callbackRequest struct {
	V    int         `json:"v"`
	Cb   uint64      `json:"cb"`
	Call uint64      `json:"call"`
	Args interface{} `json:"args"`
}

type op func(args json.RawMessage) (interface{}, error)

var ops = map[string]op{
	"Version": func(json.RawMessage) (interface{}, error) { return version, nil },
}

func register(name string, f op) {
	if _, dup := ops[name]; dup {
		panic("duplicate bridge op " + name)
	}
	ops[name] = f
}

// --- output, shared by responses and callbacks ---

var (
	outMu sync.Mutex
	out   = json.NewEncoder(os.Stdout)
)

func emit(v interface{}) {
	outMu.Lock()
	defer outMu.Unlock()
	if err := out.Encode(v); err != nil {
		os.Exit(1)
	}
}

// --- handles ---

var (
	handlesMu  sync.Mutex
	handles    = map[uint64]interface{}{}
	nextHandle uint64
)

func put(v interface{}) uint64 {
	handlesMu.Lock()
	defer handlesMu.Unlock()
	nextHandle++
	handles[nextHandle] = v
	return nextHandle
}

func get[T any](h uint64) (T, error) {
	handlesMu.Lock()
	v, ok := handles[h]
	handlesMu.Unlock()
	t, isT := v.(T)
	if !ok || !isT {
		var zero T
		return zero, fmt.Errorf("handle %d is not a %T", h, zero)
	}
	return t, nil
}

// --- callbacks into the caller ---

var (
	callsMu  sync.Mutex
	calls    = map[uint64]chan json.RawMessage{}
	nextCall atomic.Uint64
)

// callback invokes caller-side function fn with args and returns its JSON result.
func callback(fn uint64, args interface{}) json.RawMessage {
	n := nextCall.Add(1)
	ch := make(chan json.RawMessage, 1)
	callsMu.Lock()
	calls[n] = ch
	callsMu.Unlock()
	emit(callbackRequest{V: version, Cb: fn, Call: n, Args: args})
	return <-ch
}

func handle(req request) {
	resp := response{V: version, ID: *req.ID}
	defer func() {
		if r := recover(); r != nil {
			resp.Ok = nil
			resp.Err = fmt.Sprintf("panic: %v", r)
			emit(resp)
		}
	}()
	f, ok := ops[req.Op]
	if !ok {
		resp.Err = fmt.Sprintf("unknown op %q", req.Op)
	} else if res, err := f(req.Args); err != nil {
		resp.Err = err.Error()
	} else {
		resp.Ok = res
	}
	emit(resp)
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 64<<20)
	for in.Scan() {
		var req request
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			fmt.Fprintln(os.Stderr, "benchi-bridge: bad request:", err)
			os.Exit(1)
		}
		if req.V != version {
			fmt.Fprintln(os.Stderr, "benchi-bridge: unsupported version", req.V)
			os.Exit(1)
		}
		switch {
		case req.Reply != nil:
			callsMu.Lock()
			ch := calls[*req.Reply]
			delete(calls, *req.Reply)
			callsMu.Unlock()
			if ch != nil {
				ch <- req.Ok
			}
		case req.ID != nil:
			go handle(req)
		default:
			fmt.Fprintln(os.Stderr, "benchi-bridge: request without id")
			os.Exit(1)
		}
	}
}
