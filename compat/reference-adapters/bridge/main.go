// Command bridge is a test-only reference adapter (SPEC.md §5 "Internal units").
// It exposes exported functions of the pinned Go implementation over a versioned
// JSON-lines protocol on stdin/stdout so translated Rust tests can run against Go.
// It must only marshal arguments and results: no behavior is reimplemented here.
//
// Request:  {"v":1,"op":"<Name>","args":{...}}
// Response: {"v":1,"ok":<result>} or {"v":1,"err":"<message>"} (Go error values are
// returned inside results, never as bridge errors).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/anycable/anycable-go/common"
)

const version = 1

type request struct {
	V    int             `json:"v"`
	Op   string          `json:"op"`
	Args json.RawMessage `json:"args"`
}

type response struct {
	V   int         `json:"v"`
	Ok  interface{} `json:"ok,omitempty"`
	Err string      `json:"err,omitempty"`
}

// sessionEnv mirrors common.SessionEnv; pointer fields keep nil vs empty distinct.
type sessionEnv struct {
	URL             string                        `json:"url"`
	Headers         *map[string]string            `json:"headers"`
	Identifiers     string                        `json:"identifiers"`
	ConnectionState *map[string]string            `json:"connection_state"`
	ChannelStates   *map[string]map[string]string `json:"channel_states"`
}

func (e sessionEnv) common() *common.SessionEnv {
	return &common.SessionEnv{URL: e.URL, Headers: e.Headers, Identifiers: e.Identifiers,
		ConnectionState: e.ConnectionState, ChannelStates: e.ChannelStates}
}

// parsed carries a Go (result, error) pair.
type parsed struct {
	Result interface{} `json:"result"`
	Error  *string     `json:"error"`
}

func errString(err error) *string {
	if err == nil {
		return nil
	}
	s := err.Error()
	return &s
}

// op handles one bridge request: unmarshal args, call the exported Go function,
// return its result.
type op func(args json.RawMessage) (interface{}, error)

// ops is the registry; each upstream package registers its ops from its own file
// (bridge/<pkg>.go) in init(), so translations can be added independently.
var ops = map[string]op{
	"Version": func(json.RawMessage) (interface{}, error) { return version, nil },
}

func register(name string, f op) {
	if _, dup := ops[name]; dup {
		panic("duplicate bridge op " + name)
	}
	ops[name] = f
}

func handle(name string, raw json.RawMessage) (interface{}, error) {
	f, ok := ops[name]
	if !ok {
		return nil, fmt.Errorf("unknown op %q", name)
	}
	return f(raw)
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 64<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		var req request
		resp := response{V: version}
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			resp.Err = "bad request: " + err.Error()
		} else if req.V != version {
			resp.Err = fmt.Sprintf("unsupported bridge version %d", req.V)
		} else if res, err := handle(req.Op, req.Args); err != nil {
			resp.Err = err.Error()
		} else {
			resp.Ok = res
		}
		if err := out.Encode(resp); err != nil {
			os.Exit(1)
		}
	}
}
