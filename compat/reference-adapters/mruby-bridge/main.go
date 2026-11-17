// Command mruby-bridge is a test-only reference adapter (SPEC.md §5 "Internal units")
// for the vendored go-mruby binding (compat/reference/vendorlib/go-mruby) and the
// upstream `mrb` package. It links libmruby.a through cgo, so it lives in its own
// module instead of the main bridge (compat/reference-adapters/bridge).
//
// It only marshals: every op unmarshals arguments, calls one exported go-mruby or
// `mrb` function and returns its result. Go objects (VMs, values, classes, parsers)
// stay in this process and are referred to by integer handles.
//
// Protocol (JSON lines, version 1):
//
//	request:  {"v":1,"s":<session>,"op":"<Name>","args":{...}}
//	response: {"v":1,"s":<session>,"ok":<result>} or {"v":1,"s":<session>,"err":"..."}
//	callback: {"v":1,"s":<session>,"cb":<id>,"args":{"mrb":<h>,"self":<h>}}
//
// A session is one Rust test thread. Each session runs on its own goroutine, so
// independent VMs run concurrently like the upstream goroutine tests. When a
// Ruby method defined from Rust is invoked, the session's goroutine sends a
// callback message and keeps serving that session's requests (nested calls made
// by the Rust callback) until it receives {"op":"Return","args":{"ret":..,"exc":..}}.
// "Version" and "EndSession" are handled by the main loop.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
)

const version = 1

type request struct {
	V    int             `json:"v"`
	S    uint64          `json:"s"`
	Op   string          `json:"op"`
	Args json.RawMessage `json:"args"`
}

type message struct {
	V    int         `json:"v"`
	S    uint64      `json:"s"`
	Ok   interface{} `json:"ok,omitempty"`
	Err  string      `json:"err,omitempty"`
	Cb   uint64      `json:"cb,omitempty"`
	Args interface{} `json:"args,omitempty"`
}

var (
	outMu sync.Mutex
	out   = json.NewEncoder(os.Stdout)
)

func send(m message) {
	m.V = version
	outMu.Lock()
	defer outMu.Unlock()
	if err := out.Encode(m); err != nil {
		os.Exit(1)
	}
}

// op handles one request of a session.
type op func(s *session, args json.RawMessage) (interface{}, error)

var ops = map[string]op{}

func register(name string, f op) {
	if _, dup := ops[name]; dup {
		panic("duplicate bridge op " + name)
	}
	ops[name] = f
}

type session struct {
	id uint64
	in chan request
}

func (s *session) run() {
	for req := range s.in {
		s.serve(req)
	}
}

func (s *session) serve(req request) {
	resp := message{S: s.id}
	f, ok := ops[req.Op]
	if !ok {
		resp.Err = fmt.Sprintf("unknown op %q", req.Op)
	} else if res, err := f(s, req.Args); err != nil {
		resp.Err = err.Error()
	} else {
		resp.Ok = res
	}
	send(resp)
}

// callback asks the Rust side to run callback `id` and serves nested requests
// until the matching Return arrives. It returns nil if the session ended.
func (s *session) callback(id uint64, args interface{}) json.RawMessage {
	send(message{S: s.id, Cb: id, Args: args})
	for req := range s.in {
		if req.Op == "Return" {
			return req.Args
		}
		s.serve(req)
	}
	return nil
}

// ---- handle registry: Go objects stay here, Rust holds integer handles ----

var reg = struct {
	sync.Mutex
	next uint64
	objs map[uint64]interface{}
	ids  map[interface{}]uint64
}{objs: map[uint64]interface{}{}, ids: map[interface{}]uint64{}}

// put returns the handle of o (nil for a nil pointer). The same pointer always
// gets the same handle, so Go pointer identity is observable from Rust.
func put(o interface{}) *uint64 {
	if o == nil {
		return nil
	}
	if rv := reflect.ValueOf(o); rv.Kind() == reflect.Ptr && rv.IsNil() {
		return nil
	}
	reg.Lock()
	defer reg.Unlock()
	if id, ok := reg.ids[o]; ok {
		return &id
	}
	reg.next++
	id := reg.next
	reg.objs[id] = o
	reg.ids[o] = id
	return &id
}

func get[T any](h uint64) (T, error) {
	reg.Lock()
	o, ok := reg.objs[h]
	reg.Unlock()
	t, isT := o.(T)
	if !ok || !isT {
		var zero T
		return zero, fmt.Errorf("handle %d is not a %T", h, zero)
	}
	return t, nil
}

// getOpt resolves an optional handle (nil -> zero value, e.g. a nil *Class).
func getOpt[T any](h *uint64) (T, error) {
	if h == nil {
		var zero T
		return zero, nil
	}
	return get[T](*h)
}

func decode[T any](raw json.RawMessage) (T, error) {
	var a T
	err := json.Unmarshal(raw, &a)
	return a, err
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 64<<20)
	sessions := map[uint64]*session{}
	var running sync.WaitGroup
	defer running.Wait()
	defer func() {
		for _, s := range sessions {
			close(s.in)
		}
	}()
	for in.Scan() {
		var req request
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			send(message{Err: "bad request: " + err.Error()})
			continue
		}
		if req.V != version {
			send(message{S: req.S, Err: fmt.Sprintf("unsupported bridge version %d", req.V)})
			continue
		}
		switch req.Op {
		case "Version":
			send(message{S: req.S, Ok: version})
		case "EndSession":
			if s, ok := sessions[req.S]; ok {
				close(s.in)
				delete(sessions, req.S)
			}
		default:
			s, ok := sessions[req.S]
			if !ok {
				s = &session{id: req.S, in: make(chan request, 16)}
				sessions[req.S] = s
				running.Add(1)
				go func() {
					defer running.Done()
					s.run()
				}()
			}
			s.in <- req
		}
	}
}
