package main

// Object handles and asynchronous calls for the rpc, metrics and telemetry ops.
//
// Go values that live across bridge requests (controllers, metrics registries,
// contexts) are kept in a table and referred to by integer handles. Calls that
// must run concurrently with the test (held RPCs, cancellation) start in a
// goroutine and are collected with "rpcm.Await". This is lifecycle plumbing only:
// no upstream behavior is reimplemented.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/anycable/anycable-go/rpc"
	"google.golang.org/grpc/status"
)

var rpcmObjects = struct {
	sync.Mutex
	next int
	m    map[int]interface{}
}{m: map[int]interface{}{}}

func rpcmPut(v interface{}) int {
	rpcmObjects.Lock()
	defer rpcmObjects.Unlock()
	rpcmObjects.next++
	rpcmObjects.m[rpcmObjects.next] = v
	return rpcmObjects.next
}

func rpcmGet[T any](h int) (T, error) {
	rpcmObjects.Lock()
	defer rpcmObjects.Unlock()
	v, ok := rpcmObjects.m[h].(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("no object %d of type %T", h, zero)
	}
	return v, nil
}

func rpcmDrop(h int) {
	rpcmObjects.Lock()
	defer rpcmObjects.Unlock()
	delete(rpcmObjects.m, h)
}

// rpcmLogger keeps the reference quiet on stderr (stdout is the bridge channel).
func rpcmLogger() *slog.Logger {
	if os.Getenv("CABBY_BRIDGE_LOG") == "1" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// asyncCall is a call running in a goroutine.
type asyncCall struct {
	done   chan struct{}
	result interface{}
}

func rpcmStart(f func() interface{}) int {
	c := &asyncCall{done: make(chan struct{})}
	go func() {
		defer close(c.done)
		c.result = f()
	}()
	return rpcmPut(c)
}

// ctxEntry is a context created by the test (background, cancelled, with a
// deadline and/or incoming metadata) plus its cancel function.
type ctxEntry struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func rpcmCtx(h int) (context.Context, error) {
	if h == 0 {
		return context.Background(), nil
	}
	e, err := rpcmGet[*ctxEntry](h)
	if err != nil {
		return nil, err
	}
	return e.ctx, nil
}

// goError describes a Go error for the Rust facades: its text plus the
// classifications the upstream tests make (errors.Is, status.FromError).
type goError struct {
	Text        string  `json:"text"`
	WaitTimeout bool    `json:"wait_timeout"`
	Canceled    bool    `json:"canceled"`
	Code        *uint32 `json:"code"`
	Message     string  `json:"message"`
}

func describeErr(err error) *goError {
	if err == nil {
		return nil
	}
	e := &goError{Text: err.Error(), WaitTimeout: errors.Is(err, rpc.ErrWaitTimeout), Canceled: errors.Is(err, context.Canceled)}
	if st, ok := status.FromError(err); ok {
		code := uint32(st.Code())
		e.Code = &code
		e.Message = st.Message()
	}
	return e
}

func init() {
	register("rpcm.Await", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Call, TimeoutMs int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		c, err := rpcmGet[*asyncCall](a.Call)
		if err != nil {
			return nil, err
		}
		select {
		case <-c.done:
			rpcmDrop(a.Call)
			return c.result, nil
		case <-time.After(time.Duration(a.TimeoutMs) * time.Millisecond):
			return nil, fmt.Errorf("call %d did not finish within %dms", a.Call, a.TimeoutMs)
		}
	})
	register("rpcm.NewContext", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Canceled bool
			// Zero: no deadline.
			TimeoutMs int
			// Incoming metadata pairs (metadata.NewIncomingContext).
			Metadata [][2]string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		if a.TimeoutMs > 0 {
			var c2 context.CancelFunc
			ctx, c2 = context.WithTimeout(ctx, time.Duration(a.TimeoutMs)*time.Millisecond)
			outer := cancel
			cancel = func() { c2(); outer() }
		}
		if len(a.Metadata) > 0 {
			ctx = incomingMetadata(ctx, a.Metadata)
		}
		if a.Canceled {
			cancel()
		}
		return rpcmPut(&ctxEntry{ctx, cancel}), nil
	})
	register("rpcm.CancelContext", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Ctx int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		e, err := rpcmGet[*ctxEntry](a.Ctx)
		if err != nil {
			return nil, err
		}
		e.cancel()
		return true, nil
	})
	register("rpcm.Drop", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Handle int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		rpcmDrop(a.Handle)
		return true, nil
	})
}
