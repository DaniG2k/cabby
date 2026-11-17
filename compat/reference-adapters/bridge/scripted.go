package main

// Scripted test doubles for upstream interfaces (the bridge counterpart of the
// testify mocks upstream tests use). A double answers each call with the first
// expectation whose method and JSON-encoded arguments match, and records every
// call so the Rust side can assert on them. Unexpected calls get zero values and
// are reported like any other call; the Rust mock rejects them on replay.

import (
	"encoding/json"
	"reflect"

	"github.com/anycable/anycable-go/common"
)

type scriptedExpectation struct {
	Method  string          `json:"method"`
	Args    json.RawMessage `json:"args"`
	Returns json.RawMessage `json:"returns"`
}

type scriptedCall struct {
	Method string      `json:"method"`
	Args   interface{} `json:"args"`
}

type scripted struct {
	exps  []scriptedExpectation
	calls []scriptedCall
}

func newScripted(exps []scriptedExpectation) *scripted {
	return &scripted{exps: exps, calls: []scriptedCall{}}
}

func scriptedNormalize(v interface{}) interface{} {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return out
}

// answer records the call and returns the matching expectation's `returns`
// (nil when unexpected).
func (s *scripted) answer(method string, args interface{}) json.RawMessage {
	actual := scriptedNormalize(args)
	s.calls = append(s.calls, scriptedCall{method, actual})
	for _, e := range s.exps {
		if e.Method != method {
			continue
		}
		var expected interface{}
		if len(e.Args) > 0 {
			if err := json.Unmarshal(e.Args, &expected); err != nil {
				panic(err)
			}
		}
		if reflect.DeepEqual(expected, actual) {
			return e.Returns
		}
	}
	return nil
}

// scriptedReturn is a Go (*T, error) pair: {"result": ..., "error": "..."|null}.
type scriptedReturn struct {
	Result json.RawMessage `json:"result"`
	Error  *string         `json:"error"`
}

func decodeScriptedReturn(raw json.RawMessage) scriptedReturn {
	var r scriptedReturn
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &r); err != nil {
			panic(err)
		}
	}
	return r
}

func (r scriptedReturn) err() error {
	if r.Error == nil {
		return nil
	}
	return scriptedError(*r.Error)
}

func (r scriptedReturn) isNil() bool {
	return len(r.Result) == 0 || string(r.Result) == "null"
}

func (r scriptedReturn) connectResult() *common.ConnectResult {
	if r.isNil() {
		return nil
	}
	var res common.ConnectResult
	if err := json.Unmarshal(r.Result, &res); err != nil {
		panic(err)
	}
	return &res
}

func (r scriptedReturn) commandResult() *common.CommandResult {
	if r.isNil() {
		return nil
	}
	var res common.CommandResult
	if err := json.Unmarshal(r.Result, &res); err != nil {
		panic(err)
	}
	return &res
}

type scriptedError string

func (e scriptedError) Error() string { return string(e) }

// pairResult is what an op returns for a Go (*T, error) result.
type pairResult struct {
	Result interface{} `json:"result"`
	Error  *string     `json:"error"`
}

// bridgeEnvOf converts a *common.SessionEnv into the bridge's sessionEnv shape.
func bridgeEnvOf(env *common.SessionEnv) *sessionEnv {
	if env == nil {
		return nil
	}
	return &sessionEnv{URL: env.URL, Headers: env.Headers, Identifiers: env.Identifiers,
		ConnectionState: env.ConnectionState, ChannelStates: env.ChannelStates}
}

func (e *sessionEnv) commonOrNil() *common.SessionEnv {
	if e == nil {
		return nil
	}
	return e.common()
}
