//go:build (darwin && mrb) || (linux && mrb)

package main

// Ops for the upstream `mrb` package (built with the `mrb` tag, like upstream's
// mrb_test.go). Values returned by Eval share the go-mruby handle table.

import (
	"encoding/json"

	"github.com/anycable/anycable-go/mrb"
)

func init() {
	register("mrb.NewEngine", func(*session, json.RawMessage) (interface{}, error) {
		return put(mrb.NewEngine()), nil
	})
	type codeArgs struct {
		H    uint64 `json:"h"`
		Code string `json:"code"`
	}
	register("mrb.Engine.LoadString", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[codeArgs](raw)
		if err != nil {
			return nil, err
		}
		e, err := get[*mrb.Engine](a.H)
		if err != nil {
			return nil, err
		}
		return newFallible(nil, e.LoadString(a.Code)), nil
	})
	register("mrb.Engine.Eval", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[codeArgs](raw)
		if err != nil {
			return nil, err
		}
		e, err := get[*mrb.Engine](a.H)
		if err != nil {
			return nil, err
		}
		return newFallible(e.Eval(a.Code)), nil
	})
}
