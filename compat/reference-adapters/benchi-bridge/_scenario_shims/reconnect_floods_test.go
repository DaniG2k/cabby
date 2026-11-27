// Test-only shim for scenarios/reconnect_floods (a minimal test-only
// adapter for private package state). It is never written into the reference
// checkout: `go test -c -overlay` adds it, with ../protocol.go, to the scenario's
// package main as extra _test.go files, so the unexported run, parseFlags,
// runWithConfig and commandStall can be called. Upstream files are unchanged.
//
// With CABBY_BENCHI_SHIM=1 the test binary serves the adapter protocol on stdin
// instead of running tests. Ops only marshal.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("CABBY_BENCHI_SHIM") == "1" {
		brServe()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type shimOutput struct {
	Code           int
	Stdout, Stderr string
}

func init() {
	brRegister("Run", func(raw json.RawMessage) (interface{}, error) {
		a, err := brDecode[struct{ Args []string }](raw)
		if err != nil {
			return nil, err
		}
		var stdout, stderr bytes.Buffer
		code := run(a.Args, &stdout, &stderr)
		return shimOutput{code, stdout.String(), stderr.String()}, nil
	})
	// parseFlags + runWithConfig with a context that is done after TimeoutNanos
	// (upstream TestRun_InterruptPrintsSummary).
	brRegister("RunWithTimeout", func(raw json.RawMessage) (interface{}, error) {
		a, err := brDecode[struct {
			Args         []string
			TimeoutNanos int64
		}](raw)
		if err != nil {
			return nil, err
		}
		var stdout, stderr bytes.Buffer
		cfg, err := parseFlags(a.Args, &stderr)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(a.TimeoutNanos))
		defer cancel()
		code := runWithConfig(ctx, cfg, &stdout, &stderr)
		return shimOutput{code, stdout.String(), stderr.String()}, nil
	})
	brRegister("ParseFlags", func(raw json.RawMessage) (interface{}, error) {
		a, err := brDecode[struct{ Args []string }](raw)
		if err != nil {
			return nil, err
		}
		cfg, err := parseFlags(a.Args, &bytes.Buffer{})
		if err != nil {
			s := err.Error()
			return struct{ Error *string }{&s}, nil
		}
		return struct {
			Waves           []int
			S               int
			RPCLatencyNanos int64
		}{cfg.waves, cfg.s, int64(cfg.rpcLatency)}, nil
	})
	brRegister("CommandStall", func(raw json.RawMessage) (interface{}, error) {
		a, err := brDecode[struct {
			Samples []struct {
				AtNanos int64
				States  [4]int64
				RPCDone [3]int64
			}
		}](raw)
		if err != nil {
			return nil, err
		}
		samples := make([]sample, len(a.Samples))
		for i, s := range a.Samples {
			samples[i] = sample{at: time.Duration(s.AtNanos), states: s.States, rpcDone: s.RPCDone}
		}
		cur, longest := commandStall(samples)
		return struct{ CurrentNanos, LongestNanos int64 }{int64(cur), int64(longest)}, nil
	})
}
