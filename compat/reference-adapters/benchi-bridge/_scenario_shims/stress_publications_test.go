// Test-only shim for scenarios/stress_publications (SPEC.md §5: a minimal test-only
// adapter for private package state). It is never written into the reference
// checkout: `go test -c -overlay` adds it, with ../protocol.go, to the scenario's
// package main as extra _test.go files, so the upstream run(args, stdout, stderr,
// runOpts{...}) can be called with its test seams. Upstream files are unchanged.
//
// With CABBY_BENCHI_SHIM=1 the test binary serves the adapter protocol on stdin
// instead of running tests. Ops only marshal.
package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/anycable/anycable-go/etc/benchi/lib"
)

func TestMain(m *testing.M) {
	if os.Getenv("CABBY_BENCHI_SHIM") == "1" {
		brServe()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func init() {
	brRegister("Run", func(raw json.RawMessage) (interface{}, error) {
		a, err := brDecode[struct {
			Args          []string
			StreamSubsets [][]string
			BroadcastURL  string
			// SetupHook is a caller-side callback id, called with the pool handle.
			SetupHook *uint64
		}](raw)
		if err != nil {
			return nil, err
		}
		opts := runOpts{streamSubsets: a.StreamSubsets, broadcastURL: a.BroadcastURL}
		if a.SetupHook != nil {
			fn := *a.SetupHook
			opts.setupHook = func(_ *lib.Server, pool *lib.ClientPool, _ *lib.Publisher) {
				brCallback(fn, struct{ Pool uint64 }{brPut(pool)})
			}
		}
		var stdout, stderr bytes.Buffer
		code := run(a.Args, &stdout, &stderr, opts)
		return struct {
			Code           int
			Stdout, Stderr string
		}{code, stdout.String(), stderr.String()}, nil
	})
	brRegister("ClientPool.Clients", func(raw json.RawMessage) (interface{}, error) {
		a, err := brDecode[struct{ H uint64 }](raw)
		if err != nil {
			return nil, err
		}
		pool, err := brGet[*lib.ClientPool](a.H)
		if err != nil {
			return nil, err
		}
		hs := []uint64{}
		pool.Each(func(c *lib.Client) { hs = append(hs, brPut(c)) })
		return hs, nil
	})
	brRegister("Client.Close", func(raw json.RawMessage) (interface{}, error) {
		a, err := brDecode[struct{ H uint64 }](raw)
		if err != nil {
			return nil, err
		}
		c, err := brGet[*lib.Client](a.H)
		if err != nil {
			return nil, err
		}
		c.Close()
		return nil, nil
	})
}
