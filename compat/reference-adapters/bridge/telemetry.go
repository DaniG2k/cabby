package main

// Bridge ops for upstream package telemetry. The endpoint is always the Rust
// test's recording HTTP server; nothing reaches a real telemetry service.
//
// Upstream reads the process environment; the tests pass the variables they
// set (t.Setenv) with each op, and the bridge applies them for the duration of
// that op only (bridge requests are serialized, so this is not shared state
// between concurrent tests).

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/anycable/anycable-go/config"
	"github.com/anycable/anycable-go/metrics"
	"github.com/anycable/anycable-go/telemetry"
	anyversion "github.com/anycable/anycable-go/version"
)

// withEnv runs f with the given variables set and removes every other
// variable that could influence platform detection.
func withEnv(env map[string]string, f func() interface{}) interface{} {
	saved := os.Environ()
	os.Clearenv()
	for _, kv := range saved {
		// Keep the toolchain basics; drop anything platform/telemetry related.
		if k, v, ok := strings.Cut(kv, "="); ok && (k == "PATH" || k == "HOME" || k == "TMPDIR") {
			os.Setenv(k, v)
		}
	}
	for k, v := range env {
		os.Setenv(k, v)
	}
	defer func() {
		os.Clearenv()
		for _, kv := range saved {
			if k, v, ok := strings.Cut(kv, "="); ok {
				os.Setenv(k, v)
			}
		}
	}()
	return f()
}

type telemetryConfig struct {
	Token       string
	Endpoint    string
	CustomProps map[string]string
	Debug       bool
}

func init() {
	register("telemetry.NewConfig", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Env map[string]string }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return withEnv(a.Env, func() interface{} {
			c := telemetry.NewConfig()
			return telemetryConfig{c.Token, c.Endpoint, c.CustomProps, c.Debug}
		}), nil
	})
	register("telemetry.Version", func(json.RawMessage) (interface{}, error) { return anyversion.Version(), nil })
	register("telemetry.NewTracker", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Metrics int
			Config  telemetryConfig
			Env     map[string]string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		m, err := rpcmGet[*metrics.Metrics](a.Metrics)
		if err != nil {
			return nil, err
		}
		if a.Config.Endpoint == "" || strings.Contains(a.Config.Endpoint, "anycable.io") {
			return nil, fmt.Errorf("refusing a real telemetry endpoint: %q", a.Config.Endpoint)
		}
		return withEnv(a.Env, func() interface{} {
			conf := config.NewConfig()
			tc := &telemetry.Config{Token: a.Config.Token, Endpoint: a.Config.Endpoint,
				CustomProps: a.Config.CustomProps, Debug: a.Config.Debug}
			return rpcmPut(telemetry.NewTracker(m, &conf, tc))
		}), nil
	})
	register("telemetry.Tracker", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Handle int
			Op     string
			Env    map[string]string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		t, err := rpcmGet[*telemetry.Tracker](a.Handle)
		if err != nil {
			return nil, err
		}
		switch a.Op {
		case "Enabled":
			return t.Enabled(), nil
		case "Announce":
			return t.Announce(), nil
		case "Collect":
			return withEnv(a.Env, func() interface{} { t.Collect(); return true }), nil
		case "Shutdown":
			return resultOrErr{Error: describeErr(t.Shutdown(context.Background()))}, nil
		}
		return nil, fmt.Errorf("unknown telemetry.Tracker op %q", a.Op)
	})
}
