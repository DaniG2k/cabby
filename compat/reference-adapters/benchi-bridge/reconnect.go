package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/anycable/anycable-go/etc/benchi/lib"
)

func init() {
	// --- rpc_server.go ---
	brRegister("NewRPCServer", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			LatencyNanos, ConnectLatencyNanos, CommandLatencyNanos, DisconnectLatencyNanos int64
			Jitter                                                                         float64
			// SessionClosed is a caller-side callback id (func(sid string) bool).
			SessionClosed *uint64
		}](raw)
		if err != nil {
			return nil, err
		}
		cfg := lib.RPCServerConfig{
			Latency:           time.Duration(a.LatencyNanos),
			ConnectLatency:    time.Duration(a.ConnectLatencyNanos),
			CommandLatency:    time.Duration(a.CommandLatencyNanos),
			DisconnectLatency: time.Duration(a.DisconnectLatencyNanos),
			Jitter:            a.Jitter,
		}
		if a.SessionClosed != nil {
			fn := *a.SessionClosed
			cfg.SessionClosed = func(sid string) bool {
				// A panic here would be swallowed by net/http; fail the adapter instead.
				var closed bool
				if err := json.Unmarshal(brCallback(fn, sid), &closed); err != nil {
					brFatal("SessionClosed callback reply: %v", err)
				}
				return closed
			}
		}
		rpc := lib.NewRPCServer(cfg)
		return struct {
			H   uint64
			URL string
		}{brPut(rpc), rpc.URL()}, nil
	})
	method("RPCServer.Close", func(s *lib.RPCServer) (interface{}, error) { s.Close(); return nil, nil })
	brRegister("RPCServer.Stats", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H      uint64
			Method int
		}](raw)
		if err != nil {
			return nil, err
		}
		s, err := brGet[*lib.RPCServer](a.H)
		if err != nil {
			return nil, err
		}
		return s.Stats(lib.RPCMethod(a.Method)), nil
	})

	// --- reconnecting_client.go ---
	brRegister("NewClientStats", func(json.RawMessage) (interface{}, error) { return brPut(&lib.ClientStats{}), nil })
	stat := func(name string, f func(*lib.ClientStats, int) int64) {
		brRegister(name, func(raw json.RawMessage) (interface{}, error) {
			a, err := decode[struct {
				H   uint64
				Arg int
			}](raw)
			if err != nil {
				return nil, err
			}
			s, err := brGet[*lib.ClientStats](a.H)
			if err != nil {
				return nil, err
			}
			return f(s, a.Arg), nil
		})
	}
	stat("ClientStats.State", func(s *lib.ClientStats, st int) int64 { return s.State(lib.ClientState(st)) })
	stat("ClientStats.Reason", func(s *lib.ClientStats, r int) int64 { return s.Reason(lib.CloseReason(r)) })
	stat("ClientStats.Attempts", func(s *lib.ClientStats, _ int) int64 { return s.Attempts() })
	stat("ClientStats.Reconnects", func(s *lib.ClientStats, _ int) int64 { return s.Reconnects() })
	stat("ClientStats.SubscribeRetries", func(s *lib.ClientStats, _ int) int64 { return s.SubscribeRetries() })
	brRegister("ClientStats.SessionClosed", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H   uint64
			Sid string
		}](raw)
		if err != nil {
			return nil, err
		}
		s, err := brGet[*lib.ClientStats](a.H)
		if err != nil {
			return nil, err
		}
		return s.SessionClosed(a.Sid), nil
	})
	brRegister("ClientStats.Histogram", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H    uint64
			Name string
		}](raw)
		if err != nil {
			return nil, err
		}
		s, err := brGet[*lib.ClientStats](a.H)
		if err != nil {
			return nil, err
		}
		switch a.Name {
		case "Welcome":
			return snapshot(s.Welcome.Snapshot()), nil
		case "Confirm":
			return snapshot(s.Confirm.Snapshot()), nil
		case "Healthy":
			return snapshot(s.Healthy.Snapshot()), nil
		case "Probe":
			return snapshot(s.Probe.Snapshot()), nil
		}
		return nil, fmt.Errorf("unknown ClientStats histogram %q", a.Name)
	})

	brRegister("NewReconnectingClient", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			URL                         string
			Channels                    []string
			SequentialSubscribe         bool
			SubscribeRetryIntervalNanos int64
			SubscribeRetriesMax         int
			PingTimeoutNanos            int64
			ProbeIntervalNanos          int64
			BackoffMinNanos             int64
			BackoffMaxNanos             int64
			Stats                       uint64
			Seed                        uint64
		}](raw)
		if err != nil {
			return nil, err
		}
		stats, err := brGet[*lib.ClientStats](a.Stats)
		if err != nil {
			return nil, err
		}
		return brPut(lib.NewReconnectingClient(lib.ReconnectingClientConfig{
			URL:                    a.URL,
			Channels:               a.Channels,
			SequentialSubscribe:    a.SequentialSubscribe,
			SubscribeRetryInterval: time.Duration(a.SubscribeRetryIntervalNanos),
			SubscribeRetriesMax:    a.SubscribeRetriesMax,
			PingTimeout:            time.Duration(a.PingTimeoutNanos),
			ProbeInterval:          time.Duration(a.ProbeIntervalNanos),
			Backoff:                lib.Backoff{Min: time.Duration(a.BackoffMinNanos), Max: time.Duration(a.BackoffMaxNanos)},
			Stats:                  stats,
			Seed:                   a.Seed,
		})), nil
	})
	brRegister("ReconnectingClient.Run", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H, Ctx       uint64
			InitialNanos int64
		}](raw)
		if err != nil {
			return nil, err
		}
		c, err := brGet[*lib.ReconnectingClient](a.H)
		if err != nil {
			return nil, err
		}
		ctx, err := brGet[*cancelCtx](a.Ctx)
		if err != nil {
			return nil, err
		}
		c.Run(ctx.ctx, time.Duration(a.InitialNanos))
		return nil, nil
	})
	method("ReconnectingClient.Drop", func(c *lib.ReconnectingClient) (interface{}, error) { return c.Drop(), nil })
}
