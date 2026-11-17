package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/anycable/anycable-go/cli"
	"github.com/anycable/anycable-go/config"
	anycable "github.com/anycable/anycable-go/etc/benchi/client"
	"github.com/anycable/anycable-go/etc/benchi/lib"
)

// h is the common argument shape of methods: the receiver's handle.
type h struct{ H uint64 }

func decode[T any](raw json.RawMessage) (T, error) {
	var a T
	err := json.Unmarshal(raw, &a)
	return a, err
}

// method registers an op taking only a receiver handle.
func method[T any](name string, f func(T) (interface{}, error)) {
	register(name, func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[h](raw)
		if err != nil {
			return nil, err
		}
		recv, err := get[T](a.H)
		if err != nil {
			return nil, err
		}
		return f(recv)
	})
}

func errText(err error) *string {
	if err == nil {
		return nil
	}
	s := err.Error()
	return &s
}

// --- Go values the tests need as plain inputs ---

type cancelCtx struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type sample struct {
	TNanos int64 `json:"t_nanos"`
	N      int64 `json:"n"`
}

func toSamples(in []sample) []lib.ReceiveSample {
	if in == nil {
		return nil
	}
	out := make([]lib.ReceiveSample, len(in))
	for i, s := range in {
		out[i] = lib.ReceiveSample{T: time.Unix(0, s.TNanos), N: s.N}
	}
	return out
}

func fromSamples(in []lib.ReceiveSample) []sample {
	out := make([]sample, len(in))
	for i, s := range in {
		out[i] = sample{TNanos: s.T.UnixNano(), N: s.N}
	}
	return out
}

type snapshotResult struct {
	H        uint64 `json:"h"`
	Count    int64  `json:"count"`
	MaxNanos int64  `json:"max_nanos"`
}

func snapshot(s lib.HistogramSnapshot) snapshotResult {
	return snapshotResult{H: put(s), Count: s.Count, MaxNanos: int64(s.Max)}
}

func init() {
	// Linkage: referencing cli.Embedded and the forked client proves the bridge
	// links both modules (TestModuleLinks, TestClientPackageCompiles).
	register("ModuleLinks", func(json.RawMessage) (interface{}, error) {
		var _ *cli.Embedded
		var _ *anycable.Client
		return true, nil
	})
	register("cable.NewClient", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct{ URL string }](raw)
		if err != nil {
			return nil, err
		}
		c := anycable.NewClient(context.Background(), a.URL, slog.New(slog.NewTextHandler(io.Discard, nil)))
		return put(c), nil
	})

	register("context.WithCancel", func(json.RawMessage) (interface{}, error) {
		ctx, cancel := context.WithCancel(context.Background())
		return put(&cancelCtx{ctx, cancel}), nil
	})
	method("context.Cancel", func(c *cancelCtx) (interface{}, error) { c.cancel(); return nil, nil })

	register("atomic.NewInt64", func(json.RawMessage) (interface{}, error) { return put(&atomic.Int64{}), nil })
	register("atomic.Store", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H uint64
			V int64
		}](raw)
		if err != nil {
			return nil, err
		}
		n, err := get[*atomic.Int64](a.H)
		if err != nil {
			return nil, err
		}
		n.Store(a.V)
		return nil, nil
	})

	// --- counter.go ---
	register("NewAccumulator", func(json.RawMessage) (interface{}, error) { return put(lib.NewAccumulator()), nil })
	register("Accumulator.Bump", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H  uint64
			ID string
		}](raw)
		if err != nil {
			return nil, err
		}
		acc, err := get[*lib.Accumulator](a.H)
		if err != nil {
			return nil, err
		}
		acc.Bump(a.ID)
		return nil, nil
	})
	method("Accumulator.Snapshot", func(a *lib.Accumulator) (interface{}, error) { return a.Snapshot(), nil })
	method("Accumulator.TotalReceived", func(a *lib.Accumulator) (interface{}, error) { return a.TotalReceived(), nil })

	// --- throughput.go ---
	register("NewReceiveSampler", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			IntervalNanos int64
			Source        uint64
		}](raw)
		if err != nil {
			return nil, err
		}
		n, err := get[*atomic.Int64](a.Source)
		if err != nil {
			return nil, err
		}
		return put(lib.NewReceiveSampler(time.Duration(a.IntervalNanos), n.Load)), nil
	})
	method("ReceiveSampler.Start", func(s *lib.ReceiveSampler) (interface{}, error) { s.Start(); return nil, nil })
	method("ReceiveSampler.Stop", func(s *lib.ReceiveSampler) (interface{}, error) { s.Stop(); return nil, nil })
	method("ReceiveSampler.Samples", func(s *lib.ReceiveSampler) (interface{}, error) {
		return fromSamples(s.Samples()), nil
	})
	register("Overall", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct{ Samples []sample }](raw)
		if err != nil {
			return nil, err
		}
		return lib.Overall(toSamples(a.Samples)), nil
	})
	register("ComputeWindowStats", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			Samples     []sample
			WindowNanos int64
		}](raw)
		if err != nil {
			return nil, err
		}
		return lib.ComputeWindowStats(toSamples(a.Samples), time.Duration(a.WindowNanos)), nil
	})

	// --- histogram.go ---
	register("NewHistogram", func(json.RawMessage) (interface{}, error) { return put(&lib.Histogram{}), nil })
	register("Histogram.Record", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64
			Nanos int64
		}](raw)
		if err != nil {
			return nil, err
		}
		hist, err := get[*lib.Histogram](a.H)
		if err != nil {
			return nil, err
		}
		hist.Record(time.Duration(a.Nanos))
		return nil, nil
	})
	method("Histogram.Snapshot", func(x *lib.Histogram) (interface{}, error) { return snapshot(x.Snapshot()), nil })
	method("Histogram.Swap", func(x *lib.Histogram) (interface{}, error) { return snapshot(x.Swap()), nil })
	register("HistogramSnapshot.Quantile", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H uint64
			Q float64
		}](raw)
		if err != nil {
			return nil, err
		}
		s, err := get[lib.HistogramSnapshot](a.H)
		if err != nil {
			return nil, err
		}
		return int64(s.Quantile(a.Q)), nil
	})

	// --- server.go ---
	register("BuildServer", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			BrokerAdapter string
			RPCHost       string
			Tuning        tuning
		}](raw)
		if err != nil {
			return nil, err
		}
		cfg := lib.ServerConfig{BrokerAdapter: a.BrokerAdapter, RPCHost: a.RPCHost}
		if !a.Tuning.empty() {
			cfg.Configure = a.Tuning.apply
		}
		srv, err := lib.BuildServer(cfg)
		if err != nil {
			return struct{ Error *string }{errText(err)}, nil
		}
		return struct {
			H            uint64
			WebSocketURL string
			BroadcastURL string
		}{put(srv), srv.WebSocketURL(), srv.BroadcastURL()}, nil
	})
	method("Server.Shutdown", func(s *lib.Server) (interface{}, error) {
		return errText(s.Shutdown(context.Background())), nil
	})
	method("Server.DisconnectQueueSize", func(s *lib.Server) (interface{}, error) { return s.DisconnectQueueSize(), nil })

	// --- client.go ---
	register("BuildClient", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct{ URL string }](raw)
		if err != nil {
			return nil, err
		}
		c, err := lib.BuildClient(a.URL)
		if err != nil {
			return struct{ Error *string }{errText(err)}, nil
		}
		return struct{ H uint64 }{put(c)}, nil
	})
	register("Client.Connect", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H            uint64
			TimeoutNanos int64
		}](raw)
		if err != nil {
			return nil, err
		}
		c, err := get[*lib.Client](a.H)
		if err != nil {
			return nil, err
		}
		ctx := context.Background()
		if a.TimeoutNanos > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(a.TimeoutNanos))
			defer cancel()
		}
		return errText(c.Connect(ctx)), nil
	})
	register("Client.Subscribe", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H       uint64
			Streams []string
		}](raw)
		if err != nil {
			return nil, err
		}
		c, err := get[*lib.Client](a.H)
		if err != nil {
			return nil, err
		}
		return errText(c.Subscribe(a.Streams)), nil
	})
	method("Client.Receive", func(c *lib.Client) (interface{}, error) {
		msg, ok := c.Receive()
		return struct {
			Stream, Data string
			Ok           bool
		}{msg.Stream, msg.Data, ok}, nil
	})
	method("Client.Close", func(c *lib.Client) (interface{}, error) { c.Close(); return nil, nil })

	// --- pool.go ---
	register("BuildPool", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			ServerURL   string
			C, S, BigS  int
			Tolerance   int
			Seed        uint64
			Accumulator *uint64
			Streams     [][]string
		}](raw)
		if err != nil {
			return nil, err
		}
		cfg := lib.PoolConfig{ServerURL: a.ServerURL, C: a.C, S: a.S, BigS: a.BigS, Tolerance: a.Tolerance,
			Seed: a.Seed, Streams: a.Streams}
		if a.Accumulator != nil {
			if cfg.Accumulator, err = get[*lib.Accumulator](*a.Accumulator); err != nil {
				return nil, err
			}
		}
		pool, err := lib.BuildPool(cfg)
		if err != nil {
			return struct {
				Error *string
				IsNil bool
			}{errText(err), pool == nil}, nil
		}
		return struct{ H uint64 }{put(pool)}, nil
	})
	method("ClientPool.Size", func(p *lib.ClientPool) (interface{}, error) { return p.Size(), nil })
	method("ClientPool.Streams", func(p *lib.ClientPool) (interface{}, error) { return p.Streams(), nil })
	method("ClientPool.Close", func(p *lib.ClientPool) (interface{}, error) { p.Close(); return nil, nil })
	register("BuildStreamSubsets", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			Seed       uint64
			C, S, BigS int
		}](raw)
		if err != nil {
			return nil, err
		}
		return lib.BuildStreamSubsets(a.Seed, a.C, a.S, a.BigS), nil
	})

	// --- publisher.go ---
	register("NewPublisher", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			URL         string
			MaxInflight int
			Workers     *int
			BatchSize   *int
		}](raw)
		if err != nil {
			return nil, err
		}
		var opts []lib.Option
		if a.Workers != nil {
			opts = append(opts, lib.WithWorkers(*a.Workers))
		}
		if a.BatchSize != nil {
			opts = append(opts, lib.WithBatchSize(*a.BatchSize))
		}
		return put(lib.NewPublisher(a.URL, a.MaxInflight, opts...)), nil
	})
	register("Publisher.Publish", func(raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H            uint64
			Stream, Data string
		}](raw)
		if err != nil {
			return nil, err
		}
		p, err := get[*lib.Publisher](a.H)
		if err != nil {
			return nil, err
		}
		p.Publish(a.Stream, a.Data)
		return nil, nil
	})
	method("Publisher.IssuedCount", func(p *lib.Publisher) (interface{}, error) { return p.IssuedCount(), nil })
	method("Publisher.DroppedCount", func(p *lib.Publisher) (interface{}, error) { return p.DroppedCount(), nil })
	method("Publisher.CompletedCount", func(p *lib.Publisher) (interface{}, error) { return p.CompletedCount(), nil })
	method("Publisher.TargetCounts", func(p *lib.Publisher) (interface{}, error) { return p.TargetCounts(), nil })
	method("Publisher.Close", func(p *lib.Publisher) (interface{}, error) { p.Close(); return nil, nil })
}

// tuning is the declarative form of the ServerConfig.Configure hooks the tests use.
type tuning struct {
	PingInterval *int
}

func (t tuning) empty() bool { return t.PingInterval == nil }

func (t tuning) apply(c *config.Config) {
	if t.PingInterval != nil {
		c.App.PingInterval = *t.PingInterval
	}
}
