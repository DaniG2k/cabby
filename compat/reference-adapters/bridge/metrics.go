package main

// Bridge ops for upstream package metrics. Registries, counters, gauges and
// writers are kept as handles (rpcm_objects.go).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/metrics"
)

// signalWriter is the upstream test's IntervalWriter double: it records that
// Run was called so the Rust test can wait for it.
type signalWriter struct{ ran chan struct{} }

func (w *signalWriter) Run(interval int) error         { close(w.ran); return nil }
func (w *signalWriter) Write(m *metrics.Metrics) error { return nil }
func (w *signalWriter) Stop()                          {}

// rotationWriter turns the exported Run loop into a rotation the test can step.
// Run calls m.rotate() and then each writer's Write on the same goroutine, so a
// Write that blocks right after reporting is a barrier just after a rotation:
// nothing rotates again until the test releases it.
type rotationWriter struct {
	written chan struct{}
	proceed chan struct{}
}

func (w *rotationWriter) Run(interval int) error { return nil }
func (w *rotationWriter) Write(m *metrics.Metrics) error {
	w.written <- struct{}{}
	<-w.proceed
	return nil
}
func (w *rotationWriter) Stop() {}

var rotationWriters = struct {
	sync.Mutex
	m map[int]*rotationWriter
}{m: map[int]*rotationWriter{}}

// gaugeConcurrently is the upstream TestGaugeIncDec body: inc goroutines calling
// Inc and dec goroutines calling Dec at once, joined with a WaitGroup.
func gaugeConcurrently(g *metrics.Gauge, inc, dec int) {
	var wg sync.WaitGroup
	for i := 0; i < inc; i++ {
		wg.Add(1)
		go func() { g.Inc(); wg.Done() }()
	}
	for i := 0; i < dec; i++ {
		wg.Add(1)
		go func() { g.Dec(); wg.Done() }()
	}
	wg.Wait()
}

func init() {
	register("metrics.NewMetrics", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ RotateInterval int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return rpcmPut(metrics.NewMetrics(nil, a.RotateInterval, rpcmLogger())), nil
	})
	register("metrics.NewFromConfig", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config metrics.Config }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		m, err := metrics.NewFromConfig(&a.Config, rpcmLogger())
		if err != nil {
			return handleOrErr{Error: describeErr(err)}, nil
		}
		return handleOrErr{Handle: rpcmPut(m)}, nil
	})
	register("metrics.NewConfig", func(json.RawMessage) (interface{}, error) { return metrics.NewConfig(), nil })
	register("metrics.NewStatsdConfig", func(json.RawMessage) (interface{}, error) { return metrics.NewStatsdConfig(), nil })
	register("metrics.Config", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config metrics.Config
			Op     string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		switch a.Op {
		case "LogEnabled":
			return a.Config.LogEnabled(), nil
		case "HTTPEnabled":
			return a.Config.HTTPEnabled(), nil
		case "LogFormatterEnabled":
			return a.Config.LogFormatterEnabled(), nil
		case "ToToml":
			return a.Config.ToToml(), nil
		case "StatsdEnabled":
			return a.Config.Statsd.Enabled(), nil
		}
		return nil, fmt.Errorf("unknown metrics.Config op %q", a.Op)
	})
	register("metrics.Config.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Base metrics.Config
			Toml string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		c := a.Base
		_, err := toml.Decode(a.Toml, &c)
		return resultOrErr{c, describeErr(err)}, nil
	})

	register("metrics.NewCounter", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Name, Desc string }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return rpcmPut(metrics.NewCounter(a.Name, a.Desc)), nil
	})
	register("metrics.Counter", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Handle int
			Op     string
			N      uint64
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		c, err := rpcmGet[*metrics.Counter](a.Handle)
		if err != nil {
			return nil, err
		}
		switch a.Op {
		case "Name":
			return c.Name(), nil
		case "Desc":
			return c.Desc(), nil
		case "Value":
			return c.Value(), nil
		case "IntervalValue":
			return c.IntervalValue(), nil
		case "Inc":
			return c.Inc(), nil
		case "Add":
			return c.Add(a.N), nil
		case "UpdateDelta":
			c.UpdateDelta()
			return true, nil
		}
		return nil, fmt.Errorf("unknown metrics.Counter op %q", a.Op)
	})
	register("metrics.NewGauge", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Name, Desc string }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return rpcmPut(metrics.NewGauge(a.Name, a.Desc)), nil
	})
	register("metrics.Gauge", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Handle int
			Op     string
			Int    int
			U64    uint64
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		g, err := rpcmGet[*metrics.Gauge](a.Handle)
		if err != nil {
			return nil, err
		}
		switch a.Op {
		case "Name":
			return g.Name(), nil
		case "Desc":
			return g.Desc(), nil
		case "Value":
			return g.Value(), nil
		case "Set":
			g.Set(a.Int)
			return true, nil
		case "Set64":
			g.Set64(a.U64)
			return true, nil
		case "Inc":
			return g.Inc(), nil
		case "Dec":
			return g.Dec(), nil
		}
		return nil, fmt.Errorf("unknown metrics.Gauge op %q", a.Op)
	})

	register("metrics.Metrics", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Handle     int
			Op         string
			Name, Desc string
			Value      uint64
			Tags       map[string]string
			Writer     int
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		m, err := rpcmGet[*metrics.Metrics](a.Handle)
		if err != nil {
			return nil, err
		}
		switch a.Op {
		case "RegisterCounter":
			m.RegisterCounter(a.Name, a.Desc)
		case "RegisterGauge":
			m.RegisterGauge(a.Name, a.Desc)
		case "CounterIncrement":
			m.CounterIncrement(a.Name)
		case "CounterAdd":
			m.CounterAdd(a.Name, a.Value)
		case "GaugeIncrement":
			m.GaugeIncrement(a.Name)
		case "GaugeDecrement":
			m.GaugeDecrement(a.Name)
		case "GaugeSet":
			m.GaugeSet(a.Name, a.Value)
		case "DefaultTags":
			m.DefaultTags(a.Tags)
		case "Counter":
			if c := m.Counter(a.Name); c != nil {
				return rpcmPut(c), nil
			}
			return nil, nil
		case "Gauge":
			if g := m.Gauge(a.Name); g != nil {
				return rpcmPut(g), nil
			}
			return nil, nil
		case "EachCounter":
			hs := []int{}
			m.EachCounter(func(c *metrics.Counter) { hs = append(hs, rpcmPut(c)) })
			return hs, nil
		case "EachGauge":
			hs := []int{}
			m.EachGauge(func(g *metrics.Gauge) { hs = append(hs, rpcmPut(g)) })
			return hs, nil
		case "IntervalSnapshot":
			return m.IntervalSnapshot(), nil
		case "Prometheus":
			return m.Prometheus(), nil
		case "PrometheusHandler":
			req := httptest.NewRequest("GET", "/", nil)
			rr := httptest.NewRecorder()
			http.HandlerFunc(m.PrometheusHandler).ServeHTTP(rr, req)
			return map[string]interface{}{"code": rr.Code, "body": rr.Body.String()}, nil
		case "RegisterSignalWriter":
			w := &signalWriter{ran: make(chan struct{})}
			m.RegisterWriter(w)
			return rpcmPut(w), nil
		case "RegisterStatsdWriter":
			w, err := rpcmGet[*metrics.StatsdWriter](a.Writer)
			if err != nil {
				return nil, err
			}
			m.RegisterWriter(w)
		case "Run":
			return rpcmStart(func() interface{} { return resultOrErr{Error: describeErr(m.Run())} }), nil
		case "Shutdown":
			return resultOrErr{Error: describeErr(m.Shutdown(context.Background()))}, nil
		default:
			return nil, fmt.Errorf("unknown metrics.Metrics op %q", a.Op)
		}
		return true, nil
	})
	// Steps one rotation of a registry through the exported Run loop: the first
	// call registers a rotationWriter and starts Run; later calls release the
	// writer's previous Write. Returns once Run has rotated and called Write
	// again (the registry then stays frozen until the next step).
	register("metrics.Metrics.StepRotation", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Handle, TimeoutMs int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		m, err := rpcmGet[*metrics.Metrics](a.Handle)
		if err != nil {
			return nil, err
		}
		rotationWriters.Lock()
		w, running := rotationWriters.m[a.Handle]
		if !running {
			w = &rotationWriter{written: make(chan struct{}), proceed: make(chan struct{})}
			rotationWriters.m[a.Handle] = w
		}
		rotationWriters.Unlock()
		if running {
			w.proceed <- struct{}{}
		} else {
			m.RegisterWriter(w)
			go m.Run() // nolint: errcheck
		}
		select {
		case <-w.written:
			return true, nil
		case <-time.After(time.Duration(a.TimeoutMs) * time.Millisecond):
			return nil, fmt.Errorf("no rotation within %dms", a.TimeoutMs)
		}
	})
	register("metrics.Gauge.Concurrently", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Handle, Inc, Dec int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		g, err := rpcmGet[*metrics.Gauge](a.Handle)
		if err != nil {
			return nil, err
		}
		gaugeConcurrently(g, a.Inc, a.Dec)
		return true, nil
	})
	register("metrics.SignalWriter.AwaitRun", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Handle, TimeoutMs int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		w, err := rpcmGet[*signalWriter](a.Handle)
		if err != nil {
			return nil, err
		}
		select {
		case <-w.ran:
			return true, nil
		case <-time.After(time.Duration(a.TimeoutMs) * time.Millisecond):
			return false, nil
		}
	})

	register("metrics.NewStatsdWriter", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config metrics.StatsdConfig
			Tags   map[string]string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return rpcmPut(metrics.NewStatsdWriter(a.Config, a.Tags, rpcmLogger())), nil
	})
	register("metrics.StatsdWriter", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Handle, Metrics, Interval int
			Op                        string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		w, err := rpcmGet[*metrics.StatsdWriter](a.Handle)
		if err != nil {
			return nil, err
		}
		switch a.Op {
		case "Run":
			return resultOrErr{Error: describeErr(w.Run(a.Interval))}, nil
		case "Write":
			m, err := rpcmGet[*metrics.Metrics](a.Metrics)
			if err != nil {
				return nil, err
			}
			return resultOrErr{Error: describeErr(w.Write(m))}, nil
		case "Stop":
			w.Stop()
			return resultOrErr{}, nil
		}
		return nil, fmt.Errorf("unknown metrics.StatsdWriter op %q", a.Op)
	})
}
