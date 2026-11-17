package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/broker"
	"github.com/anycable/anycable-go/common"
	natsconfig "github.com/anycable/anycable-go/nats"
	"github.com/anycable/anycable-go/pubsub"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Stateful broker objects live here between calls, keyed by handle.
var (
	brokerObjects = map[uint64]interface{}{}
	brokerNextID  uint64
)

func brokerPut(o interface{}) uint64 {
	brokerNextID++
	brokerObjects[brokerNextID] = o
	return brokerNextID
}

func brokerGet[T any](id uint64) (T, error) {
	o, ok := brokerObjects[id].(T)
	if !ok {
		return o, fmt.Errorf("no %T with handle %d", o, id)
	}
	return o, nil
}

// noopBroadcaster stands in for the nil broadcaster upstream memory tests use:
// the facade stores messages through HandleBroadcast, which broadcasts them.
type noopBroadcaster struct{}

func (noopBroadcaster) Broadcast(*common.StreamMessage)               {}
func (noopBroadcaster) BroadcastCommand(*common.RemoteCommandMessage) {}
func (noopBroadcaster) Subscribe(string)                              {}
func (noopBroadcaster) Unsubscribe(string)                            {}

// recordingPresenter is upstream's MockPresenter with On(Anything, Anything).
type recordingPresenter struct{ calls []map[string]interface{} }

func (p *recordingPresenter) HandleJoin(stream string, msg *common.PresenceEvent) {
	p.calls = append(p.calls, map[string]interface{}{"method": "HandleJoin", "stream": stream, "event": msg})
}

func (p *recordingPresenter) HandleLeave(stream string, msg *common.PresenceEvent) {
	p.calls = append(p.calls, map[string]interface{}{"method": "HandleLeave", "stream": stream, "event": msg})
}

func (p *recordingPresenter) drain() []map[string]interface{} {
	if p == nil {
		return nil
	}
	out := p.calls
	p.calls = nil
	return out
}

type memoryObj struct {
	b         *broker.Memory
	presenter *recordingPresenter
}

type natsObj struct{ b *broker.NATS }

// fakeBroadcastHandler is upstream's FakeBroadastHandler (nats_test.go).
type fakeBroadcastHandler struct{}

func (fakeBroadcastHandler) Broadcast(*common.StreamMessage)                   {}
func (fakeBroadcastHandler) ExecuteRemoteCommand(*common.RemoteCommandMessage) {}

// bytesCacheable is upstream's TestCacheable.
type bytesCacheable struct{ data string }

func (c *bytesCacheable) ToCacheEntry() ([]byte, error) { return []byte(c.data), nil }

type brokerArgs struct {
	Stream      string          `json:"stream"`
	Data        string          `json:"data"`
	Epoch       string          `json:"epoch"`
	Offset      uint64          `json:"offset"`
	Ts          int64           `json:"ts"`
	TsUnixNanos *int64          `json:"ts_unix_nanos"`
	Sid         string          `json:"sid"`
	Pid         string          `json:"pid"`
	Info        interface{}     `json:"info"`
	TimeoutMs   int64           `json:"timeout_ms"`
	Msg         json.RawMessage `json:"msg"`
}

type brokerCall struct {
	ID     uint64     `json:"id"`
	Method string     `json:"method"`
	Args   brokerArgs `json:"args"`
}

func brokerReply(result interface{}, err error) map[string]interface{} {
	return map[string]interface{}{"result": result, "error": errString(err)}
}

func init() {
	register("broker.NewConfig", func(json.RawMessage) (interface{}, error) {
		return broker.NewConfig(), nil
	})
	register("broker.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config broker.Config }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return a.Config.ToToml(), nil
	})
	register("broker.Config.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config broker.Config
			Toml   string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config
		_, err := toml.Decode(a.Toml, &conf)
		return map[string]interface{}{"config": conf, "error": errString(err)}, nil
	})
	register("broker.Release", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Kind string `json:"kind"`
			ID   uint64 `json:"id"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		if n, ok := brokerObjects[a.ID].(*natsObj); ok {
			n.b.Shutdown(context.Background()) // nolint:errcheck
		}
		delete(brokerObjects, a.ID)
		return nil, nil
	})

	register("broker.NewStreamsTracker", func(json.RawMessage) (interface{}, error) {
		return brokerPut(broker.NewStreamsTracker()), nil
	})
	register("broker.StreamsTracker", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			ID     uint64 `json:"id"`
			Method string `json:"method"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		t, err := brokerGet[*broker.StreamsTracker](a.ID)
		if err != nil {
			return nil, err
		}
		// Counts callback invocations so the facade can replay each one.
		calls := 0
		mark := func() { calls++ }
		var result bool
		switch a.Method {
		case "Add":
			result = t.Add(a.Name, mark)
		case "Has":
			result = t.Has(a.Name)
		case "Remove":
			result = t.Remove(a.Name, mark)
		default:
			return nil, fmt.Errorf("unknown tracker method %q", a.Method)
		}
		return map[string]interface{}{"result": result, "calls": calls}, nil
	})

	register("broker.NewMemoryBroker", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config    broker.Config
			Presenter bool
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config
		obj := &memoryObj{}
		var presenter broker.Presenter
		if a.Presenter {
			obj.presenter = &recordingPresenter{}
			presenter = obj.presenter
		}
		obj.b = broker.NewMemoryBroker(noopBroadcaster{}, presenter, &conf)
		return brokerPut(obj), nil
	})
	register("broker.Memory", func(raw json.RawMessage) (interface{}, error) {
		var c brokerCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		obj, err := brokerGet[*memoryObj](c.ID)
		if err != nil {
			return nil, err
		}
		b, a := obj.b, c.Args
		var out map[string]interface{}
		switch c.Method {
		case "Add":
			msg := &common.StreamMessage{Stream: a.Stream, Data: a.Data}
			err := b.HandleBroadcast(msg)
			out = brokerReply(msg.Offset, err)
		case "GetEpoch":
			out = brokerReply(b.GetEpoch(), nil)
		case "SetEpoch":
			b.SetEpoch(a.Epoch)
			out = brokerReply(nil, nil)
		case "Store":
			ts := time.Time{}
			if a.TsUnixNanos != nil {
				ts = time.Unix(0, *a.TsUnixNanos)
			}
			out = brokerReply(b.Store(a.Stream, []byte(a.Data), a.Offset, ts))
		case "HistoryFrom":
			out = brokerReply(b.HistoryFrom(a.Stream, a.Epoch, a.Offset))
		case "HistorySince":
			out = brokerReply(b.HistorySince(a.Stream, a.Ts))
		case "CommitSession":
			out = brokerReply(nil, b.CommitSession(a.Sid, &bytesCacheable{a.Data}))
		case "RestoreSession":
			out = brokerReply(b.RestoreSession(a.Sid))
		case "TouchSession":
			out = brokerReply(nil, b.TouchSession(a.Sid))
		case "PresenceAdd":
			ev, err := b.PresenceAdd(a.Stream, a.Sid, a.Pid, a.Info)
			out = brokerReply(nilPtr(ev), err)
		case "PresenceRemove":
			ev, err := b.PresenceRemove(a.Stream, a.Sid)
			out = brokerReply(nilPtr(ev), err)
		case "PresenceInfo":
			info, err := b.PresenceInfo(a.Stream)
			out = brokerReply(nilPtr(info), err)
		case "TouchPresence":
			out = brokerReply(nil, b.TouchPresence(a.Sid))
		case "Peak":
			msg, err := b.Peak(a.Stream)
			out = brokerReply(nilPtr(msg), err)
		default:
			return nil, fmt.Errorf("unknown memory broker method %q", c.Method)
		}
		out["presenter_calls"] = obj.presenter.drain()
		return out, nil
	})

	register("broker.NewNATSConfig", func(json.RawMessage) (interface{}, error) {
		return natsconfig.NewNATSConfig(), nil
	})
	register("broker.NewNATSBroker", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config      broker.Config
			NATS        natsconfig.NATSConfig
			Broadcaster bool
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf, nconf := a.Config, a.NATS
		var bro broker.Broadcaster
		if a.Broadcaster {
			bro = pubsub.NewLegacySubscriber(fakeBroadcastHandler{})
		}
		return brokerPut(&natsObj{broker.NewNATSBroker(bro, &conf, &nconf, slog.Default())}), nil
	})
	register("broker.NATS", func(raw json.RawMessage) (interface{}, error) {
		var c brokerCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		obj, err := brokerGet[*natsObj](c.ID)
		if err != nil {
			return nil, err
		}
		b, a := obj.b, c.Args
		switch c.Method {
		case "Start":
			return brokerReply(nil, b.Start(nil)), nil
		case "Shutdown":
			return brokerReply(nil, b.Shutdown(context.Background())), nil
		case "Ready":
			return brokerReply(nil, b.Ready(time.Duration(a.TimeoutMs)*time.Millisecond)), nil
		case "Reset":
			return brokerReply(nil, b.Reset()), nil
		case "Subscribe":
			return brokerReply(b.Subscribe(a.Stream), nil), nil
		case "Unsubscribe":
			return brokerReply(b.Unsubscribe(a.Stream), nil), nil
		case "HandleBroadcast":
			var msg common.StreamMessage
			if err := json.Unmarshal(a.Msg, &msg); err != nil {
				return nil, err
			}
			return brokerReply(nil, b.HandleBroadcast(&msg)), nil
		case "HistorySince":
			return brokerReply(b.HistorySince(a.Stream, a.Ts)), nil
		case "HistoryFrom":
			return brokerReply(b.HistoryFrom(a.Stream, a.Epoch, a.Offset)), nil
		case "Epoch":
			return brokerReply(b.Epoch(), nil), nil
		case "SetEpoch":
			return brokerReply(nil, b.SetEpoch(a.Epoch)), nil
		case "CommitSession":
			return brokerReply(nil, b.CommitSession(a.Sid, &bytesCacheable{a.Data})), nil
		case "RestoreSession":
			return brokerReply(b.RestoreSession(a.Sid)), nil
		case "TouchSession":
			return brokerReply(nil, b.TouchSession(a.Sid)), nil
		}
		return nil, fmt.Errorf("unknown NATS broker method %q", c.Method)
	})

	// Test tooling (upstream nats_test.go consumerSequence): reads stream
	// sequence numbers straight from JetStream; not a broker operation.
	register("broker.test.ConsumerSequence", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Servers string `json:"servers"`
			Stream  string `json:"stream"`
			Offset  uint64 `json:"offset"`
			N       int    `json:"n"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		nc, err := nats.Connect(a.Servers)
		if err != nil {
			return brokerReply(nil, err), nil
		}
		defer nc.Close()
		js, err := jetstream.New(nc)
		if err != nil {
			return brokerReply(nil, err), nil
		}
		conf := jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy}
		if a.Offset > 0 {
			conf.OptStartSeq = a.Offset
			conf.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		}
		// Upstream: streamPrefix + stream ("_ac_").
		cons, err := js.CreateConsumer(context.Background(), "_ac_"+a.Stream, conf)
		if err != nil {
			return brokerReply(nil, err), nil
		}
		seq := make(chan uint64, a.N)
		cc, err := cons.Consume(func(msg jetstream.Msg) {
			meta, _ := msg.Metadata()
			select {
			case seq <- meta.Sequence.Stream:
			default:
			}
		})
		if err != nil {
			return brokerReply(nil, err), nil
		}
		defer cc.Stop()
		out := make([]uint64, 0, a.N)
		for len(out) < a.N {
			select {
			case v := <-seq:
				out = append(out, v)
			case <-time.After(1 * time.Second):
				return brokerReply(nil, fmt.Errorf("timed out to read from consumer; received %d messages, expected %d", len(out), a.N)), nil
			}
		}
		return brokerReply(out, nil), nil
	})
}
