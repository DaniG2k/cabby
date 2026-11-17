package main

// Bridge ops for upstream package `node`. Real Go nodes, sessions, disconnect
// queues and subscription states are kept behind integer handles and driven
// through their exported API only. Objects are wired the way upstream's test
// helpers (node/node_mocks_test.go) wire them; nothing is reimplemented.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/broker"
	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/metrics"
	"github.com/anycable/anycable-go/mocks"
	"github.com/anycable/anycable-go/node"
	"github.com/anycable/anycable-go/pubsub"
)

var (
	nodeObjects = map[uint64]interface{}{}
	nodeNextID  uint64
)

func nodePut(o interface{}) uint64 {
	nodeNextID++
	nodeObjects[nodeNextID] = o
	return nodeNextID
}

func nodeGet[T any](id uint64) (T, error) {
	o, ok := nodeObjects[id].(T)
	if !ok {
		return o, fmt.Errorf("no %T with handle %d", o, id)
	}
	return o, nil
}

type nodeObj struct {
	n       *node.Node
	metrics *metrics.Metrics
}

// gatedConn is mocks.MockConnection whose writes wait until released: the
// bridge counterpart of upstream tests holding the session's write mutex.
type gatedConn struct {
	mocks.MockConnection
	arrived chan struct{}
	release chan struct{}
}

func (c *gatedConn) Write(msg []byte, deadline time.Time) error {
	c.arrived <- struct{}{}
	<-c.release
	return c.MockConnection.Write(msg, deadline)
}

func (c *gatedConn) WriteBinary(msg []byte, deadline time.Time) error {
	c.arrived <- struct{}{}
	<-c.release
	return c.MockConnection.WriteBinary(msg, deadline)
}

type reader interface {
	Read() ([]byte, error)
}

type sessionObj struct {
	s    *node.Session
	conn reader
	gate *gatedConn
}

type nodeCall struct {
	ID      uint64          `json:"id"`
	Method  string          `json:"method"`
	Session uint64          `json:"session"`
	Name    string          `json:"name"`
	Ids     string          `json:"ids"`
	Env     *sessionEnv     `json:"env"`
	Val     bool            `json:"val"`
	Reply   json.RawMessage `json:"reply"`
	Data    string          `json:"data"`
	N       int             `json:"n"`
	Timeout *int64          `json:"timeout_ms"`
	Channel string          `json:"channel"`
	Stream  string          `json:"stream"`
}

func readReply(msg []byte, err error) map[string]interface{} {
	if err != nil {
		return map[string]interface{}{"msg": nil, "error": errString(err)}
	}
	return map[string]interface{}{"msg": string(msg), "error": nil}
}

func init() {
	register("node.NewConfig", func(json.RawMessage) (interface{}, error) {
		return node.NewConfig(), nil
	})
	register("node.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config node.Config }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return a.Config.ToToml(), nil
	})
	register("node.Config.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config node.Config
			Toml   string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config
		_, err := toml.Decode(a.Toml, &conf)
		return map[string]interface{}{"config": conf, "error": errString(err)}, nil
	})
	register("node.NewDisconnectQueueConfig", func(json.RawMessage) (interface{}, error) {
		return node.NewDisconnectQueueConfig(), nil
	})
	register("node.DisconnectQueueConfig.ToToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config node.DisconnectQueueConfig }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return a.Config.ToToml(), nil
	})
	register("node.DisconnectQueueConfig.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config node.DisconnectQueueConfig
			Toml   string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config
		_, err := toml.Decode(a.Toml, &conf)
		return map[string]interface{}{"config": conf, "error": errString(err)}, nil
	})

	register("node.NewSubscriptionState", func(json.RawMessage) (interface{}, error) {
		return nodePut(node.NewSubscriptionState()), nil
	})
	register("node.SubscriptionState", func(raw json.RawMessage) (interface{}, error) {
		var c nodeCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		st, err := nodeGet[*node.SubscriptionState](c.ID)
		if err != nil {
			return nil, err
		}
		var result interface{}
		switch c.Method {
		case "HasChannel":
			result = st.HasChannel(c.Channel)
		case "AddChannel":
			st.AddChannel(c.Channel)
		case "RemoveChannel":
			st.RemoveChannel(c.Channel)
		case "Channels":
			result = st.Channels()
		case "AddChannelStream":
			st.AddChannelStream(c.Channel, c.Stream)
		case "RemoveChannelStream":
			st.RemoveChannelStream(c.Channel, c.Stream)
		case "RemoveChannelStreams":
			result = st.RemoveChannelStreams(c.Channel)
		case "StreamsFor":
			result = st.StreamsFor(c.Channel)
		default:
			return nil, fmt.Errorf("unknown SubscriptionState method %q", c.Method)
		}
		return map[string]interface{}{"result": result}, nil
	})

	// Upstream NewMockNode (node/node_mocks_test.go).
	register("node.NewMockNode", func(json.RawMessage) (interface{}, error) {
		controller := mocks.NewMockController()
		config := node.NewConfig()
		config.HubGopoolSize = 2
		m := metrics.NewMetrics(nil, 10, slog.Default())
		n := node.NewNode(&config, node.WithInstrumenter(m), node.WithController(&controller))
		n.SetBroker(broker.NewLegacyBroker(pubsub.NewLegacySubscriber(n)))
		dconfig := node.NewDisconnectQueueConfig()
		dconfig.Rate = 1
		q := node.NewDisconnectQueue(n, &dconfig, slog.Default())
		n.SetDisconnector(q)
		return map[string]interface{}{"node": nodePut(&nodeObj{n: n, metrics: m}), "queue": nodePut(q)}, nil
	})
	register("node.Node", func(raw json.RawMessage) (interface{}, error) {
		var c nodeCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		o, err := nodeGet[*nodeObj](c.ID)
		if err != nil {
			return nil, err
		}
		switch c.Method {
		case "Start":
			return nil, o.n.Start()
		case "Size":
			return o.n.Size(), nil
		case "Authenticate":
			s, err := nodeGet[*sessionObj](c.Session)
			if err != nil {
				return nil, err
			}
			res, aerr := o.n.Authenticate(s.s)
			var result interface{}
			if res != nil {
				result = res
			}
			return map[string]interface{}{"result": result, "error": errString(aerr)}, nil
		case "Disconnect":
			s, err := nodeGet[*sessionObj](c.Session)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{"error": errString(o.n.Disconnect(s.s))}, nil
		case "Counter":
			counter := o.metrics.Counter(c.Name)
			if counter == nil {
				return nil, fmt.Errorf("no counter %q", c.Name)
			}
			return counter.Value(), nil
		default:
			return nil, fmt.Errorf("unknown Node method %q", c.Method)
		}
	})

	// Sessions over mocks.NewMockConnection(): node.NewSession with pings
	// disabled (upstream NewMockSession starts no timers) and identifiers equal
	// to the uid.
	register("node.NewSession", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Node           uint64             `json:"node"`
			UID            string             `json:"uid"`
			URL            string             `json:"url"`
			Headers        *map[string]string `json:"headers"`
			MaxPendingSize *uint64            `json:"max_pending_size"`
			Gated          bool               `json:"gated"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		o, err := nodeGet[*nodeObj](a.Node)
		if err != nil {
			return nil, err
		}
		opts := []node.SessionOption{node.WithPingInterval(0)}
		if a.MaxPendingSize != nil {
			opts = append(opts, node.WithMaxPendingSize(*a.MaxPendingSize))
		}
		obj := &sessionObj{}
		var conn node.Connection
		if a.Gated {
			obj.gate = &gatedConn{MockConnection: mocks.NewMockConnection(), arrived: make(chan struct{}, 1024), release: make(chan struct{})}
			conn = obj.gate
			obj.conn = obj.gate
		} else {
			mc := mocks.NewMockConnection()
			conn = mc
			obj.conn = mc
		}
		obj.s = node.NewSession(o.n, conn, a.URL, a.Headers, a.UID, opts...)
		obj.s.SetIdentifiers(a.UID)
		return nodePut(obj), nil
	})
	register("node.BuildSession", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			URL     string             `json:"url"`
			Headers *map[string]string `json:"headers"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		mc := mocks.NewMockConnection()
		s := node.BuildSession(mc, common.NewSessionEnv(a.URL, a.Headers))
		return nodePut(&sessionObj{s: s, conn: mc}), nil
	})
	register("node.Session", func(raw json.RawMessage) (interface{}, error) {
		var c nodeCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		o, err := nodeGet[*sessionObj](c.ID)
		if err != nil {
			return nil, err
		}
		s := o.s
		switch c.Method {
		case "Read":
			return readReply(o.conn.Read()), nil
		case "GetID":
			return s.GetID(), nil
		case "IsConnected":
			return s.IsConnected(), nil
		case "IsClosed":
			return s.IsClosed(), nil
		case "GetIdentifiers":
			return s.GetIdentifiers(), nil
		case "SetIdentifiers":
			s.SetIdentifiers(c.Ids)
			return nil, nil
		case "GetEnv":
			return bridgeEnvOf(s.GetEnv()), nil
		case "SetEnv":
			s.SetEnv(c.Env.commonOrNil())
			return nil, nil
		case "MergeEnv":
			s.MergeEnv(c.Env.commonOrNil())
			return nil, nil
		case "MarkDisconnectable":
			s.MarkDisconnectable(c.Val)
			return nil, nil
		case "IsDisconnectable":
			return s.IsDisconnectable(), nil
		case "Send":
			var reply common.Reply
			if err := json.Unmarshal(c.Reply, &reply); err != nil {
				return nil, err
			}
			s.Send(&reply)
			return nil, nil
		case "ToCacheEntry":
			data, cerr := s.ToCacheEntry()
			return readReply(data, cerr), nil
		case "RestoreFromCache":
			return map[string]interface{}{"error": errString(s.RestoreFromCache([]byte(c.Data)))}, nil
		case "AwaitWritesHeld":
			if o.gate == nil {
				return nil, fmt.Errorf("session %d has no write gate", c.ID)
			}
			timeout := time.After(time.Duration(*c.Timeout) * time.Millisecond)
			for i := 0; i < c.N; i++ {
				select {
				case <-o.gate.arrived:
				case <-timeout:
					return false, nil
				}
			}
			return true, nil
		case "ReleaseWrites":
			if o.gate == nil {
				return nil, fmt.Errorf("session %d has no write gate", c.ID)
			}
			close(o.gate.release)
			return nil, nil
		default:
			return nil, fmt.Errorf("unknown Session method %q", c.Method)
		}
	})

	register("node.NewDisconnectQueue", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Node   uint64                     `json:"node"`
			Config node.DisconnectQueueConfig `json:"config"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		o, err := nodeGet[*nodeObj](a.Node)
		if err != nil {
			return nil, err
		}
		conf := a.Config
		return nodePut(node.NewDisconnectQueue(o.n, &conf, slog.Default())), nil
	})
	register("node.DisconnectQueue", func(raw json.RawMessage) (interface{}, error) {
		var c nodeCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		q, err := nodeGet[*node.DisconnectQueue](c.ID)
		if err != nil {
			return nil, err
		}
		switch c.Method {
		case "Enqueue":
			s, err := nodeGet[*sessionObj](c.Session)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{"error": errString(q.Enqueue(s.s))}, nil
		case "Size":
			return map[string]interface{}{"size": q.Size()}, nil
		case "Shutdown":
			ctx := context.Background()
			if c.Timeout != nil {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(*c.Timeout)*time.Millisecond)
				defer cancel()
			}
			return map[string]interface{}{"error": errString(q.Shutdown(ctx))}, nil
		case "Run":
			go q.Run() // nolint:errcheck
			return nil, nil
		default:
			return nil, fmt.Errorf("unknown DisconnectQueue method %q", c.Method)
		}
	})
}
