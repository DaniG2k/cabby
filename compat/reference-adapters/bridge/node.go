package main

// Bridge ops for upstream package `node`. Real Go nodes, sessions, brokers,
// disconnect queues and subscription states are kept behind integer handles
// and driven through their exported API, plus two unexported fields that
// upstream's tests (package node) use directly and the bridge reads by
// reflection: node.hub and session.closeCtx. Objects are wired the way
// upstream's test helpers (node/node_mocks_test.go, setupIntegrationNode) wire
// them, and testify mocks (mocks.Controller, mocks.Broker) are programmed from
// JSON expectations with canned Run callbacks. The test scaffolding upstream
// runs in goroutines (racing callers, enqueuers, readers) runs here too.
// Nothing of the node itself is reimplemented.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/broker"
	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/hub"
	"github.com/anycable/anycable-go/metrics"
	"github.com/anycable/anycable-go/mocks"
	natsconfig "github.com/anycable/anycable-go/nats"
	"github.com/anycable/anycable-go/node"
	"github.com/anycable/anycable-go/pubsub"
	"github.com/anycable/anycable-go/ws"
	"github.com/stretchr/testify/mock"
)

var (
	nodeMu      sync.Mutex
	nodeObjects = map[uint64]interface{}{}
	nodeNextID  uint64
)

func nodePut(o interface{}) uint64 {
	nodeMu.Lock()
	defer nodeMu.Unlock()
	nodeNextID++
	nodeObjects[nodeNextID] = o
	return nodeNextID
}

func nodeGet[T any](id uint64) (T, error) {
	nodeMu.Lock()
	defer nodeMu.Unlock()
	o, ok := nodeObjects[id].(T)
	if !ok {
		return o, fmt.Errorf("no %T with handle %d", o, id)
	}
	return o, nil
}

// handleOf finds the handle of a session object wrapping s: nil for no
// session, "unregistered" for a session the bridge does not know.
func handleOf(s *node.Session) interface{} {
	if s == nil {
		return nil
	}
	nodeMu.Lock()
	defer nodeMu.Unlock()
	for id, o := range nodeObjects {
		if so, ok := o.(*sessionObj); ok && so.s == s {
			return id
		}
	}
	return "unregistered"
}

type nodeObj struct {
	n          *node.Node
	metrics    *metrics.Metrics
	controller node.Controller
	broker     broker.Broker
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
	node *nodeObj
}

func readReply(msg []byte, err error) map[string]interface{} {
	if err != nil {
		return map[string]interface{}{"msg": nil, "error": errString(err)}
	}
	return map[string]interface{}{"msg": string(msg), "error": nil}
}

// recovered turns a panic (e.g. testify rejecting an unexpected call) into a
// bridge error instead of killing the shared bridge.
func recovered(f op) op {
	return func(raw json.RawMessage) (res interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				res, err = nil, fmt.Errorf("panic: %v", r)
			}
		}()
		return f(raw)
	}
}

// ------------------------------------------------------------ testify mocks

type mockArg struct {
	Any bool            `json:"any"`
	Eq  json.RawMessage `json:"eq"`
}

type mockRet struct {
	// unit | err | json | echo_first | echo_states
	Kind  string          `json:"kind"`
	Error string          `json:"error"`
	Value json.RawMessage `json:"value"`
}

type mockRun struct {
	// broadcast | handle_join | handle_leave | forbidden | inflight_gate | count
	Kind    string                `json:"kind"`
	Stream  string                `json:"stream"`
	Event   *common.PresenceEvent `json:"event"`
	Message string                `json:"message"`
}

type mockExpectation struct {
	Method string    `json:"method"`
	Args   []mockArg `json:"args"`
	Ret    mockRet   `json:"ret"`
	Run    *mockRun  `json:"run"`
}

// Argument types of the mocked methods (after the implicit context of controller calls).
var brokerArgTypes = map[string][]string{
	"Subscribe": {"s"}, "Unsubscribe": {"s"}, "RestoreSession": {"s"}, "TouchPresence": {"s"},
	"TouchSession": {"s"}, "DeleteSession": {"s"}, "HistorySince": {"s", "i64"}, "HistoryFrom": {"s", "s", "u64"},
	"PresenceAdd": {"s", "s", "s", "any"}, "PresenceRemove": {"s", "s"}, "CommitSession": {"s", "opaque"},
	"HandleBroadcast": {"opaque"},
}

var controllerArgTypes = map[string][]string{
	"Start": {}, "Shutdown": {}, "Authenticate": {"s", "opaque"}, "Subscribe": {"s", "opaque", "s", "s"},
	"Unsubscribe": {"s", "opaque", "s", "s"}, "Perform": {"s", "opaque", "s", "s", "s"},
	"Disconnect": {"s", "opaque", "s", "strs"},
}

func typedArgs(types []string, args []mockArg) ([]interface{}, error) {
	if len(types) != len(args) {
		return nil, fmt.Errorf("expected %d arguments, got %d", len(types), len(args))
	}
	out := []interface{}{}
	for i, a := range args {
		if a.Any {
			out = append(out, mock.Anything)
			continue
		}
		var v interface{}
		var err error
		switch types[i] {
		case "s":
			var s string
			err = json.Unmarshal(a.Eq, &s)
			v = s
		case "i64":
			var n int64
			err = json.Unmarshal(a.Eq, &n)
			v = n
		case "u64":
			var n uint64
			err = json.Unmarshal(a.Eq, &n)
			v = n
		case "strs":
			var ss []string
			err = json.Unmarshal(a.Eq, &ss)
			v = ss
		case "any":
			err = json.Unmarshal(a.Eq, &v)
		default:
			return nil, fmt.Errorf("argument %d can only be matched with mock.Anything", i)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (r mockRet) err() error {
	if r.Kind == "err" {
		return errors.New(r.Error)
	}
	return nil
}

func (r mockRet) isNull() bool {
	return r.Kind != "json" || len(r.Value) == 0 || string(r.Value) == "null"
}

// callsOf serializes recorded calls (contexts, envs and sessions as null).
func callsOf(m *mock.Mock, dropCtx bool) []map[string]interface{} {
	out := []map[string]interface{}{}
	for _, c := range m.Calls {
		args := []interface{}{}
		for i, a := range c.Arguments {
			if dropCtx && i == 0 {
				continue
			}
			switch v := a.(type) {
			case string, int64, uint64, []string, *common.StreamMessage:
				args = append(args, v)
			default:
				if _, ok := v.(fmt.Stringer); ok {
					args = append(args, nil)
				} else if b, err := json.Marshal(v); err == nil && len(b) < 4096 {
					var x interface{}
					_ = json.Unmarshal(b, &x)
					args = append(args, x)
				} else {
					args = append(args, nil)
				}
			}
		}
		out = append(out, map[string]interface{}{"method": c.Method, "args": args})
	}
	return out
}

// gateState is upstream's blocking Disconnect mock bookkeeping (inflight, peak,
// calls atomics and a release channel).
type gateState struct {
	inflight, peak, calls, arrived atomic.Int32
	release                        chan struct{}
	releaseOnce                    sync.Once
}

type mockObj struct {
	m          *mock.Mock
	broker     *mocks.Broker
	controller *mocks.Controller
	node       *nodeObj
	gate       *gateState
	violations []string
	vmu        sync.Mutex
}

func (o *mockObj) runFor(r *mockRun) func(mock.Arguments) {
	if r == nil {
		return nil
	}
	switch r.Kind {
	case "broadcast":
		return func(a mock.Arguments) { o.node.n.Broadcast(a.Get(0).(*common.StreamMessage)) }
	case "handle_join":
		return func(mock.Arguments) { o.node.n.HandleJoin(r.Stream, r.Event) }
	case "handle_leave":
		return func(mock.Arguments) { o.node.n.HandleLeave(r.Stream, r.Event) }
	case "forbidden":
		return func(mock.Arguments) {
			o.vmu.Lock()
			o.violations = append(o.violations, r.Message)
			o.vmu.Unlock()
		}
	case "inflight_gate":
		g := o.gate
		return func(mock.Arguments) {
			g.arrived.Add(1)
			n := g.inflight.Add(1)
			if n > g.peak.Load() {
				g.peak.Store(n)
			}
			<-g.release
			g.inflight.Add(-1)
			g.calls.Add(1)
		}
	case "count":
		g := o.gate
		return func(mock.Arguments) { g.calls.Add(1) }
	}
	panic("unknown run kind " + r.Kind)
}

func (o *mockObj) brokerReturns(e mockExpectation) ([]interface{}, error) {
	r := e.Ret
	switch e.Method {
	case "Subscribe", "Unsubscribe":
		if r.Kind == "echo_first" {
			return []interface{}{func(name string) string { return name }}, nil
		}
		var s string
		return []interface{}{s}, json.Unmarshal(r.Value, &s)
	case "HistorySince", "HistoryFrom":
		if r.isNull() {
			return []interface{}{nil, r.err()}, nil
		}
		var msgs []common.StreamMessage
		return []interface{}{msgs, r.err()}, json.Unmarshal(r.Value, &msgs)
	case "RestoreSession":
		if r.isNull() {
			return []interface{}{nil, r.err()}, nil
		}
		var s string
		if err := json.Unmarshal(r.Value, &s); err != nil {
			return nil, err
		}
		return []interface{}{[]byte(s), r.err()}, nil
	case "PresenceAdd", "PresenceRemove":
		if r.isNull() {
			return []interface{}{nil, r.err()}, nil
		}
		var ev common.PresenceEvent
		return []interface{}{&ev, r.err()}, json.Unmarshal(r.Value, &ev)
	case "CommitSession", "TouchPresence", "TouchSession", "HandleBroadcast":
		return []interface{}{r.err()}, nil
	}
	return nil, fmt.Errorf("unsupported broker method %q", e.Method)
}

func (o *mockObj) controllerReturns(e mockExpectation) ([]interface{}, error) {
	r := e.Ret
	switch e.Method {
	case "Start", "Shutdown", "Disconnect":
		return []interface{}{r.err()}, nil
	case "Authenticate":
		if r.isNull() {
			return []interface{}{nil, r.err()}, nil
		}
		var res common.ConnectResult
		return []interface{}{&res, r.err()}, json.Unmarshal(r.Value, &res)
	case "Subscribe", "Unsubscribe", "Perform":
		if r.Kind == "echo_states" {
			// The Perform responder of TestIntegrationRestore ("echo" the states).
			return []interface{}{func(ctx context.Context, sid string, env *common.SessionEnv, ids string, identifier string, data string) *common.CommandResult {
				res := &common.CommandResult{Status: common.SUCCESS}
				res.Transmissions = []string{
					fmt.Sprintf("city:%s", env.GetConnectionStateField("city")),
					fmt.Sprintf("locale:%s", env.GetChannelStateField(identifier, "locale")),
				}
				return res
			}, nil}, nil
		}
		if r.isNull() {
			return []interface{}{nil, r.err()}, nil
		}
		var res common.CommandResult
		return []interface{}{&res, r.err()}, json.Unmarshal(r.Value, &res)
	}
	return nil, fmt.Errorf("unsupported controller method %q", e.Method)
}

func (o *mockObj) on(e mockExpectation) error {
	var types []string
	var returns []interface{}
	var err error
	if o.broker != nil {
		types = brokerArgTypes[e.Method]
		returns, err = o.brokerReturns(e)
	} else {
		types = controllerArgTypes[e.Method]
		returns, err = o.controllerReturns(e)
	}
	if err != nil {
		return err
	}
	if types == nil {
		return fmt.Errorf("unsupported method %q", e.Method)
	}
	args, err := typedArgs(types, e.Args)
	if err != nil {
		return fmt.Errorf("%s: %w", e.Method, err)
	}
	if o.controller != nil && e.Method != "Start" && e.Method != "Shutdown" {
		args = append([]interface{}{mock.Anything}, args...)
	}
	call := o.m.On(e.Method, args...).Return(returns...)
	if run := o.runFor(e.Run); run != nil {
		call.Run(run)
	}
	return nil
}

func newMockObj(n *nodeObj, isBroker bool) *mockObj {
	o := &mockObj{node: n, gate: &gateState{release: make(chan struct{})}}
	if isBroker {
		o.broker = &mocks.Broker{}
		o.m = &o.broker.Mock
	} else {
		o.controller = &mocks.Controller{}
		o.m = &o.controller.Mock
	}
	return o
}

// ------------------------------------------------------- blocking disconnector

// blockingDisconnector is upstream's (node_test.go): Enqueue signals `enqueued`
// and blocks until released.
type blockingDisconnector struct {
	node.NoopDisconnectQueue
	enqueued chan struct{}
	release  chan struct{}
	// Upstream closes `enqueued` on every call, so a second Enqueue panics;
	// here calls are counted and the test asserts exactly one.
	calls atomic.Int32
}

func (d *blockingDisconnector) Enqueue(*node.Session) error {
	if d.calls.Add(1) == 1 {
		close(d.enqueued)
	}
	<-d.release
	return nil
}

// unexportedField points to the unexported field `name` of the struct `obj`
// points to, which must have type T. Upstream's node tests live in package
// node and use such fields directly (node.hub, session.closeCtx); the bridge
// reads the same memory instead of reimplementing what the call does.
func unexportedField[T any](obj interface{}, name string) (*T, error) {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("%T is not a pointer to a struct", obj)
	}
	f := v.Elem().FieldByName(name)
	if !f.IsValid() {
		return nil, fmt.Errorf("%T has no field %q", obj, name)
	}
	if want := reflect.TypeOf((*T)(nil)).Elem(); f.Type() != want {
		return nil, fmt.Errorf("field %q of %T is %s, not %s", name, obj, f.Type(), want)
	}
	return (*T)(unsafe.Pointer(f.UnsafeAddr())), nil
}

// --------------------------------------------------------------------- calls

type nodeCall struct {
	ID         uint64             `json:"id"`
	Method     string             `json:"method"`
	Session    uint64             `json:"session"`
	Broker     uint64             `json:"broker"`
	Controller uint64             `json:"controller"`
	Name       string             `json:"name"`
	Ids        string             `json:"ids"`
	Env        *sessionEnv        `json:"env"`
	Val        bool               `json:"val"`
	Reply      json.RawMessage    `json:"reply"`
	Msg        json.RawMessage    `json:"msg"`
	Data       string             `json:"data"`
	N          int                `json:"n"`
	Timeout    *int64             `json:"timeout_ms"`
	Channel    string             `json:"channel"`
	Stream     string             `json:"stream"`
	Identifier string             `json:"identifier"`
	State      *map[string]string `json:"state"`
	Reason     string             `json:"reason"`
	Code       int                `json:"code"`
	Kind       string             `json:"kind"`
	Expect     mockExpectation    `json:"expect"`
	Epoch      string             `json:"epoch"`
}

func (c nodeCall) message() (*common.Message, error) {
	var m common.Message
	if err := json.Unmarshal(c.Msg, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func commandReply(res *common.CommandResult, err error) map[string]interface{} {
	var result interface{}
	if res != nil {
		result = res
	}
	return map[string]interface{}{"result": result, "error": errString(err)}
}

func errReply(err error) map[string]interface{} {
	return map[string]interface{}{"error": errString(err)}
}

// subscriptionsOf reads a session's subscriptions from its exported cache
// entry (`subs`).
func subscriptionsOf(s *node.Session) (map[string][]string, error) {
	raw, err := s.ToCacheEntry()
	if err != nil {
		return nil, err
	}
	var entry struct {
		Subs map[string][]string `json:"subs"`
	}
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, err
	}
	return entry.Subs, nil
}

// restoreSubscriptions re-applies a cache entry with `subs` replaced: the
// exported RestoreFromCache calls AddChannel/AddChannelStream on the session's
// subscriptions (and re-merges the identical states).
func restoreSubscriptions(s *node.Session, subs map[string][]string) error {
	raw, err := s.ToCacheEntry()
	if err != nil {
		return err
	}
	var entry map[string]interface{}
	if err := json.Unmarshal(raw, &entry); err != nil {
		return err
	}
	entry["subs"] = subs
	out, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return s.RestoreFromCache(out)
}

func ms(v *int64, def time.Duration) time.Duration {
	if v == nil {
		return def
	}
	return time.Duration(*v) * time.Millisecond
}

func newNodeObj(n *node.Node, m *metrics.Metrics, c node.Controller) *nodeObj {
	return &nodeObj{n: n, metrics: m, controller: c}
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
	register("node.DisconnectQueueConfig.Validate", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config node.DisconnectQueueConfig }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return errString(a.Config.Validate()), nil
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
	register("node.NewMockNode", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			// Upstream tests assign the unexported `config.PingInterval` after NewMockNode.
			PingInterval *int `json:"ping_interval"`
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &a); err != nil {
				return nil, err
			}
		}
		controller := mocks.NewMockController()
		config := node.NewConfig()
		config.HubGopoolSize = 2
		if a.PingInterval != nil {
			config.PingInterval = *a.PingInterval
		}
		m := metrics.NewMetrics(nil, 10, slog.Default())
		n := node.NewNode(&config, node.WithInstrumenter(m), node.WithController(&controller))
		o := newNodeObj(n, m, &controller)
		o.broker = broker.NewLegacyBroker(pubsub.NewLegacySubscriber(n))
		n.SetBroker(o.broker)
		dconfig := node.NewDisconnectQueueConfig()
		dconfig.Rate = 1
		q := node.NewDisconnectQueue(n, &dconfig, slog.Default())
		n.SetDisconnector(q)
		return map[string]interface{}{"node": nodePut(o), "queue": nodePut(q)}, nil
	})

	// A testify mock (mocks.Controller or mocks.Broker) bound to a node for its
	// Run callbacks; programmed with `node.Mock` On.
	register("node.NewMock", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Node uint64 `json:"node"`
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		var n *nodeObj
		if a.Node != 0 {
			var err error
			if n, err = nodeGet[*nodeObj](a.Node); err != nil {
				return nil, err
			}
		}
		return nodePut(newMockObj(n, a.Kind == "broker")), nil
	})
	register("node.Mock", func(raw json.RawMessage) (interface{}, error) {
		var c nodeCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		o, err := nodeGet[*mockObj](c.ID)
		if err != nil {
			return nil, err
		}
		g := o.gate
		switch c.Method {
		case "Bind":
			n, err := nodeGet[*nodeObj](c.Session)
			if err != nil {
				return nil, err
			}
			o.node = n
			return nil, nil
		case "On":
			return nil, o.on(c.Expect)
		case "Calls":
			return callsOf(o.m, o.controller != nil), nil
		case "Violations":
			o.vmu.Lock()
			defer o.vmu.Unlock()
			return append([]string{}, o.violations...), nil
		case "Gate":
			return map[string]interface{}{"inflight": g.inflight.Load(), "peak": g.peak.Load(),
				"calls": g.calls.Load(), "arrived": g.arrived.Load()}, nil
		case "Release":
			g.releaseOnce.Do(func() { close(g.release) })
			return nil, nil
		case "AwaitGate":
			// require.Eventually over the counters (no notification exists).
			deadline := time.Now().Add(ms(c.Timeout, time.Second))
			for time.Now().Before(deadline) {
				var v int32
				switch c.Name {
				case "inflight":
					v = g.inflight.Load()
				case "calls":
					v = g.calls.Load()
				}
				if int(v) >= c.N {
					return true, nil
				}
				time.Sleep(time.Millisecond)
			}
			return false, nil
		}
		return nil, fmt.Errorf("unknown Mock method %q", c.Method)
	})

	// A node with a testify controller: upstream setupIntegrationNode and the
	// queue tests' NewNode(&config, WithInstrumenter(...), WithController(ctrl)).
	register("node.NewNode", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config     node.Config `json:"config"`
			Controller uint64      `json:"controller"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		ctrl, err := nodeGet[*mockObj](a.Controller)
		if err != nil {
			return nil, err
		}
		conf := a.Config
		m := metrics.NewMetrics(nil, 10, slog.Default())
		n := node.NewNode(&conf, node.WithController(ctrl.controller), node.WithInstrumenter(m))
		o := newNodeObj(n, m, ctrl.controller)
		ctrl.node = o
		return nodePut(o), nil
	})

	// Brokers bound to a node: broker.NewMemoryBroker(pubsub.NewLegacySubscriber(n), n, &conf)
	// and broker.NewNATSBroker(pubsub.NewLegacySubscriber(n), &conf, &natsConf).
	register("node.NewBroker", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Node    uint64        `json:"node"`
			Kind    string        `json:"kind"`
			Config  broker.Config `json:"config"`
			Servers string        `json:"servers"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		o, err := nodeGet[*nodeObj](a.Node)
		if err != nil {
			return nil, err
		}
		conf := a.Config
		switch a.Kind {
		case "memory":
			return nodePut(broker.NewMemoryBroker(pubsub.NewLegacySubscriber(o.n), o.n, &conf)), nil
		case "nats":
			nconf := natsconfig.NewNATSConfig()
			nconf.Servers = a.Servers
			return nodePut(broker.NewNATSBroker(pubsub.NewLegacySubscriber(o.n), &conf, &nconf, slog.Default())), nil
		}
		return nil, fmt.Errorf("unknown broker kind %q", a.Kind)
	})
	register("node.Broker", recovered(func(raw json.RawMessage) (interface{}, error) {
		var c nodeCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		b, err := nodeGet[broker.Broker](c.ID)
		if err != nil {
			return nil, err
		}
		switch c.Method {
		case "Start":
			return errReply(b.Start(nil)), nil
		case "SetEpoch":
			switch v := b.(type) {
			case *broker.Memory:
				v.SetEpoch(c.Epoch)
				return errReply(nil), nil
			case *broker.NATS:
				return errReply(v.SetEpoch(c.Epoch)), nil
			}
		case "Reset":
			if v, ok := b.(*broker.NATS); ok {
				return errReply(v.Reset()), nil
			}
		case "Shutdown":
			return errReply(b.Shutdown(context.Background())), nil
		case "RestoreSession":
			data, rerr := b.RestoreSession(c.Data)
			var cached *string
			if data != nil {
				s := string(data)
				cached = &s
			}
			return map[string]interface{}{"data": cached, "error": errString(rerr)}, nil
		}
		return nil, fmt.Errorf("unsupported Broker method %q for %T", c.Method, b)
	}))

	register("node.Node", recovered(func(raw json.RawMessage) (interface{}, error) {
		var c nodeCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		o, err := nodeGet[*nodeObj](c.ID)
		if err != nil {
			return nil, err
		}
		n := o.n
		session := func() (*sessionObj, error) { return nodeGet[*sessionObj](c.Session) }
		switch c.Method {
		case "Start":
			return errReply(n.Start()), nil
		case "Shutdown":
			return errReply(n.Shutdown(context.Background())), nil
		case "Size":
			return n.Size(), nil
		case "Counter":
			counter := o.metrics.Counter(c.Name)
			if counter == nil {
				return nil, fmt.Errorf("no counter %q", c.Name)
			}
			return counter.Value(), nil
		case "SetBroker":
			b, err := nodeGet[broker.Broker](c.Broker)
			if err != nil {
				mo, merr := nodeGet[*mockObj](c.Broker)
				if merr != nil {
					return nil, err
				}
				mo.node = o
				b = mo.broker
			}
			o.broker = b
			n.SetBroker(b)
			return nil, nil
		case "SetController":
			mo, err := nodeGet[*mockObj](c.Controller)
			if err != nil {
				return nil, err
			}
			mo.node = o
			o.controller = mo.controller
			node.WithController(mo.controller)(n)
			return nil, nil
		case "SetDisconnector":
			switch c.Kind {
			case "noop":
				n.SetDisconnector(node.NewNoopDisconnector())
				return nil, nil
			case "blocking":
				d := &blockingDisconnector{enqueued: make(chan struct{}), release: make(chan struct{})}
				n.SetDisconnector(d)
				return nodePut(d), nil
			}
			return nil, fmt.Errorf("unknown disconnector %q", c.Kind)
		case "BrokerSubscribe":
			return o.broker.Subscribe(c.Stream), nil
		case "HandleBroadcast":
			return errReply(n.HandleBroadcast([]byte(c.Data))), nil
		case "HandlePubSub":
			n.HandlePubSub([]byte(c.Data))
			return nil, nil
		case "Broadcast":
			var m common.StreamMessage
			if err := json.Unmarshal(c.Msg, &m); err != nil {
				return nil, err
			}
			n.Broadcast(&m)
			return nil, nil
		case "LookupSession":
			return handleOf(n.LookupSession(c.Ids)), nil
		case "RemoteDisconnect":
			n.RemoteDisconnect(&common.RemoteDisconnectMessage{Identifier: c.Ids, Reconnect: c.Val})
			return nil, nil
		}

		s, err := session()
		if err != nil {
			return nil, err
		}
		switch c.Method {
		case "Authenticate":
			res, aerr := n.Authenticate(s.s)
			var result interface{}
			if res != nil {
				result = res
			}
			return map[string]interface{}{"result": result, "error": errString(aerr)}, nil
		case "Authenticated":
			n.Authenticated(s.s, c.Ids)
			return nil, nil
		case "Disconnect":
			return errReply(n.Disconnect(s.s)), nil
		case "HubSubscribe":
			// Upstream's tests (package node) call node.hub.SubscribeSession
			// directly; the hub is an unexported field, read by reflection, and
			// the call is the upstream one.
			h, err := unexportedField[*hub.Hub](n, "hub")
			if err != nil {
				return nil, err
			}
			(*h).SubscribeSession(s.s, c.Stream, c.Identifier)
			return errReply(nil), nil
		case "CloseContextErr":
			// Upstream asserts session.closeCtx.Err() (unexported, read by
			// reflection).
			ctx, err := unexportedField[context.Context](s.s, "closeCtx")
			if err != nil {
				return nil, err
			}
			return (*ctx).Err() != nil, nil
		}

		msg, err := c.message()
		if err != nil {
			return nil, err
		}
		switch c.Method {
		case "Subscribe":
			return commandReply(n.Subscribe(s.s, msg)), nil
		case "Unsubscribe":
			return commandReply(n.Unsubscribe(s.s, msg)), nil
		case "Perform":
			return commandReply(n.Perform(s.s, msg)), nil
		case "History":
			return errReply(n.History(s.s, msg)), nil
		case "Whisper":
			return errReply(n.Whisper(s.s, msg)), nil
		case "Presence":
			return errReply(n.Presence(s.s, msg)), nil
		case "PresenceJoin":
			return errReply(n.PresenceJoin(s.s, msg)), nil
		case "PresenceLeave":
			return errReply(n.PresenceLeave(s.s, msg)), nil
		}
		return nil, fmt.Errorf("unknown Node method %q", c.Method)
	}))

	register("node.BlockingDisconnector", func(raw json.RawMessage) (interface{}, error) {
		var c nodeCall
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		d, err := nodeGet[*blockingDisconnector](c.ID)
		if err != nil {
			return nil, err
		}
		switch c.Method {
		case "AwaitEnqueued":
			select {
			case <-d.enqueued:
				return true, nil
			case <-time.After(ms(c.Timeout, time.Second)):
				return false, nil
			}
		case "Release":
			close(d.release)
			return nil, nil
		case "Enqueues":
			return d.calls.Load(), nil
		}
		return nil, fmt.Errorf("unknown BlockingDisconnector method %q", c.Method)
	})

	// Sessions over mocks.NewMockConnection().
	//
	// mode "mock" is upstream NewMockSession: BuildSession, executor = node, uid,
	// Log, identifiers = uid, the options, `go SendMessages()`, no timers.
	// (session.broker is unexported; it is only used by timers, which mock
	// sessions never start.) mode "timers" is node.NewSession with pings off,
	// which also starts the keepalive timers, plus SetIdentifiers(uid).
	register("node.NewSession", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Node           uint64             `json:"node"`
			Mode           string             `json:"mode"`
			UID            string             `json:"uid"`
			URL            string             `json:"url"`
			Headers        *map[string]string `json:"headers"`
			MaxPendingSize *uint64            `json:"max_pending_size"`
			Gated          bool               `json:"gated"`
			SessionsTTL    *int64             `json:"sessions_ttl_ms"`
			PresenceTTL    *int64             `json:"presence_ttl_ms"`
			PrevSid        *string            `json:"prev_sid"`
			PingIntervalNs *int64             `json:"ping_interval_ns"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		o, err := nodeGet[*nodeObj](a.Node)
		if err != nil {
			return nil, err
		}
		opts := []node.SessionOption{}
		if a.SessionsTTL != nil || a.PresenceTTL != nil {
			opts = append(opts, node.WithKeepaliveIntervals(ms(a.SessionsTTL, 0), ms(a.PresenceTTL, 0)))
		}
		if a.PrevSid != nil {
			opts = append(opts, node.WithPrevSID(*a.PrevSid))
		}
		if a.MaxPendingSize != nil {
			opts = append(opts, node.WithMaxPendingSize(*a.MaxPendingSize))
		}
		if a.PingIntervalNs != nil {
			opts = append(opts, node.WithPingInterval(time.Duration(*a.PingIntervalNs)))
		}
		obj := &sessionObj{node: o}
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
		switch a.Mode {
		case "mock":
			s := node.BuildSession(conn, common.NewSessionEnv(a.URL, a.Headers))
			node.WithExecutor(o.n)(s)
			s.SetID(a.UID)
			s.Log = slog.With("sid", a.UID)
			s.SetIdentifiers(a.UID)
			for _, opt := range opts {
				opt(s)
			}
			go s.SendMessages()
			obj.s = s
		case "timers":
			opts = append([]node.SessionOption{node.WithPingInterval(0)}, opts...)
			obj.s = node.NewSession(o.n, conn, a.URL, a.Headers, a.UID, opts...)
			obj.s.SetIdentifiers(a.UID)
		case "new":
			// Upstream `NewSession(node, mocks.NewMockConnection(), url, headers, uid, opts...)`.
			obj.s = node.NewSession(o.n, conn, a.URL, a.Headers, a.UID, opts...)
		default:
			return nil, fmt.Errorf("unknown session mode %q", a.Mode)
		}
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
	register("node.Session", recovered(func(raw json.RawMessage) (interface{}, error) {
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
		case "MergeChannelState":
			s.GetEnv().MergeChannelState(c.Channel, c.State)
			return nil, nil
		case "MergeConnectionState":
			s.GetEnv().MergeConnectionState(c.State)
			return nil, nil
		case "RemoveChannelState":
			s.GetEnv().RemoveChannelState(c.Channel)
			return nil, nil
		case "MarkDisconnectable":
			s.MarkDisconnectable(c.Val)
			return nil, nil
		case "IsDisconnectable":
			return s.IsDisconnectable(), nil
		case "MarkUnrecoverable":
			s.MarkUnrecoverable()
			return nil, nil
		case "Send":
			var reply common.Reply
			if err := json.Unmarshal(c.Reply, &reply); err != nil {
				return nil, err
			}
			s.Send(&reply)
			return nil, nil
		case "SendJSONTransmission":
			s.SendJSONTransmission(c.Data)
			return nil, nil
		case "Disconnect":
			s.Disconnect(c.Reason, c.Code)
			return nil, nil
		case "DisconnectNow":
			s.DisconnectNow(c.Reason, c.Code)
			return nil, nil
		case "DisconnectNowAsync":
			go s.DisconnectNow(c.Reason, c.Code)
			return nil, nil
		case "SetResumeInterval":
			// WithKeepaliveIntervals(resume, 0): mock sessions keep no presence interval.
			node.WithKeepaliveIntervals(ms(c.Timeout, 0), 0)(s)
			return nil, nil
		case "Subscriptions":
			return subscriptionsOf(s)
		case "AddChannel", "AddChannelStream":
			subs, err := subscriptionsOf(s)
			if err != nil {
				return nil, err
			}
			if subs == nil {
				subs = map[string][]string{}
			}
			if c.Method == "AddChannel" {
				subs[c.Channel] = []string{}
			} else if streams, ok := subs[c.Channel]; ok {
				subs[c.Channel] = append(streams, c.Stream)
			}
			return nil, restoreSubscriptions(s, subs)
		case "ToCacheEntry":
			data, cerr := s.ToCacheEntry()
			return readReply(data, cerr), nil
		case "RestoreFromCache":
			return map[string]interface{}{"error": errString(s.RestoreFromCache([]byte(c.Data)))}, nil
		case "AwaitWritesHeld":
			if o.gate == nil {
				return nil, fmt.Errorf("session %d has no write gate", c.ID)
			}
			timeout := time.After(ms(c.Timeout, 10*time.Second))
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
		}
		return nil, fmt.Errorf("unknown Session method %q", c.Method)
	}))

	// TestMergeEnv's two concurrent MergeEnv calls.
	register("node.Session.MergeEnvConcurrently", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			ID   uint64       `json:"id"`
			Envs []sessionEnv `json:"envs"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		o, err := nodeGet[*sessionObj](a.ID)
		if err != nil {
			return nil, err
		}
		var wg sync.WaitGroup
		for _, e := range a.Envs {
			env := e.common()
			wg.Add(1)
			go func() {
				o.s.MergeEnv(env)
				wg.Done()
			}()
		}
		wg.Wait()
		return nil, nil
	})

	// TestSendRaceConditions' scaffolding: 10 sessions of a node with readers
	// draining their connections, each sent four frames from nested goroutines
	// (sendFrame(TextFrame) is SendJSONTransmission with the JSON encoder).
	register("node.SendRaceConditions", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Node    uint64 `json:"node"`
			Timeout *int64 `json:"timeout_ms"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		o, err := nodeGet[*nodeObj](a.Node)
		if err != nil {
			return nil, err
		}
		var wg sync.WaitGroup
		for i := 1; i <= 10; i++ {
			mc := mocks.NewMockConnection()
			s := node.BuildSession(mc, common.NewSessionEnv("/cable-test", &map[string]string{}))
			node.WithExecutor(o.n)(s)
			s.SetID("123")
			s.Log = slog.With("sid", "123")
			go s.SendMessages()

			go func() {
				for {
					mc.Read() // nolint:errcheck
				}
			}()

			for _, pair := range [][2]string{{"hi!", "bye"}, {"bye", "why"}} {
				wg.Add(2)
				go func() {
					for _, p := range pair {
						go func() {
							s.SendJSONTransmission(p)
							wg.Done()
						}()
					}
				}()
			}
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			return true, nil
		case <-time.After(ms(a.Timeout, 10*time.Second)):
			return false, nil
		}
	})

	// TestAuthenticatedRacesWithDisconnect's scaffolding: for each of n mock
	// sessions, Authenticated and Disconnect start together; returns the
	// session handles once both calls of every session returned.
	register("node.AuthenticatedRacesWithDisconnect", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Node uint64 `json:"node"`
			N    int    `json:"n"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		o, err := nodeGet[*nodeObj](a.Node)
		if err != nil {
			return nil, err
		}
		handles := []uint64{}
		var wg sync.WaitGroup
		for i := 0; i < a.N; i++ {
			uid := fmt.Sprintf("s%d", i)
			mc := mocks.NewMockConnection()
			s := node.BuildSession(mc, common.NewSessionEnv("/cable-test", &map[string]string{}))
			node.WithExecutor(o.n)(s)
			s.SetID(uid)
			s.Log = slog.With("sid", uid)
			s.SetIdentifiers(uid)
			go s.SendMessages()
			handles = append(handles, nodePut(&sessionObj{s: s, conn: mc, node: o}))

			start := make(chan struct{})
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				o.n.Authenticated(s, s.GetID())
			}()
			go func() {
				defer wg.Done()
				<-start
				s.Disconnect("Ping failed", ws.CloseAbnormalClosure)
			}()
			close(start)
		}
		wg.Wait()
		return handles, nil
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
	register("node.DisconnectQueue", recovered(func(raw json.RawMessage) (interface{}, error) {
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
			return errReply(q.Enqueue(s.s)), nil
		case "Size":
			return map[string]interface{}{"size": q.Size()}, nil
		case "Shutdown":
			ctx := context.Background()
			if c.Timeout != nil {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(*c.Timeout)*time.Millisecond)
				defer cancel()
			}
			return errReply(q.Shutdown(ctx)), nil
		case "Run":
			go q.Run() // nolint:errcheck
			return nil, nil
		case "RunUntilCalls":
			// TestDisconnectQueue_Run/Respects_the_rate...: start := time.Now();
			// go q.Run(); require.Eventually(calls == n, timeout, 1ms); time.Since(start).
			mo, err := nodeGet[*mockObj](c.Controller)
			if err != nil {
				return nil, err
			}
			start := time.Now()
			go q.Run() // nolint:errcheck
			deadline := start.Add(ms(c.Timeout, 2*time.Second))
			for int(mo.gate.calls.Load()) < c.N {
				if time.Now().After(deadline) {
					return map[string]interface{}{"reached": false, "elapsed_ms": time.Since(start).Milliseconds()}, nil
				}
				time.Sleep(time.Millisecond)
			}
			return map[string]interface{}{"reached": true, "elapsed_ms": time.Since(start).Milliseconds()}, nil
		case "ShutdownWhileEnqueueing":
			// TestDisconnectQueue_Shutdown/Does_not_block...: 8 goroutines keep
			// enqueueing their own session while Shutdown runs (5s limit).
			o, err := nodeGet[*nodeObj](c.Session)
			if err != nil {
				return nil, err
			}
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for j := 0; j < 8; j++ {
				uid := "c-" + strconv.Itoa(j)
				s := node.BuildSession(mocks.NewMockConnection(), common.NewSessionEnv("/cable-test", &map[string]string{}))
				node.WithExecutor(o.n)(s)
				s.SetID(uid)
				s.Log = slog.With("sid", uid)
				s.SetIdentifiers(uid)
				go s.SendMessages()
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						q.Enqueue(s) // nolint:errcheck
					}
				}()
			}
			done := make(chan error, 1)
			go func() { done <- q.Shutdown(context.Background()) }()
			var res map[string]interface{}
			select {
			case err := <-done:
				res = map[string]interface{}{"blocked": false, "error": errString(err)}
			case <-time.After(5 * time.Second):
				res = map[string]interface{}{"blocked": true, "error": nil}
			}
			close(stop)
			wg.Wait()
			return res, nil
		}
		return nil, fmt.Errorf("unknown DisconnectQueue method %q", c.Method)
	}))
}
