package main

// Bridge ops for upstream package rpc. The Go controller and HTTP service are
// exercised for real against the Rust fake RPC application
// (crates/cabby-testkit/src/fake_rpc.rs) over loopback sockets.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/metrics"
	pb "github.com/anycable/anycable-go/protos"
	"github.com/anycable/anycable-go/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// rpcConfig mirrors rpc.Config without DialFun (a func, not serializable).
type rpcConfig struct {
	Host               string
	ProxyHeaders       []string
	ProxyCookies       []string
	Concurrency        int
	EnableTLS          bool
	TLSVerify          bool
	TLSRootCA          string
	MaxRecvSize        int
	MaxSendSize        int
	Implementation     string
	Secret             string
	RequestTimeout     int
	CommandTimeout     int
	WaitTimeout        int
	HTTPRequestTimeout int
	SecretBase         string
}

func toRPCConfig(c rpc.Config) rpcConfig {
	return rpcConfig{c.Host, c.ProxyHeaders, c.ProxyCookies, c.Concurrency, c.EnableTLS, c.TLSVerify, c.TLSRootCA,
		c.MaxRecvSize, c.MaxSendSize, c.Implementation, c.Secret, c.RequestTimeout, c.CommandTimeout, c.WaitTimeout,
		c.HTTPRequestTimeout, c.SecretBase}
}

func (c rpcConfig) rpc() rpc.Config {
	return rpc.Config{Host: c.Host, ProxyHeaders: c.ProxyHeaders, ProxyCookies: c.ProxyCookies,
		Concurrency: c.Concurrency, EnableTLS: c.EnableTLS, TLSVerify: c.TLSVerify, TLSRootCA: c.TLSRootCA,
		MaxRecvSize: c.MaxRecvSize, MaxSendSize: c.MaxSendSize, Implementation: c.Implementation, Secret: c.Secret,
		RequestTimeout: c.RequestTimeout, CommandTimeout: c.CommandTimeout, WaitTimeout: c.WaitTimeout,
		HTTPRequestTimeout: c.HTTPRequestTimeout, SecretBase: c.SecretBase}
}

func incomingMetadata(ctx context.Context, pairs [][2]string) context.Context {
	kv := make([]string, 0, 2*len(pairs))
	for _, p := range pairs {
		kv = append(kv, p[0], p[1])
	}
	return metadata.NewIncomingContext(ctx, metadata.Pairs(kv...))
}

// readyHelper is always ready (the upstream tests' MockState{ready: true}).
type readyHelper struct{ conn *grpc.ClientConn }

func (readyHelper) Ready() error              { return nil }
func (readyHelper) SupportsActiveConns() bool { return false }
func (readyHelper) ActiveConns() int          { return 0 }
func (h readyHelper) Close() {
	if h.conn != nil {
		h.conn.Close()
	}
}

// forwardingService serves in-process calls by passing them, with the incoming
// metadata, to the Rust fake over gRPC (where the test inspects them).
type forwardingService struct {
	pb.UnimplementedRPCServer
	client pb.RPCClient
}

func forward(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	return metadata.NewOutgoingContext(ctx, md)
}

func (s *forwardingService) Connect(ctx context.Context, r *pb.ConnectionRequest) (*pb.ConnectionResponse, error) {
	return s.client.Connect(forward(ctx), r)
}

func (s *forwardingService) Command(ctx context.Context, r *pb.CommandMessage) (*pb.CommandResponse, error) {
	return s.client.Command(forward(ctx), r)
}

func (s *forwardingService) Disconnect(ctx context.Context, r *pb.DisconnectRequest) (*pb.DisconnectResponse, error) {
	return s.client.Disconnect(forward(ctx), r)
}

func dialTarget(target string) (*grpc.ClientConn, pb.RPCClient, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewRPCClient(conn), nil
}

type handleOrErr struct {
	Handle int      `json:"handle"`
	Error  *goError `json:"error"`
}

type resultOrErr struct {
	Result interface{} `json:"result"`
	Error  *goError    `json:"error"`
}

func init() {
	register("rpc.NewConfig", func(json.RawMessage) (interface{}, error) {
		return toRPCConfig(rpc.NewConfig()), nil
	})
	register("rpc.Config", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config rpcConfig
			Op     string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		c := a.Config.rpc()
		switch a.Op {
		case "Impl":
			return c.Impl(), nil
		case "TLSEnabled":
			return c.TLSEnabled(), nil
		case "GetHTTPRequestTimeout":
			return c.GetHTTPRequestTimeout(), nil
		case "ToToml":
			return c.ToToml(), nil
		}
		return nil, fmt.Errorf("unknown rpc.Config op %q", a.Op)
	})
	// Decodes a TOML document on top of a config, as upstream config files are
	// decoded on top of the defaults (toml.Decode into the struct).
	register("rpc.Config.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Base rpcConfig
			Toml string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		c := a.Base.rpc()
		_, err := toml.Decode(a.Toml, &c)
		return resultOrErr{toRPCConfig(c), describeErr(err)}, nil
	})

	register("rpc.NewController", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Metrics int
			Config  rpcConfig
			// "": config as given; "client": a plain gRPC client to Target with an
			// always-ready helper (upstream NewTestController's injected client and
			// MockState); "inprocess": NewInprocessServiceDialer over a service
			// forwarding to Target (upstream TestCustomDialFun).
			Dial   string
			Target string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		m, err := rpcmGet[*metrics.Metrics](a.Metrics)
		if err != nil {
			return nil, err
		}
		c := a.Config.rpc()
		switch a.Dial {
		case "":
		case "client":
			conn, client, err := dialTarget(a.Target)
			if err != nil {
				return handleOrErr{Error: describeErr(err)}, nil
			}
			c.DialFun = func(*rpc.Config, *slog.Logger) (pb.RPCClient, rpc.ClientHelper, error) {
				return client, readyHelper{conn}, nil
			}
		case "inprocess":
			conn, client, err := dialTarget(a.Target)
			if err != nil {
				return handleOrErr{Error: describeErr(err)}, nil
			}
			c.DialFun = rpc.NewInprocessServiceDialer(&forwardingService{client: client}, readyHelper{conn})
		default:
			return nil, fmt.Errorf("unknown dial kind %q", a.Dial)
		}
		ctrl, err := rpc.NewController(m, &c, rpcmLogger())
		if err != nil {
			return handleOrErr{Error: describeErr(err)}, nil
		}
		return handleOrErr{Handle: rpcmPut(ctrl)}, nil
	})
	register("rpc.Controller.Start", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Handle int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		ctrl, err := rpcmGet[*rpc.Controller](a.Handle)
		if err != nil {
			return nil, err
		}
		return resultOrErr{Error: describeErr(ctrl.Start())}, nil
	})
	// Shutdown waits for in-flight calls, so it runs asynchronously (rpcm.Await).
	register("rpc.Controller.Shutdown", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Handle int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		ctrl, err := rpcmGet[*rpc.Controller](a.Handle)
		if err != nil {
			return nil, err
		}
		return rpcmStart(func() interface{} { return resultOrErr{Error: describeErr(ctrl.Shutdown())} }), nil
	})
	// Starts a controller call in a goroutine; collect it with rpcm.Await.
	register("rpc.Controller.Call", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Handle, Ctx                    int
			Method, Sid, ID, Channel, Data string
			Env                            sessionEnv
			Subscriptions                  []string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		ctrl, err := rpcmGet[*rpc.Controller](a.Handle)
		if err != nil {
			return nil, err
		}
		ctx, err := rpcmCtx(a.Ctx)
		if err != nil {
			return nil, err
		}
		env := a.Env.common()
		var call func() interface{}
		switch a.Method {
		case "Authenticate":
			call = func() interface{} {
				res, err := ctrl.Authenticate(ctx, a.Sid, env)
				return resultOrErr{res, describeErr(err)}
			}
		case "Subscribe":
			call = func() interface{} {
				res, err := ctrl.Subscribe(ctx, a.Sid, env, a.ID, a.Channel)
				return resultOrErr{res, describeErr(err)}
			}
		case "Unsubscribe":
			call = func() interface{} {
				res, err := ctrl.Unsubscribe(ctx, a.Sid, env, a.ID, a.Channel)
				return resultOrErr{res, describeErr(err)}
			}
		case "Perform":
			call = func() interface{} {
				res, err := ctrl.Perform(ctx, a.Sid, env, a.ID, a.Channel, a.Data)
				return resultOrErr{res, describeErr(err)}
			}
		case "Disconnect":
			call = func() interface{} {
				return resultOrErr{nil, describeErr(ctrl.Disconnect(ctx, a.Sid, env, a.ID, a.Subscriptions))}
			}
		default:
			return nil, fmt.Errorf("unknown controller method %q", a.Method)
		}
		return rpcmStart(call), nil
	})

	register("rpc.NewHTTPService", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config rpcConfig }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		c := a.Config.rpc()
		s, err := rpc.NewHTTPService(&c)
		if err != nil {
			return handleOrErr{Error: describeErr(err)}, nil
		}
		return handleOrErr{Handle: rpcmPut(s)}, nil
	})
	// Starts an HTTPService call in a goroutine; collect it with rpcm.Await.
	register("rpc.HTTPService.Call", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Handle, Ctx int
			Method      string
			Request     json.RawMessage
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		s, err := rpcmGet[*rpc.HTTPService](a.Handle)
		if err != nil {
			return nil, err
		}
		ctx, err := rpcmCtx(a.Ctx)
		if err != nil {
			return nil, err
		}
		var call func() interface{}
		switch a.Method {
		case "Connect":
			var r pb.ConnectionRequest
			if err := json.Unmarshal(a.Request, &r); err != nil {
				return nil, err
			}
			call = func() interface{} { res, err := s.Connect(ctx, &r); return resultOrErr{res, describeErr(err)} }
		case "Command":
			var r pb.CommandMessage
			if err := json.Unmarshal(a.Request, &r); err != nil {
				return nil, err
			}
			call = func() interface{} { res, err := s.Command(ctx, &r); return resultOrErr{res, describeErr(err)} }
		case "Disconnect":
			var r pb.DisconnectRequest
			if err := json.Unmarshal(a.Request, &r); err != nil {
				return nil, err
			}
			call = func() interface{} { res, err := s.Disconnect(ctx, &r); return resultOrErr{res, describeErr(err)} }
		default:
			return nil, fmt.Errorf("unknown HTTPService method %q", a.Method)
		}
		return rpcmStart(call), nil
	})
	register("rpc.HTTPClientHelper.Ready", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Handle int }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		s, err := rpcmGet[*rpc.HTTPService](a.Handle)
		if err != nil {
			return nil, err
		}
		return resultOrErr{Error: describeErr(rpc.NewHTTPClientHelper(s).Ready())}, nil
	})
}
