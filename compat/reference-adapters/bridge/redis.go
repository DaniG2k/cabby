package main

// Ops for upstream package `redis`. A config lives behind a handle because its
// methods mutate unexported state (resolved hosts): every call first copies the
// exported fields sent by the test onto the handle, then calls the method.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	rconfig "github.com/anycable/anycable-go/redis"
)

type redisOptionsTLS struct {
	InsecureSkipVerify bool
	Certificates       int
	RootCAs            bool
}

// redisOptions is the subset of rueidis.ClientOption that upstream tests read.
type redisOptions struct {
	InitAddress   []string
	SelectDB      int
	Username      string
	Password      string
	KeepAliveNs   int64
	DialTimeoutNs int64
	ShuffleInit   bool
	TLS           *redisOptionsTLS
	Sentinel      struct{ MasterSet, Username, Password string }
}

type redisStreamHandler struct {
	received chan map[string]string

	// Upstream TestStreamerAcksClaims: on the first entry with this payload,
	// the handler closes the streamer's client (`streamer.Client().Close()`)
	// after delivering the entry, so it is never acknowledged.
	mu       sync.Mutex
	closeOn  string
	streamer *rconfig.Streamer
}

func (h *redisStreamHandler) handle(msg map[string]string) error {
	h.received <- msg
	h.mu.Lock()
	var s *rconfig.Streamer
	if h.streamer != nil && msg["payload"] == h.closeOn {
		s = h.streamer
		h.streamer = nil
	}
	h.mu.Unlock()
	if s != nil {
		s.Client().Close()
	}
	return nil
}

type redisStreamer struct {
	streamer *rconfig.Streamer
}

func init() {
	register("redis.NewRedisConfig", func(json.RawMessage) (interface{}, error) {
		c := rconfig.NewRedisConfig()
		return map[string]interface{}{"Handle": msgPut(&c), "Config": &c}, nil
	})
	register("redis.ConfigCall", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Handle int64
			Config json.RawMessage
			Method string
		}](raw)
		if err != nil {
			return nil, err
		}
		c, err := msgGet[*rconfig.RedisConfig](a.Handle)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(a.Config, c); err != nil {
			return nil, err
		}
		switch a.Method {
		case "ToRueidisOptions":
			o, oerr := c.ToRueidisOptions()
			if oerr != nil {
				return parsed{nil, errString(oerr)}, nil
			}
			res := redisOptions{
				InitAddress: o.InitAddress, SelectDB: o.SelectDB, Username: o.Username, Password: o.Password,
				KeepAliveNs: int64(o.Dialer.KeepAlive), DialTimeoutNs: int64(o.Dialer.Timeout), ShuffleInit: o.ShuffleInit,
			}
			if o.TLSConfig != nil {
				res.TLS = &redisOptionsTLS{
					InsecureSkipVerify: o.TLSConfig.InsecureSkipVerify,
					Certificates:       len(o.TLSConfig.Certificates),
					RootCAs:            o.TLSConfig.RootCAs != nil,
				}
			}
			res.Sentinel.MasterSet = o.Sentinel.MasterSet
			res.Sentinel.Username = o.Sentinel.Username
			res.Sentinel.Password = o.Sentinel.Password
			return parsed{res, nil}, nil
		case "IsCluster":
			return c.IsCluster(), nil
		case "IsSentinel":
			return c.IsSentinel(), nil
		case "Hostname":
			return c.Hostname(), nil
		case "Hostnames":
			return c.Hostnames(), nil
		case "TLSClientCertAvailable":
			return c.TLSClientCertAvailable(), nil
		case "TLSVerificationSkipped":
			return c.TLSVerificationSkipped(), nil
		case "ToToml":
			return c.ToToml(), nil
		}
		return nil, fmt.Errorf("unknown method %q", a.Method)
	})
	register("redis.RedisConfigDecodeToml", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Config json.RawMessage
			Toml   string
		}](raw)
		if err != nil {
			return nil, err
		}
		var c rconfig.RedisConfig
		if err := json.Unmarshal(a.Config, &c); err != nil {
			return nil, err
		}
		_, derr := toml.Decode(a.Toml, &c)
		return parsed{&c, errString(derr)}, nil
	})

	register("redis.NewStreamHandler", func(json.RawMessage) (interface{}, error) {
		// Capacity 10, as upstream's `received := make(chan map[string]string, 10)`.
		return msgPut(&redisStreamHandler{received: make(chan map[string]string, 10)}), nil
	})
	// Upstream drainStream: collects until `Count` messages arrived or no message
	// arrived for `TimeoutMs`.
	register("redis.StreamHandlerDrain", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Handler, Count, TimeoutMs int64 }](raw)
		if err != nil {
			return nil, err
		}
		h, err := msgGet[*redisStreamHandler](a.Handler)
		if err != nil {
			return nil, err
		}
		buffer := []map[string]string{}
		for {
			select {
			case msg := <-h.received:
				buffer = append(buffer, msg)
				if int64(len(buffer)) == a.Count {
					return buffer, nil
				}
			case <-time.After(msgTimeout(a.TimeoutMs)):
				return buffer, nil
			}
		}
	})
	register("redis.NewStreamer", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Stream, Group string
			Config        json.RawMessage
			Handler       int64
			BlockMs       int64
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := msgGet[*redisStreamHandler](a.Handler)
		if err != nil {
			return nil, err
		}
		c := rconfig.NewRedisConfig()
		if err := json.Unmarshal(a.Config, &c); err != nil {
			return nil, err
		}
		s := rconfig.NewStreamer(a.Stream, a.Group, &c, slog.Default(),
			rconfig.StreamerWithHandler(h.handle), rconfig.StreamerWithBlockMS(a.BlockMs))
		return msgPut(&redisStreamer{streamer: s}), nil
	})
	register("redis.StreamHandlerCloseClientOn", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Handler, Streamer int64
			Payload           string
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := msgGet[*redisStreamHandler](a.Handler)
		if err != nil {
			return nil, err
		}
		s, err := msgGet[*redisStreamer](a.Streamer)
		if err != nil {
			return nil, err
		}
		h.mu.Lock()
		h.closeOn, h.streamer = a.Payload, s.streamer
		h.mu.Unlock()
		return true, nil
	})
	streamerOf := func(raw json.RawMessage) (*redisStreamer, int64, error) {
		a, err := msgArgs[struct{ Streamer, TimeoutMs int64 }](raw)
		if err != nil {
			return nil, 0, err
		}
		s, err := msgGet[*redisStreamer](a.Streamer)
		return s, a.TimeoutMs, err
	}
	register("redis.StreamerStart", func(raw json.RawMessage) (interface{}, error) {
		s, _, err := streamerOf(raw)
		if err != nil {
			return nil, err
		}
		return parsed{nil, errString(s.streamer.Start())}, nil
	})
	register("redis.StreamerShutdown", func(raw json.RawMessage) (interface{}, error) {
		s, _, err := streamerOf(raw)
		if err != nil {
			return nil, err
		}
		return parsed{nil, errString(s.streamer.Shutdown(context.Background()))}, nil
	})
	// `<-streamer.Ready()` with a deadline.
	register("redis.StreamerReady", func(raw json.RawMessage) (interface{}, error) {
		s, timeout, err := streamerOf(raw)
		if err != nil {
			return nil, err
		}
		select {
		case <-s.streamer.Ready():
			return true, nil
		case <-time.After(msgTimeout(timeout)):
			return false, nil
		}
	})
}
