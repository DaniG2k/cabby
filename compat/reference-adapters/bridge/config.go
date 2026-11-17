package main

import (
	"encoding/json"
	"log/slog"
	"os"

	"github.com/anycable/anycable-go/config"
	"github.com/anycable/anycable-go/logger"
	"github.com/anycable/anycable-go/server"
	"github.com/anycable/anycable-go/ws"
)

// configView is the subset of config.Config modelled by Cabby's config::Config.
// config.Config itself is not JSON-encodable (rpc.Config holds a func), so values
// are copied field by field. Inputs are applied over config.NewConfig().
type configView struct {
	Secret               string
	BroadcastAdapters    []string
	PubSubAdapter        string
	UserPresets          *[]string // nil: not set (detect presets from env)
	DisconnectorDisabled bool
	Log                  logger.Config
	Server               server.Config
	WS                   ws.Config
	App                  struct{ PingInterval int }
	RPC                  struct{ Host, Implementation string }
	Broker               struct{ Adapter string }
	Redis                struct{ URL string }
	HTTPBroadcast        struct{ Port int }
	Metrics              struct {
		HTTP string
		Tags map[string]string
	}
	EmbeddedNats struct {
		Enabled                                     bool
		Name, ServiceAddr, ClusterAddr, ClusterName string
		Routes                                      []string
	}
	SSE struct {
		Enabled, CommentPings bool
		PingInterval          int
	}
	DS                struct{ SSEPingInterval int }
	ConfigFilePath    string
	UnknownConfigKeys []string
}

func viewOfConfig(c *config.Config) configView {
	v := configView{
		Secret: c.Secret, BroadcastAdapters: c.BroadcastAdapters, PubSubAdapter: c.PubSubAdapter,
		DisconnectorDisabled: c.DisconnectorDisabled, Log: c.Log, Server: c.Server, WS: c.WS,
		ConfigFilePath: c.ConfigFilePath, UnknownConfigKeys: c.UnknownConfigKeys,
	}
	if c.UserPresets != nil {
		presets := c.UserPresets
		v.UserPresets = &presets
	}
	v.App.PingInterval = c.App.PingInterval
	v.RPC.Host, v.RPC.Implementation = c.RPC.Host, c.RPC.Implementation
	v.Broker.Adapter = c.Broker.Adapter
	v.Redis.URL = c.Redis.URL
	v.HTTPBroadcast.Port = c.HTTPBroadcast.Port
	v.Metrics.HTTP, v.Metrics.Tags = c.Metrics.HTTP, c.Metrics.Tags
	n := &c.EmbeddedNats
	v.EmbeddedNats.Enabled, v.EmbeddedNats.Name, v.EmbeddedNats.Routes = n.Enabled, n.Name, n.Routes
	v.EmbeddedNats.ServiceAddr, v.EmbeddedNats.ClusterAddr, v.EmbeddedNats.ClusterName = n.ServiceAddr, n.ClusterAddr, n.ClusterName
	v.SSE.Enabled, v.SSE.CommentPings, v.SSE.PingInterval = c.SSE.Enabled, c.SSE.CommentPings, c.SSE.PingInterval
	v.DS.SSEPingInterval = c.DS.SSEPingInterval
	return v
}

func (v configView) apply(c *config.Config) {
	c.Secret, c.BroadcastAdapters, c.PubSubAdapter = v.Secret, v.BroadcastAdapters, v.PubSubAdapter
	c.DisconnectorDisabled, c.Log, c.Server, c.WS = v.DisconnectorDisabled, v.Log, v.Server, v.WS
	c.ConfigFilePath, c.UnknownConfigKeys = v.ConfigFilePath, v.UnknownConfigKeys
	c.UserPresets = nil
	if v.UserPresets != nil {
		c.UserPresets = *v.UserPresets
	}
	c.App.PingInterval = v.App.PingInterval
	c.RPC.Host, c.RPC.Implementation = v.RPC.Host, v.RPC.Implementation
	c.Broker.Adapter = v.Broker.Adapter
	c.Redis.URL = v.Redis.URL
	c.HTTPBroadcast.Port = v.HTTPBroadcast.Port
	c.Metrics.HTTP, c.Metrics.Tags = v.Metrics.HTTP, v.Metrics.Tags
	n := &c.EmbeddedNats
	n.Enabled, n.Name, n.Routes = v.EmbeddedNats.Enabled, v.EmbeddedNats.Name, v.EmbeddedNats.Routes
	n.ServiceAddr, n.ClusterAddr, n.ClusterName = v.EmbeddedNats.ServiceAddr, v.EmbeddedNats.ClusterAddr, v.EmbeddedNats.ClusterName
	c.SSE.Enabled, c.SSE.CommentPings, c.SSE.PingInterval = v.SSE.Enabled, v.SSE.CommentPings, v.SSE.PingInterval
	c.DS.SSEPingInterval = v.DS.SSEPingInterval
}

func configFromArgs(raw json.RawMessage) (*config.Config, error) {
	a, err := bridgeArgs[struct{ Config configView }](raw)
	if err != nil {
		return nil, err
	}
	c := config.NewConfig()
	a.Config.apply(&c)
	return &c, nil
}

func init() {
	register("config.NewConfig", func(json.RawMessage) (interface{}, error) {
		c := config.NewConfig()
		return viewOfConfig(&c), nil
	})
	register("config.Config.LoadFromFile", func(raw json.RawMessage) (interface{}, error) {
		c, err := configFromArgs(raw)
		if err != nil {
			return nil, err
		}
		lerr := c.LoadFromFile()
		return parsed{viewOfConfig(c), errString(lerr)}, nil
	})
	register("config.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		c, err := configFromArgs(raw)
		if err != nil {
			return nil, err
		}
		return c.ToToml(), nil
	})
	// Presets() and LoadPresets read FLY_*, HEROKU_*, PORT and ANYCABLE_FLY_* from the
	// process environment: tests call them in a bridge process with its own environment.
	register("config.Config.Presets", func(raw json.RawMessage) (interface{}, error) {
		c, err := configFromArgs(raw)
		if err != nil {
			return nil, err
		}
		return c.Presets(), nil
	})
	register("config.Config.LoadPresets", func(raw json.RawMessage) (interface{}, error) {
		c, err := configFromArgs(raw)
		if err != nil {
			return nil, err
		}
		perr := c.LoadPresets(slog.New(slog.NewTextHandler(os.Stderr, nil)))
		return parsed{viewOfConfig(c), errString(perr)}, nil
	})
}
