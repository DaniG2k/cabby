package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/streams"
)

type streamsCtorArg struct {
	Kind    string         `json:"kind"`
	Key     string         `json:"key"`
	Config  streams.Config `json:"config"`
	Request struct {
		StreamName       string `json:"stream_name"`
		SignedStreamName string `json:"signed_stream_name"`
	} `json:"request"`
}

func (a streamsCtorArg) build() (*streams.Controller, error) {
	switch a.Kind {
	case "fixed":
		// The test's resolver: always returns the given request.
		req := a.Request
		return streams.NewController(a.Key, func(string) (*streams.SubscribeRequest, error) {
			return &streams.SubscribeRequest{StreamName: req.StreamName, SignedStreamName: req.SignedStreamName}, nil
		}, slog.Default()), nil
	case "streams":
		conf := a.Config
		return streams.NewStreamsController(&conf, slog.Default()), nil
	case "turbo":
		return streams.NewTurboController(a.Key, slog.Default()), nil
	case "cable_ready":
		return streams.NewCableReadyController(a.Key, slog.Default()), nil
	}
	return nil, fmt.Errorf("unknown streams controller kind %q", a.Kind)
}

func init() {
	register("streams.NewConfig", func(json.RawMessage) (interface{}, error) {
		return streams.NewConfig(), nil
	})
	register("streams.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config streams.Config }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return a.Config.ToToml(), nil
	})
	register("streams.Config.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config streams.Config
			Toml   string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config
		_, err := toml.Decode(a.Toml, &conf)
		return map[string]interface{}{"config": conf, "error": errString(err)}, nil
	})
	register("streams.Controller", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Ctor       streamsCtorArg `json:"ctor"`
			Method     string         `json:"method"`
			Sid        string         `json:"sid"`
			Env        *sessionEnv    `json:"env"`
			Ids        string         `json:"ids"`
			Identifier string         `json:"identifier"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		c, err := a.Ctor.build()
		if err != nil {
			return nil, err
		}
		ctx := context.Background()
		switch a.Method {
		case "Subscribe":
			res, err := c.Subscribe(ctx, a.Sid, a.Env.commonOrNil(), a.Ids, a.Identifier)
			return pairResult{nilPtr(res), errString(err)}, nil
		case "Unsubscribe":
			res, err := c.Unsubscribe(ctx, a.Sid, a.Env.commonOrNil(), a.Ids, a.Identifier)
			return pairResult{nilPtr(res), errString(err)}, nil
		}
		return nil, fmt.Errorf("unknown streams controller method %q", a.Method)
	})
}
