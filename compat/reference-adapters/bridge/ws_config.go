package main

import (
	"encoding/json"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/ws"
)

func init() {
	register("ws.NewConfig", func(json.RawMessage) (interface{}, error) {
		return ws.NewConfig(), nil
	})
	register("ws.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Config ws.Config }](raw)
		if err != nil {
			return nil, err
		}
		return a.Config.ToToml(), nil
	})
	register("ws.Config.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Config ws.Config
			Toml   string
		}](raw)
		if err != nil {
			return nil, err
		}
		conf := a.Config
		_, derr := toml.Decode(a.Toml, &conf)
		return parsed{conf, errString(derr)}, nil
	})
}
