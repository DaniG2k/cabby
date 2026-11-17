package main

import (
	"encoding/json"

	"github.com/anycable/anycable-go/protocol"
	pb "github.com/anycable/anycable-go/protos"
)

func init() {
	register("protocol.NewConnectMessage", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Env sessionEnv }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return protocol.NewConnectMessage(a.Env.common()), nil
	})
	register("protocol.NewCommandMessage", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Env                                 sessionEnv
			Command, Channel, Identifiers, Data string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return protocol.NewCommandMessage(a.Env.common(), a.Command, a.Channel, a.Identifiers, a.Data), nil
	})
	register("protocol.NewDisconnectMessage", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Env           sessionEnv
			Identifiers   string
			Subscriptions []string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return protocol.NewDisconnectMessage(a.Env.common(), a.Identifiers, a.Subscriptions), nil
	})
	register("protocol.ParseConnectResponse", func(raw json.RawMessage) (interface{}, error) {
		var r pb.ConnectionResponse
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		res, err := protocol.ParseConnectResponse(&r)
		return parsed{res, errString(err)}, nil
	})
	register("protocol.ParseCommandResponse", func(raw json.RawMessage) (interface{}, error) {
		var r pb.CommandResponse
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		res, err := protocol.ParseCommandResponse(&r)
		return parsed{res, errString(err)}, nil
	})
	register("protocol.ParseDisconnectResponse", func(raw json.RawMessage) (interface{}, error) {
		var r pb.DisconnectResponse
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		return parsed{nil, errString(protocol.ParseDisconnectResponse(&r))}, nil
	})
}
