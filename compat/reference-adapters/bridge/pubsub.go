package main

// Ops for upstream package `pubsub`. The handler is the upstream TestHandler
// (pubsub/subscriber_test.go): buffered channels of capacity 10 and receives
// with a deadline. Subscribers are the real Go implementations connected to the
// Redis/NATS servers named in their configs.

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/pubsub"
)

type pubsubHandler struct {
	messages chan *common.StreamMessage
	commands chan *common.RemoteCommandMessage
}

var _ pubsub.Handler = (*pubsubHandler)(nil)

func (h *pubsubHandler) Broadcast(msg *common.StreamMessage)                   { h.messages <- msg }
func (h *pubsubHandler) ExecuteRemoteCommand(cmd *common.RemoteCommandMessage) { h.commands <- cmd }

type pubsubSubscriber struct {
	sub  pubsub.Subscriber
	done chan error
}

func init() {
	handlerOf := func(id int64) (*pubsubHandler, error) { return msgGet[*pubsubHandler](id) }
	subOf := func(id int64) (*pubsubSubscriber, error) { return msgGet[*pubsubSubscriber](id) }

	register("pubsub.NewTestHandler", func(json.RawMessage) (interface{}, error) {
		return msgPut(&pubsubHandler{
			messages: make(chan *common.StreamMessage, 10),
			commands: make(chan *common.RemoteCommandMessage, 10),
		}), nil
	})
	register("pubsub.HandlerReceive", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Handler, TimeoutMs int64 }](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		select {
		case <-time.After(msgTimeout(a.TimeoutMs)):
			return []interface{}{}, nil
		case msg := <-h.messages:
			return []interface{}{msg}, nil
		}
	})
	register("pubsub.HandlerReceiveCommand", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Handler, TimeoutMs int64 }](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		select {
		case <-time.After(msgTimeout(a.TimeoutMs)):
			return []interface{}{}, nil
		case cmd := <-h.commands:
			return []interface{}{cmd}, nil
		}
	})
	register("pubsub.NewLegacySubscriber", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Handler int64 }](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		return msgPut(&pubsubSubscriber{sub: pubsub.NewLegacySubscriber(h)}), nil
	})
	register("pubsub.NewRedisSubscriber", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Handler int64
			Config  pubsub.RedisConfig
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		config := a.Config
		s, serr := pubsub.NewRedisSubscriber(h, &config, slog.Default())
		if serr != nil {
			return parsed{nil, errString(serr)}, nil
		}
		return parsed{msgPut(&pubsubSubscriber{sub: s}), nil}, nil
	})
	register("pubsub.NewNATSSubscriber", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Handler int64
			Config  pubsub.NATSConfig
		}](raw)
		if err != nil {
			return nil, err
		}
		h, err := handlerOf(a.Handler)
		if err != nil {
			return nil, err
		}
		config := a.Config
		s, serr := pubsub.NewNATSSubscriber(h, &config, slog.Default())
		if serr != nil {
			return parsed{nil, errString(serr)}, nil
		}
		return parsed{msgPut(&pubsubSubscriber{sub: s}), nil}, nil
	})
	type subArgs struct {
		Subscriber int64
		Stream     string
		Msg        common.StreamMessage
		Cmd        common.RemoteCommandMessage
	}
	withSub := func(raw json.RawMessage, f func(*pubsubSubscriber, subArgs) interface{}) (interface{}, error) {
		a, err := msgArgs[subArgs](raw)
		if err != nil {
			return nil, err
		}
		s, err := subOf(a.Subscriber)
		if err != nil {
			return nil, err
		}
		return f(s, a), nil
	}
	register("pubsub.Start", func(raw json.RawMessage) (interface{}, error) {
		return withSub(raw, func(s *pubsubSubscriber, _ subArgs) interface{} {
			// Unbuffered, as upstream's `done := make(chan error)`.
			s.done = make(chan error)
			return parsed{nil, errString(s.sub.Start(s.done))}
		})
	})
	register("pubsub.Shutdown", func(raw json.RawMessage) (interface{}, error) {
		return withSub(raw, func(s *pubsubSubscriber, _ subArgs) interface{} {
			return parsed{nil, errString(s.sub.Shutdown(context.Background()))}
		})
	})
	register("pubsub.Subscribe", func(raw json.RawMessage) (interface{}, error) {
		return withSub(raw, func(s *pubsubSubscriber, a subArgs) interface{} { s.sub.Subscribe(a.Stream); return true })
	})
	register("pubsub.Unsubscribe", func(raw json.RawMessage) (interface{}, error) {
		return withSub(raw, func(s *pubsubSubscriber, a subArgs) interface{} { s.sub.Unsubscribe(a.Stream); return true })
	})
	register("pubsub.Broadcast", func(raw json.RawMessage) (interface{}, error) {
		return withSub(raw, func(s *pubsubSubscriber, a subArgs) interface{} {
			msg := a.Msg
			s.sub.Broadcast(&msg)
			return true
		})
	})
	register("pubsub.BroadcastCommand", func(raw json.RawMessage) (interface{}, error) {
		return withSub(raw, func(s *pubsubSubscriber, a subArgs) interface{} {
			cmd := a.Cmd
			s.sub.BroadcastCommand(&cmd)
			return true
		})
	})
	register("pubsub.IsMultiNode", func(raw json.RawMessage) (interface{}, error) {
		return withSub(raw, func(s *pubsubSubscriber, _ subArgs) interface{} { return s.sub.IsMultiNode() })
	})

	msgConfigOps("pubsub.RedisConfig", pubsub.NewRedisConfig, pubsub.RedisConfig.ToToml)
	msgConfigOps("pubsub.NATSConfig", pubsub.NewNATSConfig, pubsub.NATSConfig.ToToml)
}
