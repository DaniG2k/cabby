package main

// Handle registry for stateful Go objects (hubs, sessions, subscribers,
// broadcasters, ...) used by the hub/pubsub/redis/broadcast/nats/enats ops.
// Rust facades receive an integer handle and pass it back on later calls.

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
)

var msgHandles = struct {
	sync.Mutex
	next int64
	m    map[int64]interface{}
}{m: map[int64]interface{}{}}

func msgPut(v interface{}) int64 {
	msgHandles.Lock()
	defer msgHandles.Unlock()
	msgHandles.next++
	msgHandles.m[msgHandles.next] = v
	return msgHandles.next
}

func msgGet[T any](id int64) (T, error) {
	msgHandles.Lock()
	defer msgHandles.Unlock()
	var zero T
	v, ok := msgHandles.m[id]
	if !ok {
		return zero, fmt.Errorf("unknown handle %d", id)
	}
	t, ok := v.(T)
	if !ok {
		return zero, fmt.Errorf("handle %d has type %T, expected %T", id, v, zero)
	}
	return t, nil
}

func msgArgs[T any](raw json.RawMessage) (T, error) {
	var a T
	err := json.Unmarshal(raw, &a)
	return a, err
}

func msgTimeout(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

// msgConfigOps registers, for a config type T of an upstream package:
//
//	<prefix>New      -> newFn()
//	<prefix>ToToml   {Config} -> Config.ToToml()
//	<prefix>DecodeToml {Config, Toml} -> toml.Decode(Toml, &Config), {result, error}
func msgConfigOps[T any](prefix string, newFn func() T, toToml func(T) string) {
	if newFn != nil {
		register(prefix+"New", func(json.RawMessage) (interface{}, error) { return newFn(), nil })
	}
	register(prefix+"ToToml", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Config T }](raw)
		if err != nil {
			return nil, err
		}
		return toToml(a.Config), nil
	})
	register(prefix+"DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct {
			Config T
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

func init() {
	register("msg.Release", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Handles []int64 }](raw)
		if err != nil {
			return nil, err
		}
		msgHandles.Lock()
		defer msgHandles.Unlock()
		for _, h := range a.Handles {
			delete(msgHandles.m, h)
		}
		return true, nil
	})
}
