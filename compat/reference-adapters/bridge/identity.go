package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/identity"
	"github.com/golang-jwt/jwt/v5"
)

// scriptedController is a double of node.Controller (upstream tests: mocks.Controller).
type scriptedController struct{ s *scripted }

func (c *scriptedController) Start() error {
	return decodeScriptedReturn(c.s.answer("Start", struct{}{})).err()
}

func (c *scriptedController) Shutdown() error {
	return decodeScriptedReturn(c.s.answer("Shutdown", struct{}{})).err()
}

func (c *scriptedController) Authenticate(_ context.Context, sid string, env *common.SessionEnv) (*common.ConnectResult, error) {
	r := decodeScriptedReturn(c.s.answer("Authenticate", map[string]interface{}{"sid": sid, "env": bridgeEnvOf(env)}))
	return r.connectResult(), r.err()
}

func (c *scriptedController) Subscribe(_ context.Context, sid string, env *common.SessionEnv, ids string, channel string) (*common.CommandResult, error) {
	r := decodeScriptedReturn(c.s.answer("Subscribe", map[string]interface{}{"sid": sid, "env": bridgeEnvOf(env), "ids": ids, "channel": channel}))
	return r.commandResult(), r.err()
}

func (c *scriptedController) Unsubscribe(_ context.Context, sid string, env *common.SessionEnv, ids string, channel string) (*common.CommandResult, error) {
	r := decodeScriptedReturn(c.s.answer("Unsubscribe", map[string]interface{}{"sid": sid, "env": bridgeEnvOf(env), "ids": ids, "channel": channel}))
	return r.commandResult(), r.err()
}

func (c *scriptedController) Perform(_ context.Context, sid string, env *common.SessionEnv, ids string, channel string, data string) (*common.CommandResult, error) {
	r := decodeScriptedReturn(c.s.answer("Perform", map[string]interface{}{"sid": sid, "env": bridgeEnvOf(env), "ids": ids, "channel": channel, "data": data}))
	return r.commandResult(), r.err()
}

func (c *scriptedController) Disconnect(_ context.Context, sid string, env *common.SessionEnv, ids string, subscriptions []string) error {
	return decodeScriptedReturn(c.s.answer("Disconnect", map[string]interface{}{"sid": sid, "env": bridgeEnvOf(env), "ids": ids, "subscriptions": subscriptions})).err()
}

// scriptedIdentifier is a double of identity.Identifier (upstream tests: mocks.Identifier).
type scriptedIdentifier struct{ s *scripted }

func (i *scriptedIdentifier) Identify(sid string, env *common.SessionEnv) (*common.ConnectResult, error) {
	r := decodeScriptedReturn(i.s.answer("Identify", map[string]interface{}{"sid": sid, "env": bridgeEnvOf(env)}))
	return r.connectResult(), r.err()
}

type controllerCallArgs struct {
	Sid           string      `json:"sid"`
	Env           *sessionEnv `json:"env"`
	Ids           string      `json:"ids"`
	Channel       string      `json:"channel"`
	Data          string      `json:"data"`
	Subscriptions []string    `json:"subscriptions"`
}

// callController invokes one node.Controller method named by the request.
func callController(c interface {
	Start() error
	Shutdown() error
	Authenticate(context.Context, string, *common.SessionEnv) (*common.ConnectResult, error)
	Subscribe(context.Context, string, *common.SessionEnv, string, string) (*common.CommandResult, error)
	Unsubscribe(context.Context, string, *common.SessionEnv, string, string) (*common.CommandResult, error)
	Perform(context.Context, string, *common.SessionEnv, string, string, string) (*common.CommandResult, error)
	Disconnect(context.Context, string, *common.SessionEnv, string, []string) error
}, method string, a controllerCallArgs) (pairResult, error) {
	ctx := context.Background()
	env := a.Env.commonOrNil()
	switch method {
	case "Start":
		return pairResult{nil, errString(c.Start())}, nil
	case "Shutdown":
		return pairResult{nil, errString(c.Shutdown())}, nil
	case "Authenticate":
		res, err := c.Authenticate(ctx, a.Sid, env)
		return pairResult{nilPtr(res), errString(err)}, nil
	case "Subscribe":
		res, err := c.Subscribe(ctx, a.Sid, env, a.Ids, a.Channel)
		return pairResult{nilPtr(res), errString(err)}, nil
	case "Unsubscribe":
		res, err := c.Unsubscribe(ctx, a.Sid, env, a.Ids, a.Channel)
		return pairResult{nilPtr(res), errString(err)}, nil
	case "Perform":
		res, err := c.Perform(ctx, a.Sid, env, a.Ids, a.Channel, a.Data)
		return pairResult{nilPtr(res), errString(err)}, nil
	case "Disconnect":
		return pairResult{nil, errString(c.Disconnect(ctx, a.Sid, env, a.Ids, a.Subscriptions))}, nil
	}
	return pairResult{}, fmt.Errorf("unknown controller method %q", method)
}

// nilPtr keeps typed nil pointers JSON-encoded as null.
func nilPtr[T any](p *T) interface{} {
	if p == nil {
		return nil
	}
	return p
}

type jwtConfigArg struct {
	Secret string
	Param  string
	Algo   string
	Force  bool
}

func (a jwtConfigArg) config() identity.JWTConfig {
	return identity.JWTConfig{Secret: a.Secret, Param: a.Param, Algo: jwt.GetSigningMethod(a.Algo), Force: a.Force}
}

func init() {
	register("identity.IdentifiableController", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Controller []scriptedExpectation `json:"controller"`
			Identifier []scriptedExpectation `json:"identifier"`
			Call       struct {
				Method string             `json:"method"`
				Args   controllerCallArgs `json:"args"`
			} `json:"call"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		ctrl := &scriptedController{newScripted(a.Controller)}
		ident := &scriptedIdentifier{newScripted(a.Identifier)}
		subject := identity.NewIdentifiableController(ctrl, ident)
		out, err := callController(subject, a.Call.Method, a.Call.Args)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			"result": out.Result, "error": out.Error,
			"controller_calls": ctrl.s.calls, "identifier_calls": ident.s.calls,
		}, nil
	})
	register("identity.IdentifierPipeline.MockThenPublic", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Identifier []scriptedExpectation `json:"identifier"`
			Sid        string                `json:"sid"`
			Env        *sessionEnv           `json:"env"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		mocked := &scriptedIdentifier{newScripted(a.Identifier)}
		pipe := identity.NewIdentifierPipeline(mocked, identity.NewPublicIdentifier())
		res, err := pipe.Identify(a.Sid, a.Env.commonOrNil())
		return map[string]interface{}{"result": nilPtr(res), "error": errString(err), "identifier_calls": mocked.s.calls}, nil
	})
	register("identity.NewJWTConfig", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Secret string }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return identity.NewJWTConfig(a.Secret), nil
	})
	register("identity.JWTConfig.ToToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Config jwtConfigArg }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return a.Config.config().ToToml(), nil
	})
	register("identity.JWTConfig.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config jwtConfigArg
			Toml   string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config.config()
		_, err := toml.Decode(a.Toml, &conf)
		return map[string]interface{}{"config": conf, "error": errString(err)}, nil
	})
	register("identity.JWTIdentifier.Identify", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config jwtConfigArg
			Sid    string
			Env    *sessionEnv
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := a.Config.config()
		res, err := identity.NewJWTIdentifier(&conf, slog.Default()).Identify(a.Sid, a.Env.commonOrNil())
		return pairResult{nilPtr(res), errString(err)}, nil
	})
}
