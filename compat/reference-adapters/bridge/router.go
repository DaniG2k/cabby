package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/mocks"
	"github.com/anycable/anycable-go/router"
)

// routerCall describes one controller method call (context.Context is implicit).
type routerCall struct {
	Method        string
	Sid           string
	ID            string
	Channel       string
	Data          string
	Subscriptions []string
}

type routerExpectation struct {
	// Controller: "" for the default controller, otherwise the routed channel name.
	Controller    string
	Call          routerCall
	ConnectResult *common.ConnectResult
	CommandResult *common.CommandResult
	Error         *string
}

// mockArgs builds the testify argument list upstream passes to On/AssertCalled.
func (c routerCall) mockArgs(env *common.SessionEnv) []interface{} {
	ctx := context.Background()
	switch c.Method {
	case "Start", "Shutdown":
		return nil
	case "Authenticate":
		return []interface{}{ctx, c.Sid, env}
	case "Subscribe", "Unsubscribe":
		return []interface{}{ctx, c.Sid, env, c.ID, c.Channel}
	case "Perform":
		return []interface{}{ctx, c.Sid, env, c.ID, c.Channel, c.Data}
	case "Disconnect":
		return []interface{}{ctx, c.Sid, env, c.ID, c.Subscriptions}
	}
	return nil
}

func init() {
	// Builds a RouterController over upstream mocks.Controller instances, registers the
	// expectations (On(...).Return(...)), performs one call and reports its result and
	// every call each mock received (mock.Calls).
	register("router.Scenario", func(raw json.RawMessage) (res interface{}, err error) {
		// testify panics on calls without a matching expectation; report it as a
		// bridge error instead of killing the shared bridge.
		defer func() {
			if r := recover(); r != nil {
				res, err = nil, fmt.Errorf("panic: %v", r)
			}
		}()
		a, err := bridgeArgs[struct {
			Env          sessionEnv
			Routes       []string
			Expectations []routerExpectation
			Call         *routerCall // nil: only build and route
		}](raw)
		if err != nil {
			return nil, err
		}
		env := a.Env.common()
		controllers := map[string]*mocks.Controller{"": {}}
		subject := router.NewRouterController(controllers[""])
		routeErrors := []*string{}
		for _, name := range a.Routes {
			controllers[name] = &mocks.Controller{}
			routeErrors = append(routeErrors, errString(subject.Route(name, controllers[name])))
		}
		for _, e := range a.Expectations {
			m, ok := controllers[e.Controller]
			if !ok {
				return nil, fmt.Errorf("unknown controller %q", e.Controller)
			}
			var errVal error
			if e.Error != nil {
				errVal = errors.New(*e.Error)
			}
			var ret []interface{}
			switch e.Call.Method {
			case "Start", "Shutdown", "Disconnect":
				ret = []interface{}{errVal}
			case "Authenticate":
				if e.ConnectResult != nil {
					ret = []interface{}{e.ConnectResult, errVal}
				} else {
					ret = []interface{}{nil, errVal}
				}
			default:
				if e.CommandResult != nil {
					ret = []interface{}{e.CommandResult, errVal}
				} else {
					ret = []interface{}{nil, errVal}
				}
			}
			m.On(e.Call.Method, e.Call.mockArgs(env)...).Return(ret...)
		}

		ctx := context.Background()
		out := map[string]interface{}{"RouteErrors": routeErrors}
		var callErr error
		c := routerCall{Method: "(none)"}
		if a.Call != nil {
			c = *a.Call
		}
		switch c.Method {
		case "(none)":
		case "Start":
			callErr = subject.Start()
		case "Shutdown":
			callErr = subject.Shutdown()
		case "Authenticate":
			res, e := subject.Authenticate(ctx, c.Sid, env)
			out["ConnectResult"], callErr = res, e
		case "Subscribe":
			res, e := subject.Subscribe(ctx, c.Sid, env, c.ID, c.Channel)
			out["CommandResult"], callErr = res, e
		case "Unsubscribe":
			res, e := subject.Unsubscribe(ctx, c.Sid, env, c.ID, c.Channel)
			out["CommandResult"], callErr = res, e
		case "Perform":
			res, e := subject.Perform(ctx, c.Sid, env, c.ID, c.Channel, c.Data)
			out["CommandResult"], callErr = res, e
		case "Disconnect":
			callErr = subject.Disconnect(ctx, c.Sid, env, c.ID, c.Subscriptions)
		default:
			return nil, fmt.Errorf("unknown method %q", c.Method)
		}
		out["Error"] = errString(callErr)

		calls := map[string][]map[string]interface{}{}
		for name, m := range controllers {
			calls[name] = []map[string]interface{}{}
			for _, call := range m.Calls {
				rec := map[string]interface{}{"Method": call.Method}
				for i, arg := range call.Arguments {
					switch v := arg.(type) {
					case *common.SessionEnv:
						rec["Env"] = envFromCommon(v)
					case context.Context:
					default:
						rec[fmt.Sprint(i)] = v
					}
				}
				calls[name] = append(calls[name], rec)
			}
		}
		out["Calls"] = calls
		return out, nil
	})
}
