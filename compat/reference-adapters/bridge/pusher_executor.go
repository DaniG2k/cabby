package main

// pusher/executor_test.go scaffolding: MockExecutor, NewMockNode and buildSession
// are copied from the upstream test file (test doubles, not production behavior);
// the subject is the exported pusher.Executor.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/metrics"
	"github.com/anycable/anycable-go/mocks"
	"github.com/anycable/anycable-go/node"
	"github.com/anycable/anycable-go/pusher"
	"github.com/stretchr/testify/mock"
)

// executorMockExecutor is the upstream test's MockExecutor.
type executorMockExecutor struct {
	ctrl *pusher.Controller
}

func (e *executorMockExecutor) HandleCommand(s *node.Session, msg *common.Message) (err error) {
	var res *common.CommandResult

	if msg.Command == "subscribe" {
		res, err = e.ctrl.Subscribe(context.Background(), s.GetID(), s.GetEnv(), s.GetIdentifiers(), msg.Identifier)
	}

	if res != nil {
		for _, msg := range res.Transmissions {
			s.SendJSONTransmission(msg)
		}
	}

	if res == nil {
		return errors.New("not implemeented")
	}

	return
}

func (e *executorMockExecutor) Disconnect(s *node.Session) error {
	return nil
}

// executorMockNode is the upstream test's NewMockNode.
func executorMockNode() *node.Node {
	controller := mocks.NewMockController()
	config := node.NewConfig()
	config.HubGopoolSize = 2
	return node.NewNode(&config, node.WithController(&controller), node.WithInstrumenter(metrics.NewMetrics(nil, 10, quietLog)))
}

// executorBuildSession is the upstream test's buildSession.
func executorBuildSession(conn node.Connection, n *node.Node, executor node.Executor) *node.Session {
	s := node.NewSession(n, conn, "ws://anycable.io/pusher/app-id", nil, "1234.1234", node.WithEncoder(&pusher.Encoder{}), node.WithExecutor(executor))
	n.Authenticated(s, "")
	s.Log = slog.New(quietLog.Handler()).With("context", "test")
	return s
}

func init() {
	// TestHandleCommand setup plus one subtest: build the executor (config with
	// AppKey/Secret, mocks.Broker returning Presence), a session over
	// mocks.NewMockConnection(), run HandleCommand and then conn.Read() Reads times.
	register("pusher.Executor.HandleCommand", func(raw json.RawMessage) (out interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				out, err = nil, fmt.Errorf("panic: %v", r)
			}
		}()
		var a struct {
			AppID, Key       string
			Presence         *common.PresenceInfo
			Command          string
			Identifier       string
			SubscriptionData *pusher.PusherSubscriptionData
			Reads            int
		}
		if err := decodeJSON(raw, &a); err != nil {
			return nil, err
		}
		conf := pusher.NewConfig()
		conf.AppKey = a.AppID
		conf.Secret = a.Key
		bro := mocks.Broker{}
		if a.Presence != nil {
			bro.On("PresenceInfo", mock.Anything).Return(a.Presence, nil)
		}
		app := &executorMockExecutor{ctrl: pusher.NewController(&bro, &conf, quietLog)}
		n := executorMockNode()
		verifier := pusher.NewVerifier(a.AppID, a.Key)
		executor := pusher.NewExecutor(app, verifier)

		conn := mocks.NewMockConnection()
		session := executorBuildSession(conn, n, executor)
		defer session.Disconnect("test", 0)

		msg := common.Message{Command: a.Command, Identifier: a.Identifier}
		if a.SubscriptionData != nil {
			msg.Data = a.SubscriptionData
		}

		hErr := executor.HandleCommand(session, &msg)

		reads := []map[string]interface{}{}
		for i := 0; i < a.Reads; i++ {
			res, rErr := conn.Read()
			r := map[string]interface{}{"error": errString(rErr)}
			if res != nil {
				r["msg"] = string(res)
			}
			reads = append(reads, r)
		}
		return map[string]interface{}{"error": errString(hErr), "reads": reads}, nil
	})
	register("pusher.NewEncoder", func(json.RawMessage) (interface{}, error) {
		return map[string]interface{}{"ActivityTimeout": pusher.NewEncoder().ActivityTimeout}, nil
	})
	register("pusher.NewConfig", func(json.RawMessage) (interface{}, error) {
		return pusher.NewConfig(), nil
	})
}
