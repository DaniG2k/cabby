package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"

	"github.com/anycable/anycable-go/broadcast"
	"github.com/anycable/anycable-go/broker"
	anycli "github.com/anycable/anycable-go/cli"
	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/config"
	"github.com/anycable/anycable-go/mocks"
	"github.com/anycable/anycable-go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// cliRecorderT stands in for *testing.T: assertion failures (assert.*, mock
// expectation failures) are recorded instead of failing a Go test, and Cleanup
// functions (mockery's AssertExpectations) run when the scenario ends.
type cliRecorderT struct {
	mu       sync.Mutex
	errors   []string
	cleanups []func()
}

type cliRecorderFailNow struct{}

func (r *cliRecorderT) Logf(string, ...interface{}) {}
func (r *cliRecorderT) Helper()                     {}
func (r *cliRecorderT) Errorf(format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}
func (r *cliRecorderT) FailNow()         { panic(cliRecorderFailNow{}) }
func (r *cliRecorderT) Cleanup(f func()) { r.cleanups = append(r.cleanups, f) }

func init() {
	// The upstream TestRunnerDrainsBroadcastsBeforeBrokerShutdown body, with its mockery
	// doubles, driven through the exported API: NewRunner + Runner.Embed() (which calls
	// runNode) + Embedded.Shutdown (which shuts down runner.shutdownables in order).
	// Embedded.Shutdown stops at the first error, where upstream asserts NoError on each
	// shutdownable: a nil ShutdownError therefore means every Shutdown returned nil.
	// Runs in an isolated bridge process (presets read the environment).
	register("cli.Runner.DrainsBroadcastsBeforeBrokerShutdown", func(json.RawMessage) (res interface{}, err error) {
		t := &cliRecorderT{}
		defer func() {
			if p := recover(); p != nil {
				if _, ok := p.(cliRecorderFailNow); !ok {
					res, err = nil, fmt.Errorf("panic: %v", p)
				}
			}
		}()
		devnull, derr := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if derr != nil {
			return nil, derr
		}
		stdout := os.Stdout
		os.Stdout = devnull
		defer func() {
			os.Stdout = stdout
			devnull.Close()
		}()

		c := config.NewConfig()
		c.RPC.Implementation = "none"
		c.DisconnectorDisabled = true

		brokerClosed := false
		subscriberClosed := false
		serviceClosed := false
		delivered := []string{}
		service := mocks.NewBroadcaster(t)
		service.On("Shutdown", mock.Anything).Run(func(mock.Arguments) {
			serviceClosed = true
		}).Return(nil)
		subscriber := mocks.NewSubscriber(t)
		subscriber.On("Start", mock.Anything).Return(nil)
		subscriber.On("Broadcast", mock.Anything).Run(func(args mock.Arguments) {
			assert.False(t, subscriberClosed, "pub/sub must remain available while broadcasts drain")
			delivered = append(delivered, args.Get(0).(*common.StreamMessage).Data)
		})
		subscriber.On("BroadcastCommand", mock.Anything).Run(func(mock.Arguments) {
			assert.False(t, subscriberClosed, "pub/sub must remain available while the broker shuts down")
		})
		subscriber.On("Shutdown", mock.Anything).Run(func(mock.Arguments) {
			assert.False(t, serviceClosed, "shared services must outlive pub/sub")
			subscriberClosed = true
		}).Return(nil)

		appBroker := mocks.NewBroker(t)
		appBroker.On("Announce").Return("test broker")
		appBroker.On("Start", mock.Anything).Return(nil)
		appBroker.On("HandleBroadcast", mock.Anything).Return(func(msg *common.StreamMessage) error {
			assert.False(t, serviceClosed, "shared services must remain available while broadcasts drain")
			if brokerClosed {
				return errors.New("broker is closed")
			}
			subscriber.Broadcast(msg)
			return nil
		})
		appBroker.On("Shutdown", mock.Anything).Run(func(mock.Arguments) {
			assert.False(t, serviceClosed, "shared services must outlive the broker")
			brokerClosed = true
			subscriber.BroadcastCommand(&common.RemoteCommandMessage{})
		}).Return(nil)

		broadcasters := []*mocks.Broadcaster{}
		runner, rerr := anycli.NewRunner(&c, []anycli.Option{
			anycli.WithName("shutdown-test"),
			anycli.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
			anycli.WithDefaultRPCController(),
			anycli.WithShutdownable(service),
			anycli.WithSubscriber(func(pubsub.Handler, *config.Config, *slog.Logger) (pubsub.Subscriber, error) {
				return subscriber, nil
			}),
			anycli.WithBroker(func(broker.Broadcaster, broker.Presenter, *config.Config, *slog.Logger) (broker.Broker, error) {
				return appBroker, nil
			}),
			anycli.WithBroadcasters(func(handler broadcast.Handler, _ *config.Config, _ *slog.Logger) ([]broadcast.Broadcaster, error) {
				out := []broadcast.Broadcaster{}
				for index := range 2 {
					broadcaster := mocks.NewBroadcaster(t)
					broadcaster.On("Start", mock.Anything).Return(nil)
					// A reader may finish an in-flight publication while Shutdown waits for it.
					payload := []byte(fmt.Sprintf(`{"stream":"shutdown-test","data":"%d"}`, index))
					broadcaster.On("Shutdown", mock.Anything).Return(func(context.Context) error {
						return handler.HandleBroadcast(payload)
					})
					broadcasters = append(broadcasters, broadcaster)
					out = append(out, broadcaster)
				}
				return out, nil
			}),
		})
		out := map[string]interface{}{"NewRunnerError": errString(rerr)}
		var shutdownErr error
		if rerr == nil {
			embedded, eerr := runner.Embed()
			out["RunNodeError"] = errString(eerr)
			if eerr == nil {
				shutdownErr = embedded.Shutdown(context.Background())
			}
		}
		out["ShutdownError"] = errString(shutdownErr)

		for _, f := range t.cleanups {
			f()
		}

		calls := []string{}
		record := func(name string, m *mock.Mock) {
			for _, call := range m.Calls {
				calls = append(calls, name+"."+call.Method)
			}
		}
		record("service", &service.Mock)
		record("subscriber", &subscriber.Mock)
		record("broker", &appBroker.Mock)
		for i, b := range broadcasters {
			record(fmt.Sprintf("broadcaster%d", i), &b.Mock)
		}
		sort.Strings(calls)

		t.mu.Lock()
		defer t.mu.Unlock()
		out["Delivered"] = delivered
		out["BrokerClosed"] = brokerClosed
		out["SubscriberClosed"] = subscriberClosed
		out["ServiceClosed"] = serviceClosed
		out["Violations"] = append([]string{}, t.errors...)
		out["Calls"] = calls
		return out, nil
	})
}
