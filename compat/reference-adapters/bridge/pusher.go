package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/broker"
	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/encoders"
	"github.com/anycable/anycable-go/mocks"
	"github.com/anycable/anycable-go/pusher"
	"github.com/anycable/anycable-go/ws"
	"github.com/stretchr/testify/mock"
)

// quietLog discards upstream log output so it does not interleave with test output.
var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// encodedMessage selects one encoders.EncodedMessage implementation; payloads use the
// upstream JSON tags. Numbers decode as json.Number so they re-encode verbatim.
type encodedMessage struct {
	Kind string          `json:"kind"` // reply | ping | disconnect
	Msg  json.RawMessage `json:"msg"`
}

func decodeJSON(raw []byte, v interface{}) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(v)
}

func (m encodedMessage) message() (encoders.EncodedMessage, error) {
	switch m.Kind {
	case "reply":
		var r common.Reply
		return &r, decodeJSON(m.Msg, &r)
	case "ping":
		var p common.PingMessage
		return &p, decodeJSON(m.Msg, &p)
	case "disconnect":
		var d common.DisconnectMessage
		return &d, decodeJSON(m.Msg, &d)
	}
	return nil, fmt.Errorf("unknown message kind %q", m.Kind)
}

// frameResult carries an Encode result: a nil frame, or its type and payload.
type frameResult struct {
	Frame *struct {
		FrameType int    `json:"frame_type"`
		Payload   string `json:"payload"`
	} `json:"frame"`
	Error *string `json:"error"`
}

func toFrameResult(f *ws.SentFrame, err error) frameResult {
	r := frameResult{Error: errString(err)}
	if f != nil {
		r.Frame = &struct {
			FrameType int    `json:"frame_type"`
			Payload   string `json:"payload"`
		}{int(f.FrameType), string(f.Payload)}
	}
	return r
}

// decodedMessage is a *common.Message with the dynamic type of Data spelled out.
type decodedMessage struct {
	Message  *common.Message `json:"message"`
	DataType string          `json:"data_type"`
	Error    *string         `json:"error"`
}

func toDecodedMessage(m *common.Message, err error) decodedMessage {
	r := decodedMessage{Message: m, Error: errString(err)}
	if m != nil && m.Data != nil {
		r.DataType = fmt.Sprintf("%T", m.Data)
	}
	return r
}

type pusherEncoderArgs struct {
	ActivityTimeout int
	Msg             encodedMessage
	Raw             string
}

type presenceAdd struct {
	Stream, Sid, Pid string
	Info             interface{}
}

type apiResponse struct {
	Status     int                 `json:"status"`
	Headers    map[string][]string `json:"headers"`
	Body       string              `json:"body"`
	BodyRead   int                 `json:"body_read"`
	Broadcasts []string            `json:"broadcasts"`
}

// countingReader reports how many request body bytes the handler consumed.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func init() {
	register("pusher.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		var c pusher.Config
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		return c.ToToml(), nil
	})
	register("pusher.DecodeConfigToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Toml string }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := pusher.Config{}
		_, err := toml.Decode(a.Toml, &conf)
		return parsed{conf, errString(err)}, nil
	})
	register("pusher.Verifier", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			AppID, Key, Method                      string
			Socket, Channel, Info, Signature, Input string
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		v := pusher.NewVerifier(a.AppID, a.Key)
		switch a.Method {
		case "VerifyChannel":
			return v.VerifyChannel(a.Socket, a.Channel, a.Signature), nil
		case "VerifyPresenceChannel":
			return v.VerifyPresenceChannel(a.Socket, a.Channel, a.Info, a.Signature), nil
		case "VerifyUser":
			return v.VerifyUser(a.Socket, a.Info, a.Signature), nil
		case "Verify":
			return v.Verify(a.Input, a.Signature), nil
		case "Sign":
			return v.Sign(a.Input), nil
		}
		return nil, fmt.Errorf("unknown verifier method %q", a.Method)
	})
	register("pusher.Encoder.Encode", func(raw json.RawMessage) (interface{}, error) {
		var a pusherEncoderArgs
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		msg, err := a.Msg.message()
		if err != nil {
			return nil, err
		}
		enc := &pusher.Encoder{ActivityTimeout: a.ActivityTimeout}
		return toFrameResult(enc.Encode(msg)), nil
	})
	register("pusher.Encoder.EncodeTransmission", func(raw json.RawMessage) (interface{}, error) {
		var a pusherEncoderArgs
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		enc := &pusher.Encoder{ActivityTimeout: a.ActivityTimeout}
		return toFrameResult(enc.EncodeTransmission(a.Raw)), nil
	})
	register("pusher.Encoder.Decode", func(raw json.RawMessage) (interface{}, error) {
		var a pusherEncoderArgs
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		enc := &pusher.Encoder{ActivityTimeout: a.ActivityTimeout}
		return toDecodedMessage(enc.Decode([]byte(a.Raw))), nil
	})
	// Controller commands with a mocks.Broker whose PresenceInfo returns the given
	// value (nil: no expectation, so any broker call fails the mock).
	controllerOp := func(command string) op {
		return func(raw json.RawMessage) (interface{}, error) {
			var a struct {
				Config               pusher.Config
				Presence             *common.PresenceInfo
				Env                  sessionEnv
				Sid, Ids, Identifier string
			}
			if err := decodeJSON(raw, &a); err != nil {
				return nil, err
			}
			bro := &mocks.Broker{}
			if a.Presence != nil {
				bro.On("PresenceInfo", mock.Anything).Return(a.Presence, nil)
			}
			ctrl := pusher.NewController(bro, &a.Config, quietLog)
			var res *common.CommandResult
			var err error
			if command == "Subscribe" {
				res, err = ctrl.Subscribe(context.Background(), a.Sid, a.Env.common(), a.Ids, a.Identifier)
			} else {
				res, err = ctrl.Unsubscribe(context.Background(), a.Sid, a.Env.common(), a.Ids, a.Identifier)
			}
			return parsed{res, errString(err)}, nil
		}
	}
	register("pusher.Controller.Subscribe", controllerOp("Subscribe"))
	register("pusher.Controller.Unsubscribe", controllerOp("Unsubscribe"))
	// RestAPI.Handler serving one request (as http.NewRequest builds it) with a
	// mocks.Handler accepting any broadcast, and either an expectation-free
	// mocks.Broker or a started memory broker with the given presence records.
	register("pusher.RestAPI.Handler", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Config         pusher.Config
			Broker         string // mock | memory
			Presence       []presenceAdd
			Method, Target string
			Body           string
		}
		if err := decodeJSON(raw, &a); err != nil {
			return nil, err
		}
		handler := &mocks.Handler{}
		handler.On("HandleBroadcast", mock.Anything).Return(nil)
		var b broker.Broker
		switch a.Broker {
		case "mock":
			b = &mocks.Broker{}
		case "memory":
			bconf := broker.NewConfig()
			mem := broker.NewMemoryBroker(nil, nil, &bconf)
			if err := mem.Start(nil); err != nil {
				return nil, err
			}
			defer mem.Shutdown(context.Background()) // nolint:errcheck
			for _, p := range a.Presence {
				if _, err := mem.PresenceAdd(p.Stream, p.Sid, p.Pid, p.Info); err != nil {
					return nil, fmt.Errorf("PresenceAdd: %w", err)
				}
			}
			b = mem
		default:
			return nil, fmt.Errorf("unknown broker %q", a.Broker)
		}
		api := pusher.NewRestAPI(handler, b, &a.Config, quietLog)
		body := &countingReader{r: strings.NewReader(a.Body)}
		req, err := http.NewRequest(a.Method, a.Target, body)
		if err != nil {
			return nil, err
		}
		rr := httptest.NewRecorder()
		http.HandlerFunc(api.Handler).ServeHTTP(rr, req)
		res := apiResponse{Status: rr.Code, Headers: rr.Header(), Body: rr.Body.String(), BodyRead: body.n, Broadcasts: []string{}}
		for _, c := range handler.Calls {
			if c.Method == "HandleBroadcast" {
				res.Broadcasts = append(res.Broadcasts, string(c.Arguments.Get(0).([]byte)))
			}
		}
		return res, nil
	})
}
