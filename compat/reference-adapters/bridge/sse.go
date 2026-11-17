package main

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/encoders"
	"github.com/anycable/anycable-go/sse"
)

type sseEncoderArgs struct {
	Encoder sse.Encoder
	Msg     encodedMessage
	Raw     string
}

// connectionStep is one method call on an sse.Connection.
type connectionStep struct {
	Op        string // write | write_binary | read | descriptor | established | close | context_done | body | status
	Msg       []byte
	Code      int
	Reason    string
	TimeoutMs int
}

type connectionResult struct {
	Error *string `json:"error"`
	// Bytes: Read result or recorded body; nil stays null.
	Bytes *string `json:"bytes"`
	Bool  bool    `json:"bool"`
	Int   int     `json:"int"`
}

func init() {
	register("sse.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		var c sse.Config
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		return c.ToToml(), nil
	})
	register("sse.DecodeConfigToml", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Toml string }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		conf := sse.Config{}
		_, err := toml.Decode(a.Toml, &conf)
		return parsed{conf, errString(err)}, nil
	})
	register("sse.Encoder.ID", func(raw json.RawMessage) (interface{}, error) {
		var a sseEncoderArgs
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return a.Encoder.ID(), nil
	})
	register("sse.Encoder.Encode", func(raw json.RawMessage) (interface{}, error) {
		var a sseEncoderArgs
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		msg, err := a.Msg.message()
		if err != nil {
			return nil, err
		}
		return toFrameResult(a.Encoder.Encode(msg)), nil
	})
	register("sse.Encoder.EncodeTransmission", func(raw json.RawMessage) (interface{}, error) {
		var a sseEncoderArgs
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return toFrameResult(a.Encoder.EncodeTransmission(a.Raw)), nil
	})
	register("sse.Encoder.Decode", func(raw json.RawMessage) (interface{}, error) {
		var a sseEncoderArgs
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		return toDecodedMessage(a.Encoder.Decode([]byte(a.Raw))), nil
	})
	// encoders.NewCachedEncodedMessage(msg), then Fetch(enc.ID(), enc.Encode) for each
	// encoder in order.
	register("sse.CachedEncodedMessage.Fetch", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Msg      encodedMessage
			Encoders []sse.Encoder
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		msg, err := a.Msg.message()
		if err != nil {
			return nil, err
		}
		cached := encoders.NewCachedEncodedMessage(msg)
		results := []frameResult{}
		for i := range a.Encoders {
			enc := &a.Encoders[i]
			results = append(results, toFrameResult(cached.Fetch(enc.ID(), enc.Encode)))
		}
		return results, nil
	})
	// Runs method calls on sse.NewConnection(httptest.NewRecorder()) in order and
	// returns each call's result.
	register("sse.Connection", func(raw json.RawMessage) (interface{}, error) {
		var a struct{ Steps []connectionStep }
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		w := httptest.NewRecorder()
		c := sse.NewConnection(w)
		deadline := time.Now().Add(1 * time.Second)
		results := []connectionResult{}
		for _, s := range a.Steps {
			var r connectionResult
			switch s.Op {
			case "write":
				r.Error = errString(c.Write(s.Msg, deadline))
			case "write_binary":
				r.Error = errString(c.WriteBinary(s.Msg, deadline))
			case "read":
				msg, err := c.Read()
				r.Error = errString(err)
				if msg != nil {
					str := string(msg)
					r.Bytes = &str
				}
			case "descriptor":
				r.Bool = c.Descriptor() == nil
			case "established":
				c.Established()
			case "close":
				c.Close(s.Code, s.Reason)
			case "context_done":
				select {
				case <-c.Context().Done():
					r.Bool = true
				case <-time.After(time.Duration(s.TimeoutMs) * time.Millisecond):
				}
			case "body":
				if b := w.Body.Bytes(); b != nil {
					str := string(b)
					r.Bytes = &str
				}
			case "status":
				r.Int = w.Code
			default:
				return nil, fmt.Errorf("unknown connection step %q", s.Op)
			}
			results = append(results, r)
		}
		return results, nil
	})
}
