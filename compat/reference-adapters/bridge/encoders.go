package main

import (
	"encoding/json"

	"github.com/anycable/anycable-go/common"
	"github.com/anycable/anycable-go/encoders"
	"github.com/anycable/anycable-go/ws"
)

// sentFrame renders ws.SentFrame with the payload as text (instead of base64).
type sentFrame struct {
	FrameType   int
	Payload     string
	CloseCode   int
	CloseReason string
}

func frameOf(f *ws.SentFrame) *sentFrame {
	if f == nil {
		return nil
	}
	return &sentFrame{int(f.FrameType), string(f.Payload), f.CloseCode, f.CloseReason}
}

// encodersMockMessage is the upstream test's MockMessage fixture
// (encoders/encoding_cache_test.go).
type encodersMockMessage struct {
	Encoded int
	Value   string
}

func (m *encodersMockMessage) GetType() string { return "mock" }

func init() {
	register("encoders.JSON.Encode", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Reply common.Reply }](raw)
		if err != nil {
			return nil, err
		}
		f, eerr := encoders.JSON{}.Encode(&a.Reply)
		return parsed{frameOf(f), errString(eerr)}, nil
	})
	register("encoders.JSON.EncodeTransmission", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Msg string }](raw)
		if err != nil {
			return nil, err
		}
		f, eerr := encoders.JSON{}.EncodeTransmission(a.Msg)
		return parsed{frameOf(f), errString(eerr)}, nil
	})
	register("encoders.JSON.Decode", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Raw string }](raw)
		if err != nil {
			return nil, err
		}
		msg, derr := encoders.JSON{}.Decode([]byte(a.Raw))
		return parsed{msg, errString(derr)}, nil
	})
	// Fetches one mock message from a fresh EncodingCache once per entry of Encoders,
	// with the upstream test's callback (counts encodings, returns the message value as
	// a text frame). Returns the fetched frames and the encoding count.
	register("encoders.EncodingCache.FetchMock", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Value    string
			Encoders []string
		}](raw)
		if err != nil {
			return nil, err
		}
		msg := &encodersMockMessage{Value: a.Value}
		c := encoders.NewEncodingCache()
		callback := func(msg encoders.EncodedMessage) (*ws.SentFrame, error) {
			if m, ok := msg.(*encodersMockMessage); ok {
				m.Encoded++
				return &ws.SentFrame{FrameType: ws.TextFrame, Payload: []byte(m.Value)}, nil
			}
			return nil, nil
		}
		frames := []parsed{}
		for _, id := range a.Encoders {
			f, ferr := c.Fetch(msg, id, callback)
			frames = append(frames, parsed{frameOf(f), errString(ferr)})
		}
		return map[string]interface{}{"Frames": frames, "Encoded": msg.Encoded}, nil
	})
}
