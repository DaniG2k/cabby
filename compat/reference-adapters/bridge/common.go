package main

import (
	"bytes"
	"encoding/json"
	"log/slog"

	"github.com/anycable/anycable-go/common"
)

func envFromCommon(e *common.SessionEnv) sessionEnv {
	return sessionEnv{URL: e.URL, Headers: e.Headers, Identifiers: e.Identifiers,
		ConnectionState: e.ConnectionState, ChannelStates: e.ChannelStates}
}

// bridgeArgs unmarshals op arguments into a fresh value of type T.
func bridgeArgs[T any](raw json.RawMessage) (T, error) {
	var a T
	err := json.Unmarshal(raw, &a)
	return a, err
}

func init() {
	register("common.NewSessionEnv", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			URL     string
			Headers *map[string]string
		}](raw)
		if err != nil {
			return nil, err
		}
		return envFromCommon(common.NewSessionEnv(a.URL, a.Headers)), nil
	})
	register("common.SessionEnv.MergeConnectionState", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Env   sessionEnv
			Other map[string]string
		}](raw)
		if err != nil {
			return nil, err
		}
		env := a.Env.common()
		env.MergeConnectionState(&a.Other)
		return envFromCommon(env), nil
	})
	register("common.SessionEnv.MergeChannelState", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Env   sessionEnv
			ID    string
			Other map[string]string
		}](raw)
		if err != nil {
			return nil, err
		}
		env := a.Env.common()
		env.MergeChannelState(a.ID, &a.Other)
		return envFromCommon(env), nil
	})
	register("common.SessionEnv.GetChannelStateField", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Env       sessionEnv
			ID, Field string
		}](raw)
		if err != nil {
			return nil, err
		}
		return a.Env.common().GetChannelStateField(a.ID, a.Field), nil
	})
	register("common.SessionEnv.GetConnectionStateField", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Env   sessionEnv
			Field string
		}](raw)
		if err != nil {
			return nil, err
		}
		return a.Env.common().GetConnectionStateField(a.Field), nil
	})
	// Result: {"Kind": "stream"|"batch"|"remote_command", "Value": ..., "Error": ...}.
	register("common.PubSubMessageFromJSON", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Raw string }](raw)
		if err != nil {
			return nil, err
		}
		res, perr := common.PubSubMessageFromJSON([]byte(a.Raw))
		out := map[string]interface{}{"Error": errString(perr)}
		switch v := res.(type) {
		case common.StreamMessage:
			out["Kind"], out["Value"] = "stream", v
		case []*common.StreamMessage:
			out["Kind"], out["Value"] = "batch", v
		case common.RemoteCommandMessage:
			// Payload is raw JSON; return it verbatim as a string.
			out["Kind"] = "remote_command"
			out["Value"] = map[string]string{"Command": v.Command, "Payload": string(v.Payload)}
		}
		return out, nil
	})
	register("common.RemoteCommandMessage.ToRemoteDisconnectMessage", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Command, Payload string }](raw)
		if err != nil {
			return nil, err
		}
		msg := common.RemoteCommandMessage{Command: a.Command, Payload: json.RawMessage(a.Payload)}
		res, derr := msg.ToRemoteDisconnectMessage()
		return parsed{res, errString(derr)}, nil
	})
	register("common.ConfirmationMessage", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Identifier string }](raw)
		if err != nil {
			return nil, err
		}
		return common.ConfirmationMessage(a.Identifier), nil
	})
	register("common.RejectionMessage", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Identifier string }](raw)
		if err != nil {
			return nil, err
		}
		return common.RejectionMessage(a.Identifier), nil
	})
	// Returns the Reply as encoding/json renders it (Message keeps json.Number precision).
	register("common.StreamMessage.ToReplyFor", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Message    common.StreamMessage
			Identifier string
		}](raw)
		if err != nil {
			return nil, err
		}
		return a.Message.ToReplyFor(a.Identifier), nil
	})
	register("common.Message.Unmarshal", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Raw string }](raw)
		if err != nil {
			return nil, err
		}
		var msg common.Message
		uerr := json.Unmarshal([]byte(a.Raw), &msg)
		return parsed{msg, errString(uerr)}, nil
	})
	register("common.FilteredIdentifier", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Input string }](raw)
		if err != nil {
			return nil, err
		}
		return common.FilteredIdentifier(a.Input).LogValue().String(), nil
	})
	// Logs the result with slog's text handler, as the upstream test does.
	register("common.CommandResult.LogText", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Msg    string
			Key    string
			Result common.CommandResult
		}](raw)
		if err != nil {
			return nil, err
		}
		var buf bytes.Buffer
		slog.New(slog.NewTextHandler(&buf, nil)).Info(a.Msg, a.Key, &a.Result)
		return buf.String(), nil
	})
	// The "transmissions" attribute of CommandResult.LogValue(), i.e. the output of the
	// unexported filteredTransmissions, rendered per element with LogValue().String().
	register("common.CommandResult.LogValue.Transmissions", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Transmissions []string }](raw)
		if err != nil {
			return nil, err
		}
		res := &common.CommandResult{Transmissions: a.Transmissions}
		for _, attr := range res.LogValue().Group() {
			if attr.Key != "transmissions" {
				continue
			}
			vals := attr.Value.Any().([]slog.LogValuer)
			out := make([]string, len(vals))
			for i, v := range vals {
				out[i] = v.LogValue().String()
			}
			return out, nil
		}
		return nil, nil
	})
}
