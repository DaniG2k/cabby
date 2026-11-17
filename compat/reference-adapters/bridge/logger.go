package main

import (
	"bytes"
	"encoding/json"
	"log/slog"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/logger"
)

// Filtering-related ops read the package state initialized from ANYCABLE_FILTER_LOGS
// at process start; tests run them in a bridge process with its own environment.
func init() {
	register("logger.CompactValue", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Value string
			Bytes bool
		}](raw)
		if err != nil {
			return nil, err
		}
		if a.Bytes {
			return logger.CompactValue([]byte(a.Value)).LogValue().String(), nil
		}
		return logger.CompactValue(a.Value).LogValue().String(), nil
	})
	register("logger.CompactValues", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Values []string
			Bytes  bool
		}](raw)
		if err != nil {
			return nil, err
		}
		out := []string{}
		if a.Bytes {
			vals := make([][]byte, len(a.Values))
			for i, v := range a.Values {
				vals[i] = []byte(v)
			}
			for _, c := range logger.CompactValues(vals) {
				out = append(out, c.LogValue().String())
			}
		} else {
			for _, c := range logger.CompactValues(a.Values) {
				out = append(out, c.LogValue().String())
			}
		}
		return out, nil
	})
	// slog.Any(key, CompactValues(values)).String()
	register("logger.CompactValues.AttrString", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Key    string
			Values []string
			Bytes  bool
		}](raw)
		if err != nil {
			return nil, err
		}
		if a.Bytes {
			vals := make([][]byte, len(a.Values))
			for i, v := range a.Values {
				vals[i] = []byte(v)
			}
			return slog.Any(a.Key, logger.CompactValues(vals)).String(), nil
		}
		return slog.Any(a.Key, logger.CompactValues(a.Values)).String(), nil
	})
	// CompactAny of the upstream test's fixture value struct{ val string }.
	register("logger.CompactAny.StructVal", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Val string }](raw)
		if err != nil {
			return nil, err
		}
		value := struct{ val string }{a.Val}
		return logger.CompactAny(value).LogValue().String(), nil
	})
	// The upstream TestWithJSONHandler log call:
	// Debug(msg, "b", CompactValue([]byte(short)), "l", CompactValues([]string{short, long}))
	// on a JSON handler at debug level.
	register("logger.JSONHandler.CompactValues", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Msg, Short, Long string }](raw)
		if err != nil {
			return nil, err
		}
		buf := bytes.Buffer{}
		l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		l.Debug(a.Msg, "b", logger.CompactValue([]byte(a.Short)), "l", logger.CompactValues([]string{a.Short, a.Long}))
		return buf.String(), nil
	})
	register("logger.FilteredURL", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Val string }](raw)
		if err != nil {
			return nil, err
		}
		return logger.FilteredURL(a.Val).LogValue().String(), nil
	})
	register("logger.MaskValue", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Val string }](raw)
		if err != nil {
			return nil, err
		}
		return logger.MaskValue(a.Val), nil
	})
	register("logger.FilteredMap", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Map map[string]string }](raw) // null: nil map
		if err != nil {
			return nil, err
		}
		return logger.FilteredMap(a.Map).LogValue().String(), nil
	})
	register("logger.FilteringEnabled", func(json.RawMessage) (interface{}, error) {
		return logger.FilteringEnabled(), nil
	})

	register("logger.NewConfig", func(json.RawMessage) (interface{}, error) {
		return logger.NewConfig(), nil
	})
	register("logger.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Config logger.Config }](raw)
		if err != nil {
			return nil, err
		}
		return a.Config.ToToml(), nil
	})
	register("logger.Config.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Config logger.Config
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
