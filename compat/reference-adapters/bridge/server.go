package main

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/BurntSushi/toml"
	"github.com/anycable/anycable-go/server"
)

// testRequest describes an httptest.NewRequest (or, with Plain, an http.NewRequest:
// no host or RemoteAddr) plus header and TLS adjustments, as built by the upstream
// server tests.
type testRequest struct {
	Method  string
	Target  string
	Headers [][2]string // applied in order with Header.Set
	TLS     bool
	Plain   bool
}

func (r testRequest) build() *http.Request {
	var req *http.Request
	if r.Plain {
		var err error
		if req, err = http.NewRequest(r.Method, r.Target, nil); err != nil {
			panic(err)
		}
	} else {
		req = httptest.NewRequest(r.Method, r.Target, nil)
	}
	for _, h := range r.Headers {
		req.Header.Set(h[0], h[1])
	}
	if r.TLS {
		req.TLS = &tls.ConnectionState{}
	}
	return req
}

func init() {
	register("server.NewConfig", func(json.RawMessage) (interface{}, error) {
		return server.NewConfig(), nil
	})
	register("server.Config.ToToml", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Config server.Config }](raw)
		if err != nil {
			return nil, err
		}
		return a.Config.ToToml(), nil
	})
	register("server.Config.DecodeToml", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Config server.Config
			Toml   string
		}](raw)
		if err != nil {
			return nil, err
		}
		conf := a.Config
		_, derr := toml.Decode(a.Toml, &conf)
		return parsed{conf, errString(derr)}, nil
	})
	register("server.NewSSLConfig", func(json.RawMessage) (interface{}, error) {
		return server.NewSSLConfig(), nil
	})
	register("server.SSLConfig.Available", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Config server.SSLConfig }](raw)
		if err != nil {
			return nil, err
		}
		return a.Config.Available(), nil
	})
	register("server.HealthHandler", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Request testRequest }](raw)
		if err != nil {
			return nil, err
		}
		rr := httptest.NewRecorder()
		http.HandlerFunc(server.HealthHandler).ServeHTTP(rr, a.Request.build())
		return map[string]interface{}{"Code": rr.Code, "Body": rr.Body.String()}, nil
	})
	register("server.DefaultHeadersExtractor.FromRequest", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Extractor server.DefaultHeadersExtractor
			Request   testRequest
		}](raw)
		if err != nil {
			return nil, err
		}
		return a.Extractor.FromRequest(a.Request.build()), nil
	})
	register("server.FetchUID", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Request testRequest }](raw)
		if err != nil {
			return nil, err
		}
		uid, ferr := server.FetchUID(a.Request.build())
		return parsed{uid, errString(ferr)}, nil
	})
	// NewRequestInfo(req, nil) (or a blank RequestInfo{} when Request is null), with
	// Param and AnyCableHeader looked up for the given keys.
	register("server.RequestInfo", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Request         *testRequest
			Params          []string
			AnyCableHeaders []string
		}](raw)
		if err != nil {
			return nil, err
		}
		info := &server.RequestInfo{}
		if a.Request != nil {
			var ierr error
			info, ierr = server.NewRequestInfo(a.Request.build(), nil)
			if ierr != nil {
				return parsed{nil, errString(ierr)}, nil
			}
		}
		params := map[string]string{}
		for _, k := range a.Params {
			params[k] = info.Param(k)
		}
		headers := map[string]string{}
		for _, k := range a.AnyCableHeaders {
			headers[k] = info.AnyCableHeader(k)
		}
		return parsed{map[string]interface{}{
			"UID": info.UID, "URL": info.URL, "Headers": info.Headers,
			"Params": params, "AnyCableHeaders": headers,
		}, nil}, nil
	})
}
