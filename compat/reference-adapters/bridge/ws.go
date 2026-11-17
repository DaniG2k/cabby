package main

import (
	"encoding/json"
	"net/http/httptest"

	"github.com/anycable/anycable-go/ws"
)

func init() {
	register("ws.CheckOrigin", func(raw json.RawMessage) (interface{}, error) {
		var a struct {
			Origins string
			Origin  *string // nil: no Origin header
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		req := httptest.NewRequest("GET", "/", nil)
		if a.Origin != nil {
			req.Header.Set("Origin", *a.Origin)
		}
		return ws.CheckOrigin(a.Origins)(req), nil
	})
}
