package main

// Ops for upstream packages `nats` and `enats` (configuration only; the
// embedded server itself is not exercised by upstream unit tests).

import (
	"encoding/json"

	"github.com/anycable/anycable-go/enats"
	"github.com/anycable/anycable-go/nats"
)

func init() {
	msgConfigOps("nats.NATSConfig", nats.NewNATSConfig, nats.NATSConfig.ToToml)
	register("nats.NATSConfigFilteredServers", func(raw json.RawMessage) (interface{}, error) {
		a, err := msgArgs[struct{ Config nats.NATSConfig }](raw)
		if err != nil {
			return nil, err
		}
		return a.Config.FilteredServers(), nil
	})

	msgConfigOps("enats.Config", enats.NewConfig, enats.Config.ToToml)
}
