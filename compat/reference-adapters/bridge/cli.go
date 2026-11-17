package main

import (
	"encoding/json"
	"os"

	anycli "github.com/anycable/anycable-go/cli"
	"github.com/anycable/anycable-go/config"
	"github.com/urfave/cli/v2"
)

func init() {
	// NewConfigFromCLI reads the process environment (flag EnvVars) and ./anycable.toml:
	// tests call it in a bridge process with its own environment and working directory.
	// Usage/version output goes to os.Stdout, which is redirected so it cannot corrupt
	// the protocol stream.
	register("cli.NewConfigFromCLI", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Args              []string
			CustomStringFlags []struct {
				Name, Value string
				Required    bool
			}
		}](raw)
		if err != nil {
			return nil, err
		}
		custom := make([]string, len(a.CustomStringFlags))
		var cliOpts []func() ([]cli.Flag, error)
		if len(a.CustomStringFlags) > 0 {
			cliOpts = append(cliOpts, func() ([]cli.Flag, error) {
				flags := []cli.Flag{}
				for i, f := range a.CustomStringFlags {
					flags = append(flags, &cli.StringFlag{
						Name: f.Name, Destination: &custom[i], Value: f.Value, Required: f.Required,
					})
				}
				return flags, nil
			})
		}

		devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			return nil, err
		}
		stdout := os.Stdout
		os.Stdout = devnull
		defer func() {
			os.Stdout = stdout
			devnull.Close()
		}()

		var conf *config.Config
		var cerr error
		var interrupted bool
		if len(cliOpts) > 0 {
			conf, cerr, interrupted = anycli.NewConfigFromCLI(a.Args, anycli.WithCLICustomOptions(cliOpts[0]))
		} else {
			conf, cerr, interrupted = anycli.NewConfigFromCLI(a.Args)
		}
		values := map[string]string{}
		for i, f := range a.CustomStringFlags {
			values[f.Name] = custom[i]
		}
		return map[string]interface{}{
			"Config": viewOfConfig(conf), "Toml": conf.ToToml(), "Error": errString(cerr),
			"Interrupted": interrupted, "Custom": values,
		}, nil
	})
}
