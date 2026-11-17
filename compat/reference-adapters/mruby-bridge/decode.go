package main

// Targets for mruby.Decode. Go decodes by reflection into the static type of its
// output; Rust names that type, and the result is returned as JSON. Interface
// results are tagged with their dynamic Go type so int/float stay distinct.

import (
	"fmt"
	"reflect"

	"github.com/mitchellh/go-mruby"
)

// structString mirrors the struct declared in decode_test.go (TestDecode). The
// json tag only names the field in the JSON reply; Decode uses the field name.
type structString struct {
	Foo string `json:"foo"`
}

func decodeInto(target string, v *mruby.MrbValue) (interface{}, bool, error) {
	switch target {
	case "bool":
		var out bool
		err := mruby.Decode(&out, v)
		return out, true, err
	case "float64":
		var out float64
		err := mruby.Decode(&out, v)
		return out, true, err
	case "int":
		var out int
		err := mruby.Decode(&out, v)
		return out, true, err
	case "map[string]string":
		var out map[string]string
		err := mruby.Decode(&out, v)
		return out, true, err
	case "[]string":
		var out []string
		err := mruby.Decode(&out, v)
		return out, true, err
	case "*int":
		var out *int
		err := mruby.Decode(&out, v)
		return out, true, err
	case "string":
		var out string
		err := mruby.Decode(&out, v)
		return out, true, err
	case "structString":
		var out structString
		err := mruby.Decode(&out, v)
		return out, true, err
	case "interface{}":
		var out interface{}
		err := mruby.Decode(&out, v)
		return tagged(out), true, err
	}
	return nil, false, nil
}

type taggedValue struct {
	T string      `json:"t"`
	V interface{} `json:"v"`
}

func tagged(x interface{}) interface{} {
	switch x := x.(type) {
	case nil:
		return nil
	case bool:
		return taggedValue{"bool", x}
	case int:
		return taggedValue{"int", x}
	case float64:
		return taggedValue{"float", x}
	case string:
		return taggedValue{"string", x}
	case map[string]interface{}:
		m := make(map[string]interface{}, len(x))
		for k, v := range x {
			m[k] = tagged(v)
		}
		return taggedValue{"map", m}
	case []interface{}:
		s := make([]interface{}, len(x))
		for i, v := range x {
			s[i] = tagged(v)
		}
		return taggedValue{"slice", s}
	}
	return taggedValue{"unknown", fmt.Sprintf("%T", x)}
}

func deepEqual(a, b interface{}) bool { return reflect.DeepEqual(a, b) }
