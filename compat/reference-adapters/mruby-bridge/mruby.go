package main

// Ops for the vendored go-mruby binding (package `mruby`). Each op calls exactly
// one exported go-mruby function or method.

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/go-mruby"
)

// valueArg is a go-mruby Value from Rust: Int, String, Nil or a handle to any
// object implementing mruby.Value (*MrbValue, *Class, *Array, *Hash).
type valueArg struct {
	Int *int    `json:"int"`
	Str *string `json:"str"`
	Nil bool    `json:"nil"`
	H   *uint64 `json:"h"`
}

// value returns the mruby.Value (an untyped nil interface when absent).
func (a *valueArg) value() (mruby.Value, error) {
	switch {
	case a == nil:
		return nil, nil
	case a.Int != nil:
		return mruby.Int(*a.Int), nil
	case a.Str != nil:
		return mruby.String(*a.Str), nil
	case a.Nil:
		return mruby.Nil, nil
	case a.H != nil:
		return get[mruby.Value](*a.H)
	}
	return nil, fmt.Errorf("empty value")
}

func values(args []valueArg) ([]mruby.Value, error) {
	vs := make([]mruby.Value, 0, len(args))
	for i := range args {
		v, err := args[i].value()
		if err != nil {
			return nil, err
		}
		vs = append(vs, v)
	}
	return vs, nil
}

type excJSON struct {
	H         *uint64  `json:"h"`
	File      string   `json:"file"`
	Line      int      `json:"line"`
	Message   string   `json:"message"`
	Backtrace []string `json:"backtrace"`
	Error     string   `json:"error"`
}

// fallible carries a go-mruby (*MrbValue, error) pair. An *Exception error is
// returned with its fields; any other error as its Error() text.
type fallible struct {
	Value *uint64  `json:"value"`
	Exc   *excJSON `json:"exc"`
	Error *string  `json:"error"`
}

func newFallible(v *mruby.MrbValue, err error) fallible {
	f := fallible{Value: put(v)}
	if exc, ok := err.(*mruby.Exception); ok {
		f.Exc = &excJSON{H: put(exc.MrbValue), File: exc.File, Line: exc.Line, Message: exc.Message,
			Backtrace: exc.Backtrace, Error: exc.Error()}
	} else if err != nil {
		s := err.Error()
		f.Error = &s
	}
	return f
}

type parserMessage struct {
	Col     int    `json:"col"`
	Line    int    `json:"line"`
	Message string `json:"message"`
}

func parserMessages(ms []*mruby.ParserMessage) []parserMessage {
	if ms == nil {
		return nil
	}
	out := make([]parserMessage, len(ms))
	for i, m := range ms {
		out[i] = parserMessage{Col: m.Col, Line: m.Line, Message: m.Message}
	}
	return out
}

// fn builds a go-mruby Func that runs Rust callback `id` of session s.
func (s *session) fn(id uint64) mruby.Func {
	return func(m *mruby.Mrb, self *mruby.MrbValue) (mruby.Value, mruby.Value) {
		raw := s.callback(id, map[string]interface{}{"mrb": put(m), "self": put(self)})
		var r struct {
			Ret *valueArg `json:"ret"`
			Exc *valueArg `json:"exc"`
		}
		if raw == nil || json.Unmarshal(raw, &r) != nil {
			return nil, nil
		}
		ret, err1 := r.Ret.value()
		exc, err2 := r.Exc.value()
		if err1 != nil || err2 != nil {
			return nil, nil
		}
		return ret, exc
	}
}

type h struct {
	H uint64 `json:"h"`
}

type hName struct {
	H    uint64 `json:"h"`
	Name string `json:"name"`
}

// simple registers an op taking only a handle of type T.
func simple[T any](name string, f func(T) interface{}) {
	register(name, func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[h](raw)
		if err != nil {
			return nil, err
		}
		t, err := get[T](a.H)
		if err != nil {
			return nil, err
		}
		return f(t), nil
	})
}

func init() {
	// ---------------------------------------------------------------- Mrb
	register("mruby.NewMrb", func(*session, json.RawMessage) (interface{}, error) {
		return put(mruby.NewMrb()), nil
	})
	simple("mruby.Mrb.Close", func(m *mruby.Mrb) interface{} { m.Close(); return nil })
	simple("mruby.Mrb.ArenaSave", func(m *mruby.Mrb) interface{} { return int(m.ArenaSave()) })
	register("mruby.Mrb.ArenaRestore", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H   uint64 `json:"h"`
			Idx int    `json:"idx"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		m.ArenaRestore(mruby.ArenaIndex(a.Idx))
		return nil, nil
	})
	simple("mruby.Mrb.EnableGC", func(m *mruby.Mrb) interface{} { m.EnableGC(); return nil })
	simple("mruby.Mrb.DisableGC", func(m *mruby.Mrb) interface{} { m.DisableGC(); return nil })
	simple("mruby.Mrb.FullGC", func(m *mruby.Mrb) interface{} { m.FullGC(); return nil })
	simple("mruby.Mrb.IncrementalGC", func(m *mruby.Mrb) interface{} { m.IncrementalGC(); return nil })
	simple("mruby.Mrb.LiveObjectCount", func(m *mruby.Mrb) interface{} { return m.LiveObjectCount() })
	simple("mruby.Mrb.ObjectClass", func(m *mruby.Mrb) interface{} { return put(m.ObjectClass()) })
	simple("mruby.Mrb.KernelModule", func(m *mruby.Mrb) interface{} { return put(m.KernelModule()) })
	simple("mruby.Mrb.TopSelf", func(m *mruby.Mrb) interface{} { return put(m.TopSelf()) })
	simple("mruby.Mrb.FalseValue", func(m *mruby.Mrb) interface{} { return put(m.FalseValue()) })
	simple("mruby.Mrb.NilValue", func(m *mruby.Mrb) interface{} { return put(m.NilValue()) })
	simple("mruby.Mrb.TrueValue", func(m *mruby.Mrb) interface{} { return put(m.TrueValue()) })
	simple("mruby.Mrb.GetArgs", func(m *mruby.Mrb) interface{} {
		args := m.GetArgs()
		hs := make([]*uint64, len(args))
		for i, v := range args {
			hs[i] = put(v)
		}
		return hs
	})
	register("mruby.Mrb.FixnumValue", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H uint64 `json:"h"`
			N int    `json:"n"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		return put(m.FixnumValue(a.N)), nil
	})
	register("mruby.Mrb.StringValue", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H uint64 `json:"h"`
			S string `json:"s"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		return put(m.StringValue(a.S)), nil
	})
	register("mruby.Mrb.LoadString", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H    uint64 `json:"h"`
			Code string `json:"code"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		return newFallible(m.LoadString(a.Code)), nil
	})
	register("mruby.Mrb.GetGlobalVariable", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[hName](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		return put(m.GetGlobalVariable(a.Name)), nil
	})
	register("mruby.Mrb.SetGlobalVariable", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64   `json:"h"`
			Name  string   `json:"name"`
			Value valueArg `json:"value"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		v, err := a.Value.value()
		if err != nil {
			return nil, err
		}
		m.SetGlobalVariable(a.Name, v)
		return nil, nil
	})
	register("mruby.Mrb.Class", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64  `json:"h"`
			Name  string  `json:"name"`
			Super *uint64 `json:"super"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		super, err := getOpt[*mruby.Class](a.Super)
		if err != nil {
			return nil, err
		}
		return put(m.Class(a.Name, super)), nil
	})
	register("mruby.Mrb.Module", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[hName](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		return put(m.Module(a.Name)), nil
	})
	register("mruby.Mrb.ConstDefined", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64   `json:"h"`
			Name  string   `json:"name"`
			Scope valueArg `json:"scope"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		scope, err := a.Scope.value()
		if err != nil {
			return nil, err
		}
		return m.ConstDefined(a.Name, scope), nil
	})
	register("mruby.Mrb.DefineClass", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64  `json:"h"`
			Name  string  `json:"name"`
			Super *uint64 `json:"super"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		super, err := getOpt[*mruby.Class](a.Super)
		if err != nil {
			return nil, err
		}
		return put(m.DefineClass(a.Name, super)), nil
	})
	register("mruby.Mrb.DefineClassUnder", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64  `json:"h"`
			Name  string  `json:"name"`
			Super *uint64 `json:"super"`
			Outer *uint64 `json:"outer"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		super, err := getOpt[*mruby.Class](a.Super)
		if err != nil {
			return nil, err
		}
		outer, err := getOpt[*mruby.Class](a.Outer)
		if err != nil {
			return nil, err
		}
		return put(m.DefineClassUnder(a.Name, super, outer)), nil
	})
	register("mruby.Mrb.DefineModule", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[hName](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		return put(m.DefineModule(a.Name)), nil
	})
	register("mruby.Mrb.DefineModuleUnder", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64  `json:"h"`
			Name  string  `json:"name"`
			Outer *uint64 `json:"outer"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		outer, err := getOpt[*mruby.Class](a.Outer)
		if err != nil {
			return nil, err
		}
		return put(m.DefineModuleUnder(a.Name, outer)), nil
	})
	type runArgs struct {
		H         uint64    `json:"h"`
		Proc      valueArg  `json:"proc"`
		Self      *valueArg `json:"self"`
		StackKeep int       `json:"stack_keep"`
	}
	register("mruby.Mrb.Run", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[runArgs](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		proc, err := a.Proc.value()
		if err != nil {
			return nil, err
		}
		self, err := a.Self.value()
		if err != nil {
			return nil, err
		}
		return newFallible(m.Run(proc, self)), nil
	})
	register("mruby.Mrb.RunWithContext", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[runArgs](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		proc, err := a.Proc.value()
		if err != nil {
			return nil, err
		}
		self, err := a.Self.value()
		if err != nil {
			return nil, err
		}
		keep, v, rerr := m.RunWithContext(proc, self, a.StackKeep)
		return map[string]interface{}{"stack_keep": keep, "result": newFallible(v, rerr)}, nil
	})
	register("mruby.Mrb.Yield", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64     `json:"h"`
			Block valueArg   `json:"block"`
			Args  []valueArg `json:"args"`
		}](raw)
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.H)
		if err != nil {
			return nil, err
		}
		block, err := a.Block.value()
		if err != nil {
			return nil, err
		}
		args, err := values(a.Args)
		if err != nil {
			return nil, err
		}
		return newFallible(m.Yield(block, args...)), nil
	})

	// ---------------------------------------------------------------- MrbValue
	simple("mruby.Value.Type", func(v *mruby.MrbValue) interface{} { return uint32(v.Type()) })
	simple("mruby.Value.String", func(v *mruby.MrbValue) interface{} { return v.String() })
	simple("mruby.Value.Fixnum", func(v *mruby.MrbValue) interface{} { return v.Fixnum() })
	simple("mruby.Value.Float", func(v *mruby.MrbValue) interface{} { return v.Float() })
	simple("mruby.Value.IsDead", func(v *mruby.MrbValue) interface{} { return v.IsDead() })
	simple("mruby.Value.Mrb", func(v *mruby.MrbValue) interface{} { return put(v.Mrb()) })
	simple("mruby.Value.GCProtect", func(v *mruby.MrbValue) interface{} { v.GCProtect(); return nil })
	simple("mruby.Value.Array", func(v *mruby.MrbValue) interface{} { return put(v.Array()) })
	simple("mruby.Value.Hash", func(v *mruby.MrbValue) interface{} { return put(v.Hash()) })
	simple("mruby.Value.Class", func(v *mruby.MrbValue) interface{} { return put(v.Class()) })
	simple("mruby.Value.SingletonClass", func(v *mruby.MrbValue) interface{} { return put(v.SingletonClass()) })
	// Value.MrbValue(*Mrb) for any Value (Int, String, Nil, *MrbValue, *Class).
	register("mruby.ToMrbValue", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			Value valueArg `json:"value"`
			Mrb   uint64   `json:"mrb"`
		}](raw)
		if err != nil {
			return nil, err
		}
		v, err := a.Value.value()
		if err != nil {
			return nil, err
		}
		m, err := get[*mruby.Mrb](a.Mrb)
		if err != nil {
			return nil, err
		}
		return put(v.MrbValue(m)), nil
	})
	register("mruby.Value.GetInstanceVariable", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[hName](raw)
		if err != nil {
			return nil, err
		}
		v, err := get[*mruby.MrbValue](a.H)
		if err != nil {
			return nil, err
		}
		return put(v.GetInstanceVariable(a.Name)), nil
	})
	register("mruby.Value.SetInstanceVariable", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64 `json:"h"`
			Name  string `json:"name"`
			Value uint64 `json:"value"`
		}](raw)
		if err != nil {
			return nil, err
		}
		v, err := get[*mruby.MrbValue](a.H)
		if err != nil {
			return nil, err
		}
		val, err := get[*mruby.MrbValue](a.Value)
		if err != nil {
			return nil, err
		}
		v.SetInstanceVariable(a.Name, val)
		return nil, nil
	})
	register("mruby.Value.SetProcTargetClass", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64 `json:"h"`
			Class uint64 `json:"class"`
		}](raw)
		if err != nil {
			return nil, err
		}
		v, err := get[*mruby.MrbValue](a.H)
		if err != nil {
			return nil, err
		}
		c, err := get[*mruby.Class](a.Class)
		if err != nil {
			return nil, err
		}
		v.SetProcTargetClass(c)
		return nil, nil
	})
	type callArgs struct {
		H      uint64     `json:"h"`
		Method string     `json:"method"`
		Args   []valueArg `json:"args"`
	}
	register("mruby.Value.Call", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[callArgs](raw)
		if err != nil {
			return nil, err
		}
		v, err := get[*mruby.MrbValue](a.H)
		if err != nil {
			return nil, err
		}
		args, err := values(a.Args)
		if err != nil {
			return nil, err
		}
		return newFallible(v.Call(a.Method, args...)), nil
	})
	// Args carries the block as its last element, as CallBlock expects.
	register("mruby.Value.CallBlock", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[callArgs](raw)
		if err != nil {
			return nil, err
		}
		v, err := get[*mruby.MrbValue](a.H)
		if err != nil {
			return nil, err
		}
		args, err := values(a.Args)
		if err != nil {
			return nil, err
		}
		return newFallible(v.CallBlock(a.Method, args...)), nil
	})

	// ---------------------------------------------------------------- Class
	type defineArgs struct {
		H    uint64 `json:"h"`
		Name string `json:"name"`
		Cb   uint64 `json:"cb"`
		Spec uint32 `json:"spec"`
	}
	register("mruby.Class.DefineClassMethod", func(s *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[defineArgs](raw)
		if err != nil {
			return nil, err
		}
		c, err := get[*mruby.Class](a.H)
		if err != nil {
			return nil, err
		}
		c.DefineClassMethod(a.Name, s.fn(a.Cb), mruby.ArgSpec(a.Spec))
		return nil, nil
	})
	register("mruby.Class.DefineMethod", func(s *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[defineArgs](raw)
		if err != nil {
			return nil, err
		}
		c, err := get[*mruby.Class](a.H)
		if err != nil {
			return nil, err
		}
		c.DefineMethod(a.Name, s.fn(a.Cb), mruby.ArgSpec(a.Spec))
		return nil, nil
	})
	register("mruby.Class.DefineConst", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H     uint64   `json:"h"`
			Name  string   `json:"name"`
			Value valueArg `json:"value"`
		}](raw)
		if err != nil {
			return nil, err
		}
		c, err := get[*mruby.Class](a.H)
		if err != nil {
			return nil, err
		}
		v, err := a.Value.value()
		if err != nil {
			return nil, err
		}
		c.DefineConst(a.Name, v)
		return nil, nil
	})
	register("mruby.Class.New", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H    uint64     `json:"h"`
			Args []valueArg `json:"args"`
		}](raw)
		if err != nil {
			return nil, err
		}
		c, err := get[*mruby.Class](a.H)
		if err != nil {
			return nil, err
		}
		args, err := values(a.Args)
		if err != nil {
			return nil, err
		}
		return newFallible(c.New(args...)), nil
	})
	// reflect.DeepEqual of two *Class, as TestValueClass compares them.
	register("mruby.Class.DeepEqual", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			A uint64 `json:"a"`
			B uint64 `json:"b"`
		}](raw)
		if err != nil {
			return nil, err
		}
		x, err := get[*mruby.Class](a.A)
		if err != nil {
			return nil, err
		}
		y, err := get[*mruby.Class](a.B)
		if err != nil {
			return nil, err
		}
		return deepEqual(x, y), nil
	})

	// ---------------------------------------------------------------- ArgSpec
	register("mruby.ArgSpec", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			Kind string `json:"kind"`
			N    int    `json:"n"`
			O    int    `json:"o"`
		}](raw)
		if err != nil {
			return nil, err
		}
		var s mruby.ArgSpec
		switch a.Kind {
		case "any":
			s = mruby.ArgsAny()
		case "none":
			s = mruby.ArgsNone()
		case "block":
			s = mruby.ArgsBlock()
		case "req":
			s = mruby.ArgsReq(a.N)
		case "opt":
			s = mruby.ArgsOpt(a.N)
		case "arg":
			s = mruby.ArgsArg(a.N, a.O)
		default:
			return nil, fmt.Errorf("unknown arg spec %q", a.Kind)
		}
		return uint32(s), nil
	})

	// ---------------------------------------------------------------- Array, Hash
	simple("mruby.Array.Len", func(a *mruby.Array) interface{} { return a.Len() })
	register("mruby.Array.Get", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H   uint64 `json:"h"`
			Idx int    `json:"idx"`
		}](raw)
		if err != nil {
			return nil, err
		}
		arr, err := get[*mruby.Array](a.H)
		if err != nil {
			return nil, err
		}
		return newFallible(arr.Get(a.Idx)), nil
	})
	type hashArgs struct {
		H     uint64    `json:"h"`
		Key   valueArg  `json:"key"`
		Value *valueArg `json:"value"`
	}
	hashOp := func(name string, f func(*mruby.Hash, mruby.Value, mruby.Value) fallible) {
		register(name, func(_ *session, raw json.RawMessage) (interface{}, error) {
			a, err := decode[hashArgs](raw)
			if err != nil {
				return nil, err
			}
			hash, err := get[*mruby.Hash](a.H)
			if err != nil {
				return nil, err
			}
			key, err := a.Key.value()
			if err != nil {
				return nil, err
			}
			val, err := a.Value.value()
			if err != nil {
				return nil, err
			}
			return f(hash, key, val), nil
		})
	}
	hashOp("mruby.Hash.Get", func(hash *mruby.Hash, k, _ mruby.Value) fallible { return newFallible(hash.Get(k)) })
	hashOp("mruby.Hash.Delete", func(hash *mruby.Hash, k, _ mruby.Value) fallible { return newFallible(hash.Delete(k)) })
	hashOp("mruby.Hash.Set", func(hash *mruby.Hash, k, v mruby.Value) fallible { return newFallible(nil, hash.Set(k, v)) })
	simple("mruby.Hash.Keys", func(hash *mruby.Hash) interface{} { return newFallible(hash.Keys()) })

	// ---------------------------------------------------------------- Parser, CompileContext
	simple("mruby.NewParser", func(m *mruby.Mrb) interface{} { return put(mruby.NewParser(m)) })
	simple("mruby.Parser.Close", func(p *mruby.Parser) interface{} { p.Close(); return nil })
	simple("mruby.Parser.GenerateCode", func(p *mruby.Parser) interface{} { return put(p.GenerateCode()) })
	register("mruby.Parser.Parse", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H    uint64  `json:"h"`
			Code string  `json:"code"`
			Ctx  *uint64 `json:"ctx"`
		}](raw)
		if err != nil {
			return nil, err
		}
		p, err := get[*mruby.Parser](a.H)
		if err != nil {
			return nil, err
		}
		ctx, err := getOpt[*mruby.CompileContext](a.Ctx)
		if err != nil {
			return nil, err
		}
		warns, perr := p.Parse(a.Code, ctx)
		res := map[string]interface{}{"warnings": parserMessages(warns), "error": nil}
		if pe, ok := perr.(*mruby.ParserError); ok {
			res["error"] = map[string]interface{}{"errors": parserMessages(pe.Errors), "message": pe.Error()}
		} else if perr != nil {
			return nil, fmt.Errorf("unexpected parse error type %T", perr)
		}
		return res, nil
	})
	// The zero ParserError (`new(ParserError)` in TestParserError_error).
	register("mruby.ParserError.Zero", func(*session, json.RawMessage) (interface{}, error) {
		var e error = new(mruby.ParserError)
		return e.Error(), nil
	})
	simple("mruby.NewCompileContext", func(m *mruby.Mrb) interface{} { return put(mruby.NewCompileContext(m)) })
	simple("mruby.CompileContext.Close", func(c *mruby.CompileContext) interface{} { c.Close(); return nil })
	simple("mruby.CompileContext.Filename", func(c *mruby.CompileContext) interface{} { return c.Filename() })
	register("mruby.CompileContext.SetFilename", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[hName](raw)
		if err != nil {
			return nil, err
		}
		c, err := get[*mruby.CompileContext](a.H)
		if err != nil {
			return nil, err
		}
		c.SetFilename(a.Name)
		return nil, nil
	})
	register("mruby.CompileContext.CaptureErrors", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H   uint64 `json:"h"`
			Yes bool   `json:"yes"`
		}](raw)
		if err != nil {
			return nil, err
		}
		c, err := get[*mruby.CompileContext](a.H)
		if err != nil {
			return nil, err
		}
		c.CaptureErrors(a.Yes)
		return nil, nil
	})

	// ---------------------------------------------------------------- Decode
	register("mruby.Decode", func(_ *session, raw json.RawMessage) (interface{}, error) {
		a, err := decode[struct {
			H      uint64 `json:"h"`
			Target string `json:"target"`
		}](raw)
		if err != nil {
			return nil, err
		}
		v, err := get[*mruby.MrbValue](a.H)
		if err != nil {
			return nil, err
		}
		result, known, derr := decodeInto(a.Target, v)
		if !known {
			return nil, fmt.Errorf("unknown decode target %q", a.Target)
		}
		res := map[string]interface{}{"result": result, "error": nil}
		if derr != nil {
			res["error"] = derr.Error()
		}
		return res, nil
	})
}
