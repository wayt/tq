package async

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/wayt/tq"
	//"log"
	"reflect"
	"time"
)

var (
	funcs       = make(map[string]*Function)
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

type Function struct {
	Name string
	fv   reflect.Value // Kind() == reflect.Func
}

func Func(name string, i interface{}) *Function {

	f := &Function{
		Name: name,
		fv:   reflect.ValueOf(i),
	}

	t := f.fv.Type()
	if t.Kind() != reflect.Func {
		panic("not a function")
	}

	if t.NumIn() == 0 || t.In(0) != contextType {
		panic("first func argument must be a context.Context")
	}

	funcs[name] = f

	return f
}

func (f *Function) Call(args ...interface{}) (int64, error) {

	return f.CallIn(0, args...)
}

func (f *Function) CallAt(at time.Time, args ...interface{}) (int64, error) {
	in := at.Sub(time.Now())

	return f.CallIn(in, args...)
}

func (f *Function) CallIn(in time.Duration, args ...interface{}) (int64, error) {

	task, err := f.Task(args...)
	if err != nil {
		return 0, err
	}

	if err := tq.Add(task); err != nil {
		return 0, err
	}

	return task.ID, nil
}

type invocation struct {
	Args []interface{} `json:"args"`
}

func (f *Function) Task(args ...interface{}) (*tq.Task, error) {

	inv := invocation{
		Args: args,
	}

	data, err := json.Marshal(inv)
	if err != nil {
		return nil, fmt.Errorf("json marshal failed: %v", err)
	}

	return tq.NewTask(f.Name, string(data)), nil
}

func RunTask(ctx context.Context, t *tq.Task) (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	var inv invocation
	if err = json.Unmarshal([]byte(t.Args), &inv); err != nil {
		err = fmt.Errorf("fail to unmarshal task args: %v", err)
		return
	}

	f := funcs[t.Name]
	if f == nil {
		err = fmt.Errorf("no func with name %s found", t.Name)
		return
	}

	ft := f.fv.Type()
	in := []reflect.Value{reflect.ValueOf(ctx)}

	if len(inv.Args)+1 /* context.Context */ != ft.NumIn() {
		err = fmt.Errorf("[%s]: bad arguments count, got %d, expect %d", f.Name, len(inv.Args), ft.NumIn())
		return
	}

	if ft.NumIn() > 1 {
		for i, arg := range inv.Args {
			var v reflect.Value
			if arg != nil {

				paramType := ft.In(i + 1)

				tmp := reflect.New(paramType)
				mapstructure.Decode(arg, tmp.Interface())

				v = tmp.Elem()
			} else {
				// Task was passed a nil argument, so we must construct
				// the zero value for the argument here.
				n := len(in) // we're constructing the nth argument
				var at reflect.Type
				if !ft.IsVariadic() || n+1 < ft.NumIn()-1 {
					at = ft.In(n + 1)
				} else {
					at = ft.In(ft.NumIn() - 1).Elem()
				}
				v = reflect.Zero(at)
			}
			in = append(in, v)
		}
	}

	out := f.fv.Call(in)

	if n := ft.NumOut(); n > 0 && ft.Out(n-1) == errorType {
		if errv := out[n-1]; !errv.IsNil() {
			err = errv.Interface().(error)
			return
		}
	}

	return
}
