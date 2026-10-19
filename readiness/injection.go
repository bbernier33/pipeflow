package readiness

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/bbernier33/pipeflow/v3"
)

// Fault configures controlled conditions at a function execution boundary.
// Calls limits injection to the first N invocations; zero means every invocation.
// A wrapped function owns its counter, shared across all runs using that wrapper.
type Fault struct {
	Calls int
	Delay time.Duration
	Error error
	Panic any
	// WaitForCancellation requires the context-aware Pipeflow function signature.
	WaitForCancellation bool
	// Cancel invokes an explicit caller-owned cancellation API at the boundary.
	Cancel context.CancelFunc
}

// Inject wraps a supported Step function while preserving its exact Go type.
// Errors replace the invocation; delays precede it. Core owns retry, timeout,
// panic recovery, reporting, and finalization. Create a fresh wrapper to reset
// its invocation counter. The original function is never modified.
func Inject(function any, fault Fault) (any, error) {
	typeOf := reflect.TypeOf(function)
	if typeOf == nil || typeOf.Kind() != reflect.Func || reflect.ValueOf(function).IsNil() {
		return nil, errors.New("readiness: injection requires a non-nil function")
	}
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	legacyType := reflect.TypeOf((func(context.Context, *pipeflow.Context, any) (any, error))(nil))
	legacy := typeOf == legacyType
	if !legacy && (typeOf.NumIn() > 1 || typeOf.IsVariadic() || (typeOf.NumOut() != 1 && typeOf.NumOut() != 2) || typeOf.Out(typeOf.NumOut()-1) != errorType) {
		return nil, fmt.Errorf("readiness: unsupported injection function signature %s", typeOf)
	}
	if fault.Calls < 0 || fault.Delay < 0 {
		return nil, errors.New("readiness: injection calls and delay cannot be negative")
	}
	if fault.WaitForCancellation && !legacy {
		return nil, errors.New("readiness: waiting for cancellation requires func(context.Context, *pipeflow.Context, any) (any, error)")
	}
	var calls atomic.Int64
	original := reflect.ValueOf(function)
	wrapped := reflect.MakeFunc(typeOf, func(arguments []reflect.Value) []reflect.Value {
		invocation := calls.Add(1)
		if fault.Calls > 0 && invocation > int64(fault.Calls) {
			return original.Call(arguments)
		}
		ctx := context.Background()
		if legacy {
			ctx = arguments[0].Interface().(context.Context)
		}
		if fault.Cancel != nil {
			fault.Cancel()
		}
		failure := fault.Error
		if fault.Delay > 0 {
			timer := time.NewTimer(fault.Delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				failure = ctx.Err()
			}
			timer.Stop()
		}
		if fault.WaitForCancellation {
			<-ctx.Done()
			failure = ctx.Err()
		}
		if fault.Panic != nil {
			panic(fault.Panic)
		}
		if failure != nil {
			results := make([]reflect.Value, typeOf.NumOut())
			for index := range results {
				results[index] = reflect.Zero(typeOf.Out(index))
			}
			results[len(results)-1] = reflect.ValueOf(&failure).Elem()
			return results
		}
		return original.Call(arguments)
	})
	return wrapped.Interface(), nil
}
