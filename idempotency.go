package pipeflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

var ErrIdempotencyInProgress = errors.New("pipeflow: idempotent work already in progress")

type IdempotencyClaimState string

const (
	IdempotencyClaimed    IdempotencyClaimState = "claimed"
	IdempotencyInProgress IdempotencyClaimState = "in_progress"
	IdempotencyCompleted  IdempotencyClaimState = "completed"
)

type IdempotencyOutcome string

const (
	IdempotencyExecuted            IdempotencyOutcome = "completed"
	IdempotencyFailedReleasable    IdempotencyOutcome = "failed_releasable"
	IdempotencyDuplicateCompleted  IdempotencyOutcome = "duplicate_completed"
	IdempotencyDuplicateInProgress IdempotencyOutcome = "in_progress"
	IdempotencyStoreFailure        IdempotencyOutcome = "store_failure"
)

// IdempotencyStore supplies atomic claim state. Persistence, leases, and crash
// consistency are properties of the implementation, not Pipeflow Core.
type IdempotencyStore interface {
	Claim(context.Context, string) (IdempotencyClaimState, error)
	Complete(context.Context, string) error
	Release(context.Context, string) error
}

type IdempotencyInProgressError struct{ Guard string }

func (e *IdempotencyInProgressError) Error() string {
	return fmt.Sprintf("pipeflow: idempotency guard %q: %v", e.Guard, ErrIdempotencyInProgress)
}
func (e *IdempotencyInProgressError) Unwrap() error { return ErrIdempotencyInProgress }

type compiledIdempotencyKey struct {
	inputType reflect.Type
	extract   func(any) (string, error)
}

// IdempotencyGuard binds a key namespace, atomic store, and typed key extractor.
type IdempotencyGuard struct {
	name  string
	store IdempotencyStore
	key   *compiledIdempotencyKey
}

func NewIdempotencyGuard(name string, store IdempotencyStore, keyExtractor any) (*IdempotencyGuard, error) {
	if name == "" {
		return nil, fmt.Errorf("pipeflow: idempotency guard name cannot be empty")
	}
	if store == nil || isNilInterface(store) {
		return nil, fmt.Errorf("pipeflow: idempotency guard %q has nil store", name)
	}
	key, err := compileIdempotencyKey(name, keyExtractor)
	if err != nil {
		return nil, err
	}
	return &IdempotencyGuard{name: name, store: store, key: key}, nil
}

func compileIdempotencyKey(name string, extractor any) (*compiledIdempotencyKey, error) {
	t := reflect.TypeOf(extractor)
	if t == nil || t.Kind() != reflect.Func || t.NumIn() > 1 || (t.NumOut() != 1 && t.NumOut() != 2) || t.Out(0).Kind() != reflect.String || (t.NumOut() == 2 && t.Out(1) != errorType) {
		return nil, fmt.Errorf("pipeflow: idempotency guard %q key extractor must be func() string, func() (string, error), func(T) string, or func(T) (string, error)", name)
	}
	var inputType reflect.Type
	if t.NumIn() == 1 {
		inputType = t.In(0)
	}
	v := reflect.ValueOf(extractor)
	return &compiledIdempotencyKey{inputType: inputType, extract: func(input any) (key string, err error) {
		defer recoverPanic(&err)
		args := []reflect.Value{}
		if inputType != nil {
			args = append(args, reflectValue(input, inputType))
		}
		results := v.Call(args)
		if len(results) == 2 && !results[1].IsNil() {
			return "", results[1].Interface().(error)
		}
		key = results[0].String()
		if key == "" {
			return "", fmt.Errorf("pipeflow: idempotency guard %q produced an empty key", name)
		}
		return key, nil
	}}, nil
}

func (g *IdempotencyGuard) storageKey(key string) string { return g.name + "\x00" + key }

// WithIdempotencyGuard protects a pass-through Step from duplicate completed work.
func (s *Step) WithIdempotencyGuard(guard *IdempotencyGuard) *Step {
	if s == nil {
		return nil
	}
	if guard == nil && s.configErr == nil {
		s.configErr = fmt.Errorf("pipeflow: step %q has nil idempotency guard", s.name)
	}
	s.idempotency = guard
	return s
}

type memoryIdempotencyStore struct {
	mu     sync.Mutex
	states map[string]IdempotencyClaimState
}

// NewMemoryIdempotencyStore provides concurrency-safe process-local semantics.
// It is not durable and does not provide cross-process duplicate protection.
func NewMemoryIdempotencyStore() IdempotencyStore {
	return &memoryIdempotencyStore{states: make(map[string]IdempotencyClaimState)}
}
func (s *memoryIdempotencyStore) Claim(ctx context.Context, key string) (IdempotencyClaimState, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.states[key] {
	case IdempotencyClaimed:
		return IdempotencyInProgress, nil
	case IdempotencyCompleted:
		return IdempotencyCompleted, nil
	default:
		s.states[key] = IdempotencyClaimed
		return IdempotencyClaimed, nil
	}
}
func (s *memoryIdempotencyStore) Complete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states[key] != IdempotencyClaimed {
		return fmt.Errorf("pipeflow: cannot complete unclaimed idempotency key")
	}
	s.states[key] = IdempotencyCompleted
	return nil
}
func (s *memoryIdempotencyStore) Release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states[key] == IdempotencyClaimed {
		delete(s.states, key)
	}
	return nil
}
