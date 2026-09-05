package pipeflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type idempotencyWork struct {
	ID    string
	Value int
}

func guardForWork(t *testing.T, store IdempotencyStore) *IdempotencyGuard {
	t.Helper()
	guard, err := NewIdempotencyGuard("payments", store, func(work idempotencyWork) string { return work.ID })
	if err != nil {
		t.Fatal(err)
	}
	return guard
}

func TestIdempotencyDuplicateCompletedSkipsSideEffectAndPreservesFlow(t *testing.T) {
	guard := guardForWork(t, NewMemoryIdempotencyStore())
	var calls atomic.Int32
	p := NewPipeline("p", NewStage("s", NewStep("charge", func(work idempotencyWork) error { calls.Add(1); return nil }).WithIdempotencyGuard(guard)))
	input := idempotencyWork{ID: "order-1", Value: 42}
	first, _, err := p.RunWithReport(context.Background(), input)
	if err != nil || first != input {
		t.Fatalf("first=%v err=%v", first, err)
	}
	second, report, err := p.RunWithReport(context.Background(), input)
	if err != nil || second != input || calls.Load() != 1 {
		t.Fatalf("second=%v calls=%d err=%v", second, calls.Load(), err)
	}
	step := report.Stages[0].Steps[0]
	if step.Status != StatusSkipped || step.Idempotency == nil || step.Idempotency.Outcome != IdempotencyDuplicateCompleted {
		t.Fatalf("step=%+v", step)
	}
}

func TestIdempotencyConcurrentClaimRejectsInProgressDuplicate(t *testing.T) {
	guard := guardForWork(t, NewMemoryIdempotencyStore())
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	p := NewPipeline("p", NewStage("s", NewStep("charge", func(idempotencyWork) error { once.Do(func() { close(started) }); <-release; return nil }).WithIdempotencyGuard(guard)))
	done := make(chan error, 1)
	go func() { _, _, err := p.RunWithReport(context.Background(), idempotencyWork{ID: "same"}); done <- err }()
	<-started
	_, report, err := p.RunWithReport(context.Background(), idempotencyWork{ID: "same"})
	if !errors.Is(err, ErrIdempotencyInProgress) {
		t.Fatalf("err=%v", err)
	}
	if got := report.Stages[0].Steps[0].Idempotency; got == nil || got.Outcome != IdempotencyDuplicateInProgress {
		t.Fatalf("report=%+v", got)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestIdempotencyFailureReleasesClaim(t *testing.T) {
	guard := guardForWork(t, NewMemoryIdempotencyStore())
	boom := errors.New("charge failed")
	var calls atomic.Int32
	p := NewPipeline("p", NewStage("s", NewStep("charge", func(idempotencyWork) error {
		if calls.Add(1) == 1 {
			return boom
		}
		return nil
	}).WithIdempotencyGuard(guard)))
	_, firstReport, err := p.RunWithReport(context.Background(), idempotencyWork{ID: "retryable"})
	if !errors.Is(err, boom) || firstReport.Stages[0].Steps[0].Idempotency.Outcome != IdempotencyFailedReleasable {
		t.Fatalf("report=%+v err=%v", firstReport, err)
	}
	_, _, err = p.RunWithReport(context.Background(), idempotencyWork{ID: "retryable"})
	if err != nil || calls.Load() != 2 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}

func TestIdempotencyClaimWrapsRetries(t *testing.T) {
	guard := guardForWork(t, NewMemoryIdempotencyStore())
	var calls atomic.Int32
	p := NewPipeline("p", NewStage("s", NewStep("charge", func(idempotencyWork) error {
		if calls.Add(1) < 3 {
			return errors.New("temporary")
		}
		return nil
	}, WithRetry(RetryPolicy{MaxAttempts: 3})).WithIdempotencyGuard(guard)))
	_, _, err := p.RunWithReport(context.Background(), idempotencyWork{ID: "one-claim"})
	if err != nil || calls.Load() != 3 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
	_, _, err = p.RunWithReport(context.Background(), idempotencyWork{ID: "one-claim"})
	if err != nil || calls.Load() != 3 {
		t.Fatalf("duplicate calls=%d err=%v", calls.Load(), err)
	}
}

func TestIdempotencyDifferentKeysExecuteIndependently(t *testing.T) {
	guard := guardForWork(t, NewMemoryIdempotencyStore())
	var calls atomic.Int32
	p := NewPipeline("p", NewStage("s", NewStep("charge", func(idempotencyWork) error { calls.Add(1); return nil }).WithIdempotencyGuard(guard)))
	for _, id := range []string{"a", "b", "a"} {
		_, _, err := p.RunWithReport(context.Background(), idempotencyWork{ID: id})
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestIdempotencyRejectsValueProducingStep(t *testing.T) {
	guard := guardForWork(t, NewMemoryIdempotencyStore())
	p := NewPipeline("p", NewStage("s", NewStep("transform", func(work idempotencyWork) (int, error) { return work.Value, nil }).WithIdempotencyGuard(guard)))
	if err := p.ValidateInput(idempotencyWork{}); err == nil {
		t.Fatal("expected pass-through validation error")
	}
}

func TestIdempotencyKeyErrorsAndPanicsUseNormalErrorPolicy(t *testing.T) {
	keyErr := errors.New("no key")
	guard, _ := NewIdempotencyGuard("g", NewMemoryIdempotencyStore(), func(idempotencyWork) (string, error) { return "", keyErr })
	p := NewPipeline("p", NewStage("s", NewStep("work", func(idempotencyWork) error { return nil }).WithIdempotencyGuard(guard)))
	_, _, err := p.RunWithReport(context.Background(), idempotencyWork{})
	if !errors.Is(err, keyErr) {
		t.Fatalf("err=%v", err)
	}
	guard, _ = NewIdempotencyGuard("g", NewMemoryIdempotencyStore(), func(idempotencyWork) string { panic("key panic") })
	p = NewPipeline("p", NewStage("s", NewStep("work", func(idempotencyWork) error { return nil }).WithIdempotencyGuard(guard)))
	_, _, err = p.RunWithReport(context.Background(), idempotencyWork{})
	var panicErr *PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("err=%v", err)
	}
}

func TestIdempotencyValidation(t *testing.T) {
	if _, err := NewIdempotencyGuard("", NewMemoryIdempotencyStore(), func() string { return "key" }); err == nil {
		t.Fatal("expected name error")
	}
	if _, err := NewIdempotencyGuard("g", nil, func() string { return "key" }); err == nil {
		t.Fatal("expected store error")
	}
	if _, err := NewIdempotencyGuard("g", NewMemoryIdempotencyStore(), func(int) int { return 1 }); err == nil {
		t.Fatal("expected extractor error")
	}
	p := NewPipeline("p", NewStage("s", NewStep("work", func() error { return nil }).WithIdempotencyGuard(nil)))
	if err := p.Validate(); err == nil {
		t.Fatal("expected nil guard error")
	}
}

type completionFailStore struct {
	IdempotencyStore
	err error
}

func (s completionFailStore) Complete(context.Context, string) error { return s.err }

func TestIdempotencyStoreCompletionFailureIsVisible(t *testing.T) {
	storeErr := errors.New("commit unavailable")
	guard := guardForWork(t, completionFailStore{IdempotencyStore: NewMemoryIdempotencyStore(), err: storeErr})
	p := NewPipeline("p", NewStage("s", NewStep("charge", func(idempotencyWork) error { return nil }).WithIdempotencyGuard(guard)))
	_, report, err := p.RunWithReport(context.Background(), idempotencyWork{ID: "ambiguous"})
	if !errors.Is(err, storeErr) {
		t.Fatalf("err=%v", err)
	}
	idempotency := report.Stages[0].Steps[0].Idempotency
	if idempotency == nil || idempotency.Outcome != IdempotencyStoreFailure || !errors.Is(idempotency.Error, storeErr) {
		t.Fatalf("report=%+v", idempotency)
	}
}

type blockingFinalizationStore struct {
	IdempotencyStore
	completeStarted chan struct{}
	releaseStarted  chan struct{}
	unblock         chan struct{}
	completeOnce    sync.Once
	releaseOnce     sync.Once
}

func (s *blockingFinalizationStore) Complete(context.Context, string) error {
	s.completeOnce.Do(func() { close(s.completeStarted) })
	<-s.unblock
	return nil
}

func (s *blockingFinalizationStore) Release(context.Context, string) error {
	s.releaseOnce.Do(func() { close(s.releaseStarted) })
	<-s.unblock
	return nil
}

func newBlockingFinalizationStore() *blockingFinalizationStore {
	return &blockingFinalizationStore{
		IdempotencyStore: NewMemoryIdempotencyStore(),
		completeStarted:  make(chan struct{}),
		releaseStarted:   make(chan struct{}),
		unblock:          make(chan struct{}),
	}
}

func TestIdempotencyCompleteCannotWedgeExecution(t *testing.T) {
	store := newBlockingFinalizationStore()
	defer close(store.unblock)
	guard := guardForWork(t, store).WithFinalizationTimeout(10 * time.Millisecond)
	p := NewPipeline("p", NewStage("s", NewStep("charge", func(idempotencyWork) error { return nil }).WithIdempotencyGuard(guard)))
	started := time.Now()
	_, report, err := p.RunWithReport(context.Background(), idempotencyWork{ID: "complete"})
	if time.Since(started) > time.Second || !errors.Is(err, ErrIdempotencyFinalizationTimeout) {
		t.Fatalf("elapsed=%s err=%v", time.Since(started), err)
	}
	select {
	case <-store.completeStarted:
	default:
		t.Fatal("Complete was not called")
	}
	got := report.Stages[0].Steps[0].Idempotency
	if got == nil || got.Outcome != IdempotencyStoreFailure || !errors.Is(got.Error, ErrIdempotencyFinalizationTimeout) {
		t.Fatalf("idempotency report = %#v", got)
	}
}

func TestIdempotencyReleaseCannotWedgeExecution(t *testing.T) {
	store := newBlockingFinalizationStore()
	defer close(store.unblock)
	guard := guardForWork(t, store).WithFinalizationTimeout(10 * time.Millisecond)
	workErr := errors.New("charge failed")
	p := NewPipeline("p", NewStage("s", NewStep("charge", func(idempotencyWork) error { return workErr }).WithIdempotencyGuard(guard)))
	_, report, err := p.RunWithReport(context.Background(), idempotencyWork{ID: "release"})
	if !errors.Is(err, workErr) || !errors.Is(err, ErrIdempotencyFinalizationTimeout) {
		t.Fatalf("err=%v", err)
	}
	select {
	case <-store.releaseStarted:
	default:
		t.Fatal("Release was not called")
	}
	if got := report.Stages[0].Steps[0].Idempotency; got == nil || got.Outcome != IdempotencyStoreFailure {
		t.Fatalf("idempotency report = %#v", got)
	}
}
