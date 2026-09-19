package chunkflow

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
)

// ErrEmpty is returned by First and Last when the stream produced no value. It is a
// plain sentinel, not a wrapped element error: nothing failed, there was nothing to
// return. It is never passed to the WithOnError hook.
var ErrEmpty = errors.New("empty stream")

// ErrSuppressed marks an error that something decided to tolerate: a CircuitBreaker
// below its threshold, or a callback that returned Suppress(err). Terminal operations
// skip elements carrying such an error instead of failing. Use errors.Is(err, ErrSuppressed)
// to detect one, for example when iterating Stream.Seq(); the original error stays in the
// chain, so errors.Is(err, original) keeps working too.
var ErrSuppressed = errors.New("suppressed")

// Suppress marks err as tolerated. Return it from a MapCtx, FilterCtx or TapCtx callback
// when the failure of this one element is not a reason to stop the pipeline: the element
// is replaced by the marked error, terminal operations skip it, WithOnError and Seq still
// see it with err in the chain, and a downstream CircuitBreaker does not count it.
//
// Suppress(nil) is nil, so `return v, chunkflow.Suppress(err)` needs no branch. An error
// that is already suppressed is returned as is. Context errors and ErrPanic cannot be
// suppressed, exactly as with CircuitBreaker; they come back unchanged and stay fatal.
func Suppress(err error) error {
	if err == nil || isSuppressed(err) || neverSuppressed(err) {
		return err
	}
	return &suppressedError{err: err}
}

// suppressedError wraps a tolerated error. It matches both ErrSuppressed and the original
// error when inspected with errors.Is. A CircuitBreaker records how far the error got it
// towards tripping; an error from Suppress has no threshold.
type suppressedError struct {
	err                 error
	consecutiveFailures int
	threshold           int
}

func (e *suppressedError) Error() string {
	if e.threshold == 0 {
		return fmt.Sprintf("%v: %v", ErrSuppressed, e.err)
	}
	return fmt.Sprintf("%v by circuit breaker (failure %d/%d): %v", ErrSuppressed, e.consecutiveFailures, e.threshold, e.err)
}

// Unwrap exposes both the original error and the ErrSuppressed sentinel.
func (e *suppressedError) Unwrap() []error {
	return []error{e.err, ErrSuppressed}
}

// isSuppressed reports whether err is marked as tolerated.
func isSuppressed(err error) bool {
	return errors.Is(err, ErrSuppressed)
}

// neverSuppressed reports whether err belongs to the class no policy may tolerate:
// a recovered panic (a bug) or a context error (the pipeline was told to stop).
func neverSuppressed(err error) bool {
	return isPanic(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// ErrPanic marks an error produced from a panic inside a user callback of an Stream
// (MapCtx, FilterCtx, ForEachCtx, ...). The panic is recovered where it happens, also on
// worker goroutines, and travels down the pipeline as an error element that nothing may
// suppress: CircuitBreaker passes it through and every terminal returns it. The message
// carries the panic value and the stack of the goroutine that panicked; if the value is an
// error it is also in the chain, so errors.Is(err, original) keeps working.
var ErrPanic = errors.New("panic in callback")

// panicError is the recovered panic. Private: the value and stack are exposed only
// through Error() and Unwrap().
type panicError struct {
	value any
	stack []byte
}

func (e *panicError) Error() string {
	return fmt.Sprintf("%v: %v\n%s", ErrPanic, e.value, e.stack)
}

func (e *panicError) Unwrap() []error {
	if err, ok := e.value.(error); ok {
		return []error{ErrPanic, err}
	}
	return []error{ErrPanic}
}

// isPanic reports whether err originates from a recovered callback panic.
func isPanic(err error) bool {
	return errors.Is(err, ErrPanic)
}

// guardPanics turns a panic raised inside a user callback into an ErrPanic error. It is
// meant to be deferred once per stage iteration (or once per worker goroutine), not once
// per element, so the per-element cost is a single bool store. The caller sets *inCallback
// to true right before invoking the user callback and back to false right after; a panic
// caught while it is false did not come from the callback (typically the downstream
// consumer panicked inside yield) and is re-raised untouched.
func guardPanics(inCallback *bool, report func(error)) {
	v := recover()
	if v == nil {
		return
	}
	if !*inCallback {
		panic(v)
	}
	report(&panicError{value: v, stack: debug.Stack()})
}
