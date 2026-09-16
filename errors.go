package chunkflow

import (
	"errors"
	"fmt"
	"runtime/debug"
)

// ErrSuppressed marks an error that a CircuitBreaker decided to tolerate.
// Terminal operations skip elements carrying such an error instead of failing.
// Use errors.Is(err, ErrSuppressed) to detect one, for example when iterating IoStream.Seq();
// the original error stays in the chain, so errors.Is(err, original) keeps working too.
var ErrSuppressed = errors.New("suppressed by circuit breaker")

// suppressedError wraps an error tolerated by CircuitBreaker. It matches both
// ErrSuppressed and the original error when inspected with errors.Is.
type suppressedError struct {
	err                 error
	consecutiveFailures int
	threshold           int
}

func (e *suppressedError) Error() string {
	return fmt.Sprintf("%v (failure %d/%d): %v", ErrSuppressed, e.consecutiveFailures, e.threshold, e.err)
}

// Unwrap exposes both the original error and the ErrSuppressed sentinel.
func (e *suppressedError) Unwrap() []error {
	return []error{e.err, ErrSuppressed}
}

// isSuppressed reports whether err was tolerated by a CircuitBreaker.
func isSuppressed(err error) bool {
	return errors.Is(err, ErrSuppressed)
}

// ErrPanic marks an error produced from a panic inside a user callback of an IoStream
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

// recovered calls fn and turns a panic into an ErrPanic error, capturing the stack of the
// current goroutine at the point of the panic.
func recovered[R any](fn func() (R, error)) (res R, err error) {
	defer func() {
		if v := recover(); v != nil {
			var zero R
			res, err = zero, &panicError{value: v, stack: debug.Stack()}
		}
	}()
	return fn()
}
