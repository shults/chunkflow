// Package step decorates the callbacks handed to MapCtx, FilterCtx, ReduceCtx and friends.
// A decorator sees the individual call, so it can repeat it, bound it in time or reinterpret
// its error; a policy on the stream (see package policy) only sees outcomes. The two compose,
// and the composition is the point:
//
//	users.MapCtx(step.Decorate(fetch, step.Retry(3, step.RetryWithBackoff(newBackoff)), step.Timeout(2*time.Second)),
//	    chunkflow.WithParallel(8)).
//	    Through(policy.CircuitBreaker[User](5))
//
// is three paced attempts of at most two seconds each per element, then a breaker over five
// failed elements in a row.
//
// # Shape
//
// A Middleware wraps one call and sees only its context and its error; the values travel in a
// closure it never touches. That is why Retry, Timeout and Tolerate need no type argument and
// why one set of them serves every callback shape: Decorate applies them to
// func(ctx, T) (R, error), Decorate3 to a fold callback func(ctx, Acc, T) (Acc, error). The
// element and result types are inferred from the callback.
//
// In a Decorate call the first middleware is the outermost and runs first, as in an HTTP
// middleware chain: Decorate(fn, Retry(3), Timeout(d)) gives every attempt its own
// deadline, Decorate(fn, Timeout(d), Retry(3)) gives all attempts one deadline together.
//
// # Skipping an element
//
// A middleware that returns an error marked by chunkflow.Suppress (Tolerate does) says "skip
// this element", and the decorator knows what that means for its shape: Decorate returns the
// marked error, so the terminal skips the element and WithOnError and Seq still see it;
// Decorate3 returns the accumulator it was given and no error, so the fold carries on. Retry
// never repeats a suppressed error: someone has decided.
//
// A retried fold callback receives the same accumulator on every attempt. For a value type
// that is a clean restart; a reference type (map, slice) may already carry what the failed
// attempt did to it, so mutate the accumulator only after the fallible work has succeeded.
// A panic in the callback is nobody's business here: the stage above recovers it.
package step

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shults/chunkflow"
)

// Middleware wraps one callback invocation. It receives the context of the call and call,
// which runs the next layer (ultimately the callback) with the context it is given and
// returns that layer's error. A middleware may call it once, several times, with a derived
// context, or not at all, and returns the error it wants the caller to see. Values never
// pass through here, which is what makes a Middleware independent of the callback's shape.
type Middleware func(ctx context.Context, call func(context.Context) error) error

// Decorate wraps fn, a callback of the MapCtx / FilterCtx / TapCtx shape, in mws. The first
// middleware is the outermost. A suppressed error coming out of the chain is returned as is
// with a zero result, so the terminal skips the element.
func Decorate[T, R any](fn func(context.Context, T) (R, error), mws ...Middleware) func(context.Context, T) (R, error) {
	run := chain(mws)
	return func(ctx context.Context, item T) (R, error) {
		var res R
		err := run(ctx, func(ctx context.Context) error {
			var err error
			res, err = fn(ctx, item)
			return err
		})
		if err != nil {
			var zero R
			return zero, err
		}
		return res, nil
	}
}

// Decorate3 wraps fn, a fold callback of the ReduceCtx / ReduceByCtx shape, in mws. Every
// attempt receives the accumulator the call was given. A suppressed error coming out of the
// chain skips the element: the accumulator is returned unchanged and no error, so the fold
// carries on. Any other error is returned with the unchanged accumulator.
func Decorate3[T, Acc any](fn func(context.Context, Acc, T) (Acc, error), mws ...Middleware) func(context.Context, Acc, T) (Acc, error) {
	run := chain(mws)
	return func(ctx context.Context, acc Acc, item T) (Acc, error) {
		var next Acc
		err := run(ctx, func(ctx context.Context) error {
			var err error
			next, err = fn(ctx, acc, item)
			return err
		})
		switch {
		case err == nil:
			return next, nil
		case errors.Is(err, chunkflow.ErrSuppressed):
			return acc, nil
		default:
			return acc, err
		}
	}
}

// Backoff is the pacing strategy of Retry, with the method set the Go ecosystem has settled
// on: it is the interface of github.com/cenkalti/backoff, so its strategies, or anyone else's
// with these two methods, plug in unchanged. This package ships none of its own; pacing is a
// solved problem elsewhere. NextBackOff is asked after every failed
// attempt that Retry is still allowed to repeat and returns the pause before the next one;
// Stop, or any negative duration, ends the series early, before the attempt limit, which is
// how a time budget is expressed. A Backoff is stateful: NextBackOff advances it and Reset
// starts over. One instance serves one series of attempts, so RetryWithBackoff takes a
// constructor and Retry creates and resets a fresh instance per call, which keeps a parallel
// step free of shared state.
type Backoff interface {
	NextBackOff() time.Duration
	Reset()
}

// Stop is the NextBackOff result that ends a series of attempts. It has the same value as
// cenkalti/backoff.Stop; Retry treats every negative duration the same way.
const Stop time.Duration = -1

// RetryOption configures Retry.
type RetryOption func(*retryConfig)

type retryConfig struct {
	newBackoff func() Backoff
	err        error
}

// zeroBackoff is the default pacing: retry at once, never stop on its own. Having it as a
// real Backoff keeps the retry loop free of nil checks.
type zeroBackoff struct{}

// NextBackOff implements Backoff: no pause.
func (zeroBackoff) NextBackOff() time.Duration { return 0 }

// Reset implements Backoff; there is no state.
func (zeroBackoff) Reset() {}

// RetryWithBackoff paces the attempts with a fresh Backoff per series, obtained from
// newBackoff and reset before the first attempt. Wrap any constructor whose result has the
// Backoff method set:
//
//	step.RetryWithBackoff(func() step.Backoff { return backoff.NewExponentialBackOff() })
//
// Without this option Retry repeats immediately. A nil constructor makes every call fail with
// a configuration error.
func RetryWithBackoff(newBackoff func() Backoff) RetryOption {
	return func(c *retryConfig) {
		if newBackoff == nil {
			c.err = errors.New("step: RetryWithBackoff: nil constructor")
			return
		}
		c.newBackoff = newBackoff
	}
}

// Retry repeats a failed call until it succeeds or attempts calls have been made in total,
// immediately or paced by RetryWithBackoff, and returns the first success or the error of
// the last attempt, unchanged. It stops early, without retrying, when the caller's context
// is done (the context error is returned with the last failure in the chain, and a pause is
// cut short by cancellation), when the error is marked by chunkflow.Suppress, or when the
// Backoff returns Stop. A deadline set by an inner Timeout is not the caller's context, so a
// timed-out attempt is retried. attempts below 1 makes every call fail with a configuration
// error.
func Retry(attempts int, opts ...RetryOption) Middleware {
	if attempts < 1 {
		return failing(fmt.Errorf("step: Retry(%d): attempts must be at least 1", attempts))
	}
	cfg := retryConfig{newBackoff: func() Backoff { return zeroBackoff{} }}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.err != nil {
		return failing(cfg.err)
	}
	return func(ctx context.Context, call func(context.Context) error) error {
		backoff := cfg.newBackoff()
		backoff.Reset()
		for attempt := 1; ; attempt++ {
			err := call(ctx)
			if err == nil || attempt == attempts || errors.Is(err, chunkflow.ErrSuppressed) {
				return err
			}
			if cerr := ctx.Err(); cerr != nil {
				return fmt.Errorf("%w (last attempt: %w)", cerr, err)
			}
			pause := backoff.NextBackOff()
			if pause < 0 {
				return err
			}
			if pause > 0 {
				timer := time.NewTimer(pause)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return fmt.Errorf("%w (last attempt: %w)", ctx.Err(), err)
				}
			}
		}
	}
}

// Timeout runs the call with a context that expires after d. The bound is cooperative: the
// callback must honour its context, as any I/O call does, and what comes back is whatever the
// callback returned, typically an error matching context.DeadlineExceeded. That deadline is
// not the caller's context, so an outer Retry treats it as an ordinary failure. A
// non-positive d makes every call fail with a configuration error.
func Timeout(d time.Duration) Middleware {
	if d <= 0 {
		return failing(fmt.Errorf("step: Timeout(%v): duration must be positive", d))
	}
	return func(ctx context.Context, call func(context.Context) error) error {
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return call(ctx)
	}
}

// Tolerate marks an error matching any of targets (in the errors.Is sense) with
// chunkflow.Suppress, which the decorator turns into "skip this element" (see the package
// documentation). It is Suppress for callbacks you do not own. Errors that Suppress refuses,
// context errors and chunkflow.ErrPanic, pass through unchanged, so a context error as a
// target is a no-op. No targets, or a nil among them, makes every call fail with a
// configuration error.
func Tolerate(targets ...error) Middleware {
	if len(targets) == 0 {
		return failing(errors.New("step: Tolerate: no targets"))
	}
	for i, target := range targets {
		if target == nil {
			return failing(fmt.Errorf("step: Tolerate: nil target at index %d", i))
		}
	}
	return func(ctx context.Context, call func(context.Context) error) error {
		err := call(ctx)
		if err == nil {
			return nil
		}
		for _, target := range targets {
			if errors.Is(err, target) {
				return chunkflow.Suppress(err)
			}
		}
		return err
	}
}

// chain composes middlewares so that mws[0] is the outermost layer.
func chain(mws []Middleware) Middleware {
	return func(ctx context.Context, call func(context.Context) error) error {
		next := call
		for i := len(mws) - 1; i >= 0; i-- {
			mw, inner := mws[i], next
			next = func(ctx context.Context) error { return mw(ctx, inner) }
		}
		return next(ctx)
	}
}

// failing is what a misconfigured middleware degrades to: it reports the configuration error
// on every call without running anything, the way an invalid Option ends a stream.
func failing(err error) Middleware {
	return func(context.Context, func(context.Context) error) error { return err }
}
