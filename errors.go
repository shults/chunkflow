package chunkflow

import (
	"errors"
	"fmt"
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
