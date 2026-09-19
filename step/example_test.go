package step_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/seq"
	"github.com/shults/chunkflow/step"
)

func ExampleDecorate() {
	ctx := context.Background()
	errFlaky := errors.New("flaky")

	attempts := 0
	fetch := func(_ context.Context, id int) (string, error) {
		attempts++
		if attempts%3 != 0 { // succeeds on every third call
			return "", errFlaky
		}
		return fmt.Sprintf("user-%d", id), nil
	}

	// Three attempts per element, each bounded to a second: Retry is outermost, so every
	// attempt gets its own Timeout.
	users, err := chunkflow.New(ctx).Seq(seq.Items(1, 2)).
		MapCtx(step.Decorate(fetch, step.Retry(3), step.Timeout(time.Second))).
		Collect()
	fmt.Println(users, err, attempts)
	// Output: [user-1 user-2] <nil> 6
}

func ExampleDecorate3() {
	ctx := context.Background()
	errNotFound := errors.New("not found")

	// A fold with I/O inside: count the rows that were actually stored.
	store := func(_ context.Context, stored, id int) (int, error) {
		if id == 2 {
			return stored, errNotFound // nothing to store for this one
		}
		return stored + 1, nil
	}

	// The same Tolerate as for a map callback; in a fold "skip the element" means
	// "keep the accumulator and carry on".
	n, err := chunkflow.New(ctx).Seq(seq.Items(1, 2, 3)).
		ReduceCtx(0, step.Decorate3(store, step.Tolerate(errNotFound)))
	fmt.Println(n, err)
	// Output: 2 <nil>
}

// budget is a Backoff of one's own: no pause, but Stop once the series has run for longer
// than allowed. Retry resets a fresh instance per series, so the clock starts with the
// first attempt of each element.
type budget struct {
	limit    time.Duration
	deadline time.Time
}

func (b *budget) Reset() { b.deadline = time.Now().Add(b.limit) }

func (b *budget) NextBackOff() time.Duration {
	if time.Now().After(b.deadline) {
		return step.Stop
	}
	return 0
}

func ExampleRetry() {
	ctx := context.Background()
	errDown := errors.New("down")

	calls := 0
	fetch := func(context.Context, int) (int, error) {
		calls++
		time.Sleep(5 * time.Millisecond)
		return 0, errDown
	}

	// Up to 100 attempts, but no more than about 20ms per element.
	newBudget := func() step.Backoff { return &budget{limit: 20 * time.Millisecond} }
	_, err := step.Decorate(fetch, step.Retry(100, step.RetryWithBackoff(newBudget)))(ctx, 1)
	fmt.Println(err, calls < 100)
	// Output: down true
}
