package policy_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/shults/chunkflow"
	"github.com/shults/chunkflow/policy"
	"github.com/shults/chunkflow/seq"
)

func ExampleCircuitBreaker() {
	ctx := context.Background()
	errFlaky := errors.New("flaky")

	fetch := func(_ context.Context, i int) (int, error) {
		if i == 3 || i == 4 {
			return 0, errFlaky
		}
		return i * 10, nil
	}

	// Up to 2 consecutive failures are tolerated; the 3rd in a row would trip the breaker.
	got, err := chunkflow.New(ctx).Seq(seq.Range(1, 7)).
		MapCtx(fetch).
		Through(policy.CircuitBreaker[int](3)).
		Collect()

	fmt.Println(got, err)
	// Output: [10 20 50 60] <nil>
}

func ExampleCircuitBreaker_tripped() {
	ctx := context.Background()
	errDown := errors.New("service down")

	fetch := func(_ context.Context, i int) (int, error) {
		if i >= 3 {
			return 0, errDown
		}
		return i, nil
	}

	got, err := chunkflow.New(ctx).Seq(seq.Numbers(1)). // infinite source
								MapCtx(fetch).
								Through(policy.CircuitBreaker[int](3)).
								Collect()

	fmt.Println(got)
	fmt.Println(err)
	fmt.Println(errors.Is(err, errDown))
	// Output:
	// [1 2]
	// circuit breaker tripped after 3 consecutive errors: service down
	// true
}
