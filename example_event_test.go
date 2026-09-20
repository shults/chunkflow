package chunkflow_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/shults/chunkflow"
)

// Event is what the application emits.
type Event struct {
	ID      int
	Payload string
}

// DB is the dependency injected into the saver: anything that stores a batch.
type DB interface {
	Insert(ctx context.Context, events []Event) error
}

// BatchEventSaver collects events and writes them to the DB in batches: a full batch goes out
// at once, a partial one after maxWait. Push never waits for the database, only for room in the
// buffer. A failed insert loses that batch and is reported through the error callback; the saver
// itself keeps running. That is the trade-off of this architecture, chosen on purpose here.
type BatchEventSaver struct {
	events  chan Event
	done    chan struct{}
	onError atomic.Pointer[func(error)]
}

func NewBatchEventSaver(ctx context.Context, db DB, size int, maxWait time.Duration) *BatchEventSaver {
	s := &BatchEventSaver{events: make(chan Event, 1024), done: make(chan struct{})}
	go s.run(ctx, db, size, maxWait)
	return s
}

// Push queues one event. Safe for concurrent use.
func (s *BatchEventSaver) Push(e Event) { s.events <- e }

// Stop closes the queue and waits until the last batch has been written.
func (s *BatchEventSaver) Stop() {
	close(s.events)
	<-s.done
}

// SetOnError installs the callback that receives every failed insert (and the reason the saver
// stopped, if it stopped because of the context). May be called at any time.
func (s *BatchEventSaver) SetOnError(fn func(error)) { s.onError.Store(&fn) }

func (s *BatchEventSaver) run(ctx context.Context, db DB, size int, maxWait time.Duration) {
	defer close(s.done)
	report := func(err error) {
		if fn := s.onError.Load(); fn != nil {
			(*fn)(err)
		}
	}
	insert := func(ctx context.Context, batch []Event) error {
		return chunkflow.Suppress(db.Insert(ctx, batch)) // a lost batch is reported, not fatal
	}
	err := chunkflow.New(ctx).Chan(s.events).
		Opts(chunkflow.WithOnError(report)).
		ChunkTimeout[[]Event](size, maxWait).
		TapCtx(insert).
		Drain()
	if err != nil {
		report(err) // only the context can end this pipeline early
	}
}

// fakeDB prints what it stores and refuses one batch.
type fakeDB struct{}

func (fakeDB) Insert(_ context.Context, batch []Event) error {
	ids := make([]int, 0, len(batch))
	for _, e := range batch {
		ids = append(ids, e.ID)
	}
	if ids[0] == 4 {
		return fmt.Errorf("insert %v: db down", ids)
	}
	fmt.Println("insert", ids)
	return nil
}

func Example_batchEventSaver() {
	ctx := context.Background()

	saver := NewBatchEventSaver(ctx, fakeDB{}, 100, 30*time.Millisecond)
	saver.SetOnError(func(err error) { fmt.Println("lost:", err) })

	// A burst of three events, a pause longer than maxWait, two more, then Stop.
	for _, id := range []int{1, 2, 3} {
		saver.Push(Event{ID: id, Payload: "..."})
	}
	time.Sleep(300 * time.Millisecond)
	for _, id := range []int{4, 5} {
		saver.Push(Event{ID: id, Payload: "..."})
	}
	saver.Stop()
	fmt.Println("stopped")
	// Output:
	// insert [1 2 3]
	// lost: suppressed: insert [4 5]: db down
	// stopped
}
