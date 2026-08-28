package story

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errWriteStore fails every write transaction, so the apply phase of a batch
// run cannot commit.
type errWriteStore struct {
	Store
	err error
}

func (s *errWriteStore) Update(func(tx Tx) error) error { return s.err }

func TestOnBatchError(t *testing.T) {
	t.Run("reports_a_failed_apply", func(t *testing.T) {
		want := errors.New("store is read-only")

		var mu sync.Mutex
		var got []error
		tr, err := NewTracker[string](Config[string]{
			Store:         &errWriteStore{Store: NewMemStore(), err: want},
			BatchSchedule: "@every 1h",
			OnBatchError: func(err error) {
				mu.Lock()
				defer mu.Unlock()
				got = append(got, err)
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })

		tr.runBatch()

		mu.Lock()
		defer mu.Unlock()
		require.NotEmpty(t, got, "a failed batch must not fail silently")
		assert.ErrorIs(t, got[0], want)
	})

	t.Run("absent_callback_is_not_fatal", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{
			Store:         &errWriteStore{Store: NewMemStore(), err: errors.New("boom")},
			BatchSchedule: "@every 1h",
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })

		assert.NotPanics(t, tr.runBatch)
	})
}

func TestIngestDoesNotOutliveClose(t *testing.T) {
	// Close must wait for in-flight Ingest calls before closing the store,
	// so a concurrent Ingest either completes or reports the closed tracker.
	tr, err := NewTracker[string](Config[string]{
		Store:         NewMemStore(),
		BatchSchedule: "@every 1h",
	})
	require.NoError(t, err)

	const writers = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = tr.Ingest(context.Background(), Signal[string]{
				ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{[]float32{1, 0, 0}},
			})
		}()
	}

	close(start)
	assert.NoError(t, tr.Close())
	wg.Wait()
}
