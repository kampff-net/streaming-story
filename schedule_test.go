package story

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTracker_BatchSchedule_Execution(t *testing.T) {
	t.Run("invalid_cron_expression_fails_construction", func(t *testing.T) {
		_, err := NewTracker[string](Config[string]{
			Store:         newMemStore(),
			BatchSchedule: "invalid cron expr",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "BatchSchedule is invalid")
	})

	t.Run("fires_batch_on_cron_schedule", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{
			Store:         newMemStore(),
			BatchSchedule: "@every 100ms",
			OnBatchError: func(err error) {
				t.Errorf("unexpected batch error: %v", err)
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })

		// Ingest a signal
		_, err = tr.Ingest(context.Background(), Signal[string]{
			ID:         uuid.New(),
			At:         time.Now(),
			Embeddings: []Embedding{[]float32{1, 0, 0}},
			Data:       "item",
		})
		require.NoError(t, err)

		// Wait for the cron engine to fire at least once
		require.Eventually(t, func() bool {
			tr.calibMu.RLock()
			last := tr.lastBatch
			tr.calibMu.RUnlock()
			return !last.IsZero()
		}, 2*time.Second, 50*time.Millisecond, "cron should trigger batch pass within window")
	})

	t.Run("close_drains_running_batch", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{
			Store:         newMemStore(),
			BatchSchedule: "@every 100ms",
		})
		require.NoError(t, err)

		// Ingest several signals
		for i := 0; i < 5; i++ {
			_, err = tr.Ingest(context.Background(), Signal[string]{
				ID:         uuid.New(),
				At:         time.Now(),
				Embeddings: []Embedding{[]float32{float32(i) * 0.1, 1, 0}},
				Data:       "item",
			})
			require.NoError(t, err)
		}

		closedCh := make(chan error, 1)
		go func() {
			closedCh <- tr.Close()
		}()

		select {
		case err := <-closedCh:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("Close() timed out waiting for cron drain")
		}
	})
}
