package story

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockingStore holds the write lock open on the first write transaction that
// starts after it is armed, the way a single-lock KV backend does for the
// duration of a long Apply. A Draft lookup that touched the store during that
// window would block, so the test detects it as a timeout rather than passing
// quietly.
type blockingStore struct {
	*MemStore
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (s *blockingStore) Update(fn func(tx Tx) error) error {
	return s.MemStore.Update(func(tx Tx) error {
		if s.armed.CompareAndSwap(true, false) {
			close(s.entered)
			<-s.release
		}
		return fn(tx)
	})
}

func TestProvisionalStory(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	newTr := func(t *testing.T) *Tracker[string] {
		t.Helper()
		tr, err := NewTracker[string](Config[string]{
			Store:         NewMemStore(),
			Codec:         JSONCodec[string]{},
			BatchInterval: time.Hour,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })
		tr.calibMu.Lock()
		tr.sigmaGlobal = 0.5
		tr.calibMu.Unlock()
		return tr
	}

	t.Run("without_a_snapshot_there_is_no_answer", func(t *testing.T) {
		tr := newTr(t)
		assert.Equal(t, uuid.Nil, tr.provisionalStory([]float32{1, 0}, now))
	})

	t.Run("returns_the_nearest_story_within_threshold", func(t *testing.T) {
		tr := newTr(t)
		near, far := uuid.New(), uuid.New()
		tr.publishDraftSnapshot(map[uuid.UUID]storyRecord{
			near: {State: StoryStateActive, Centroid: []float32{1, 0}, LastSignalAt: now},
			far:  {State: StoryStateActive, Centroid: []float32{0, 1}, LastSignalAt: now},
		})

		assert.Equal(t, near, tr.provisionalStory([]float32{1, 0.05}, now))
	})

	t.Run("distant_signal_gets_no_story", func(t *testing.T) {
		tr := newTr(t)
		id := uuid.New()
		tr.publishDraftSnapshot(map[uuid.UUID]storyRecord{
			id: {
				State: StoryStateActive, Centroid: []float32{1, 0}, LastSignalAt: now,
				MeanDistance: 0.01, Sigma: 0.001, SignalCount: 20,
			},
		})

		assert.Equal(t, uuid.Nil, tr.provisionalStory([]float32{0, 1}, now))
	})

	t.Run("archived_and_centroidless_stories_are_skipped", func(t *testing.T) {
		tr := newTr(t)
		archived, empty := uuid.New(), uuid.New()
		tr.publishDraftSnapshot(map[uuid.UUID]storyRecord{
			archived: {State: StoryStateArchived, Centroid: []float32{1, 0}, LastSignalAt: now},
			empty:    {State: StoryStateActive, LastSignalAt: now},
		})

		assert.Equal(t, uuid.Nil, tr.provisionalStory([]float32{1, 0}, now))
	})

	t.Run("stories_outside_the_active_context_window_are_skipped", func(t *testing.T) {
		tr := newTr(t)
		id := uuid.New()
		stale := now.Add(-tr.cfg.ActiveContextWindow - time.Hour)
		tr.publishDraftSnapshot(map[uuid.UUID]storyRecord{
			id: {State: StoryStateActive, Centroid: []float32{1, 0}, LastSignalAt: stale},
		})

		assert.Equal(t, uuid.Nil, tr.provisionalStory([]float32{1, 0}, now))
	})

	t.Run("clearing_the_snapshot_removes_the_answer", func(t *testing.T) {
		tr := newTr(t)
		id := uuid.New()
		tr.publishDraftSnapshot(map[uuid.UUID]storyRecord{
			id: {State: StoryStateActive, Centroid: []float32{1, 0}, LastSignalAt: now},
		})
		require.Equal(t, id, tr.provisionalStory([]float32{1, 0}, now))

		tr.clearDraftSnapshot()
		assert.Equal(t, uuid.Nil, tr.provisionalStory([]float32{1, 0}, now))
	})
}

func TestIngestDuringApplyReturnsProvisionalStory(t *testing.T) {
	store := &blockingStore{
		MemStore: NewMemStore(),
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	tr, err := NewTracker[string](Config[string]{
		Store:         store,
		Codec:         JSONCodec[string]{},
		BatchInterval: time.Hour,
		MinStorySize:  3,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = tr.Close() })

	// Seed a story directly so the batch has something to snapshot, then let
	// the batch reach its Apply transaction and stall there.
	now := time.Now()
	storyID := uuid.New()
	seedStory(t, tr, storyID, storyRecord{
		State: StoryStateActive, Centroid: []float32{1, 0, 0},
		CreatedAt: now.Add(-time.Hour), LastSignalAt: now.Add(-time.Minute),
	})
	// Arm the store so the batch's Apply transaction — the next write — stalls
	// while holding the write lock.
	store.armed.Store(true)
	batchDone := make(chan struct{})
	go func() {
		defer close(batchDone)
		tr.runBatch()
	}()
	<-store.entered
	require.True(t, tr.applyInProgress.Load(), "Apply must have redirected the ingest path")

	// This Ingest must not touch the store — doing so would block behind the
	// stalled Apply and the deadline below would fire.
	ingested := make(chan uuid.UUID, 1)
	go func() {
		id, err := tr.Ingest(context.Background(), Signal[string]{
			ID: uuid.New(), At: time.Now(), Embedding: []float32{1, 0.02, 0},
		})
		require.NoError(t, err)
		ingested <- id
	}()

	select {
	case got := <-ingested:
		assert.Equal(t, storyID, got, "a buffered signal still gets a provisional story ID")
	case <-time.After(2 * time.Second):
		t.Fatal("Ingest blocked during Apply: the Draft lookup must not touch the store")
	}

	close(store.release)
	<-batchDone
}
