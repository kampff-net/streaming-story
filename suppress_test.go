package story

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTracker_Suppress(t *testing.T) {
	t.Run("active_story_transitions_and_emits", func(t *testing.T) {
		tr := newTestTracker(t)
		id := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tr.writeStoryMeta(tx, id, time.Time{}, storyRecord{
				State: StoryStateActive, CreatedAt: time.Now(),
			})
		}))
		ch := tr.Subscribe()

		require.NoError(t, tr.Suppress(id, "spam channel"))

		meta, err := tr.Story(id)
		require.NoError(t, err)
		assert.Equal(t, StoryStateSuppressed, meta.State)
		assert.True(t, meta.WasSuppressed)
		assert.Equal(t, "spam channel", meta.SuppressionReason)

		select {
		case ev := <-ch:
			assert.Equal(t, EventStorySuppressed, ev.Kind)
			assert.Equal(t, id, ev.StoryID)
		case <-time.After(time.Second):
			t.Fatal("EventStorySuppressed not received")
		}
	})

	t.Run("already_suppressed_updates_reason_without_reemitting", func(t *testing.T) {
		tr := newTestTracker(t)
		id := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tr.writeStoryMeta(tx, id, time.Time{}, storyRecord{
				State: StoryStateSuppressed, WasSuppressed: true,
				SuppressionReason: "first reason", CreatedAt: time.Now(),
			})
		}))
		ch := tr.Subscribe()

		require.NoError(t, tr.Suppress(id, "second reason"))

		meta, err := tr.Story(id)
		require.NoError(t, err)
		assert.Equal(t, "second reason", meta.SuppressionReason)

		select {
		case ev := <-ch:
			t.Fatalf("unexpected event on already-suppressed story: %+v", ev)
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("archived_story_returns_error", func(t *testing.T) {
		tr := newTestTracker(t)
		id := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tr.writeStoryMeta(tx, id, time.Time{}, storyRecord{
				State: StoryStateArchived, CreatedAt: time.Now(),
			})
		}))

		err := tr.Suppress(id, "too late")
		require.Error(t, err)

		meta, merr := tr.Story(id)
		require.NoError(t, merr)
		assert.Equal(t, StoryStateArchived, meta.State, "state must be untouched on rejection")
	})

	t.Run("unknown_id_returns_ErrNotFound", func(t *testing.T) {
		tr := newTestTracker(t)
		err := tr.Suppress(uuid.New(), "reason")
		require.Error(t, err)
	})

	t.Run("patches_in_memory_story_index", func(t *testing.T) {
		tr := newTestTracker(t)
		tr.dim.Store(2)
		id := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tr.writeStoryMeta(tx, id, time.Time{}, storyRecord{
				State: StoryStateActive, Centroid: []float32{1, 0},
				LastSignalAt: time.Now(), CreatedAt: time.Now(),
			})
		}))
		var idx *activeStoryIndex
		require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
			var err error
			idx, err = tr.buildActiveStoryIndex(tx)
			return err
		}))
		tr.storyIndex.Store(idx)

		require.NoError(t, tr.Suppress(id, "spam"))

		for i, sid := range tr.storyIndex.Load().ids {
			if sid == id {
				assert.Equal(t, StoryStateSuppressed, tr.storyIndex.Load().metas[i].state)
				return
			}
		}
		t.Fatal("story not found in index")
	})

	t.Run("returns_error_after_close", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{Store: newMemStore(), BatchInterval: time.Hour})
		require.NoError(t, err)
		require.NoError(t, tr.Close())

		err = tr.Suppress(uuid.New(), "reason")
		require.Error(t, err)
	})
}

func TestTracker_Unsuppress(t *testing.T) {
	t.Run("suppressed_story_transitions_and_emits", func(t *testing.T) {
		tr := newTestTracker(t)
		id := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tr.writeStoryMeta(tx, id, time.Time{}, storyRecord{
				State: StoryStateSuppressed, WasSuppressed: true,
				SuppressionReason: "spam", CreatedAt: time.Now(),
			})
		}))
		ch := tr.Subscribe()

		require.NoError(t, tr.Unsuppress(id))

		meta, err := tr.Story(id)
		require.NoError(t, err)
		assert.Equal(t, StoryStateActive, meta.State)
		assert.True(t, meta.WasSuppressed, "WasSuppressed is historical and must survive Unsuppress")
		assert.Equal(t, "spam", meta.SuppressionReason, "SuppressionReason is retained across Unsuppress")

		select {
		case ev := <-ch:
			assert.Equal(t, EventStoryUnsuppressed, ev.Kind)
			assert.Equal(t, id, ev.StoryID)
		case <-time.After(time.Second):
			t.Fatal("EventStoryUnsuppressed not received")
		}
	})

	t.Run("non_suppressed_story_is_noop", func(t *testing.T) {
		tr := newTestTracker(t)
		id := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tr.writeStoryMeta(tx, id, time.Time{}, storyRecord{
				State: StoryStateActive, CreatedAt: time.Now(),
			})
		}))
		ch := tr.Subscribe()

		require.NoError(t, tr.Unsuppress(id))

		select {
		case ev := <-ch:
			t.Fatalf("unexpected event on non-suppressed story: %+v", ev)
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("unknown_id_returns_ErrNotFound", func(t *testing.T) {
		tr := newTestTracker(t)
		err := tr.Unsuppress(uuid.New())
		require.Error(t, err)
	})

	t.Run("returns_error_after_close", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{Store: newMemStore(), BatchInterval: time.Hour})
		require.NoError(t, err)
		require.NoError(t, tr.Close())

		err = tr.Unsuppress(uuid.New())
		require.Error(t, err)
	})
}

// TestTracker_Ingest_SuppressedStoryStaysSuppressed covers spec 009's core
// design decision: a signal joining a suppressed story is treated as more
// evidence it's noise, not evidence it should reactivate.
func TestTracker_Ingest_SuppressedStoryStaysSuppressed(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	storyID := uuid.NewSHA1(TrackerNamespace, []byte("suppressed-story"))
	seedStory(t, tr, storyID, storyRecord{
		State: StoryStateSuppressed, WasSuppressed: true, SuppressionReason: "spam channel",
		Centroid: []float32{1, 0}, CreatedAt: time.Now().Add(-time.Hour), LastSignalAt: time.Now().Add(-time.Minute),
	})

	ch := tr.Subscribe()
	sig := Signal[string]{ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{[]float32{0.98, 0.02}}}

	assigned, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyID}, assigned, "signal must still join the suppressed story's clustering")

	meta, err := tr.Story(storyID)
	require.NoError(t, err)
	assert.Equal(t, StoryStateSuppressed, meta.State, "state must not auto-reactivate on ingest")

	var kinds []EventKind
	for i := 0; i < 2; i++ {
		select {
		case ev := <-ch:
			assert.Equal(t, storyID, ev.StoryID)
			assert.Equal(t, sig.ID, ev.SignalID)
			kinds = append(kinds, ev.Kind)
		case <-time.After(time.Second):
			t.Fatalf("expected 2 events, got %d", i)
		}
	}
	assert.ElementsMatch(t, []EventKind{EventDraftAssigned, EventSuppressedStorySignal}, kinds)

	select {
	case ev := <-ch:
		t.Fatalf("unexpected third event: %+v", ev)
	default:
	}
}
