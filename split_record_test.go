package story

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kvsh.ch/streaming-story/internal/keys"
)

func TestSplitRecord_ColdStartPopulatesIndex(t *testing.T) {
	store := NewMemStore()

	storyID := uuid.New()
	now := time.Now()

	// Populate the store as if a previous Tracker ran.
	rec := storyRecord{
		Centroid:       []float32{1, 0, 0},
		RecentCentroid: []float32{1, 0, 0},
		Radius:         0.1,
		CreatedAt:      now.Add(-time.Hour),
		LastSignalAt:   now.Add(-10 * time.Minute),
		State:          StoryStateActive,
	}
	hot := storyHot{
		State:        StoryStateActive,
		LastSignalAt: now.Add(-10 * time.Minute),
	}

	require.NoError(t, store.Update(func(tx Tx) error {
		mb, err := cborEncMode.Marshal(rec)
		if err != nil {
			return err
		}
		if err := tx.Put(keys.StoryMeta(storyID), mb); err != nil {
			return err
		}
		hb, err := cborEncMode.Marshal(hot)
		if err != nil {
			return err
		}
		return tx.Put(keys.StoryHot(storyID), hb)
	}))

	// Open a fresh tracker over this store.
	tr, err := NewTracker[string](Config[string]{
		Store:         store,
		BatchSchedule: "@every 1h",
	})
	require.NoError(t, err)
	defer tr.Close()

	// Ingest a signal close to the seeded story.
	sig := Signal[string]{
		ID:         uuid.New(),
		At:         now,
		Embeddings: []Embedding{[]float32{0.99, 0.01, 0}},
	}
	assigned, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyID}, assigned, "cold start must match existing story from loaded index")
}

func TestSplitRecord_IngestNeverMutatesStoryMeta(t *testing.T) {
	store := NewMemStore()

	tr, err := NewTracker[string](Config[string]{
		Store:         store,
		BatchSchedule: "@every 1h",
	})
	require.NoError(t, err)
	defer tr.Close()

	now := time.Now()
	storyID := uuid.New()
	seedStory(t, tr, storyID, storyRecord{
		Centroid:       []float32{0, 1, 0},
		RecentCentroid: []float32{0, 1, 0},
		Radius:         0.15,
		CreatedAt:      now.Add(-time.Hour),
		LastSignalAt:   now.Add(-5 * time.Minute),
		State:          StoryStateActive,
	})

	var metaBytesBefore []byte
	b, err := store.Get(keys.StoryMeta(storyID))
	require.NoError(t, err)
	metaBytesBefore = append([]byte(nil), b...)

	// Ingest 5 signals matching this story.
	for i := 0; i < 5; i++ {
		sig := Signal[string]{
			ID:         uuid.New(),
			At:         now.Add(time.Duration(i) * time.Second),
			Embeddings: []Embedding{[]float32{0.01, 0.99, 0}},
		}
		assigned, err := tr.Ingest(context.Background(), sig)
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{storyID}, assigned)
	}

	b, err = store.Get(keys.StoryMeta(storyID))
	require.NoError(t, err)
	assert.Equal(t, metaBytesBefore, b, "s:m must remain strictly byte-identical through multiple ingest calls")
}
