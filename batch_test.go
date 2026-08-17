package story

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.kvsh.ch/streaming-story/internal/keys"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSignalKey(t *testing.T) {
	sid := uuid.New()
	prefix := keys.SignalPrefix(uuid.New())
	id, ok := keys.ParseSignal(keys.Signal(uuid.New(), sid), prefix)
	assert.True(t, ok)
	assert.Equal(t, sid, id)

	_, ok = keys.ParseSignal([]byte("s:short"), prefix)
	assert.False(t, ok)
	_, ok = keys.ParseSignal(prefix, prefix)
	assert.False(t, ok)
}
func TestMoveSignal(t *testing.T) {
	ms := newMemStore()
	from := keys.Signal(uuid.New(), uuid.New())
	to := keys.Outlier(uuid.New())

	require.NoError(t, ms.Update(func(tx Tx) error {
		return tx.Put(from, []byte("payload"))
	}))
	require.NoError(t, ms.Update(func(tx Tx) error {
		return moveSignal(tx, from, to)
	}))

	require.NoError(t, ms.View(func(tx Tx) error {
		v, err := tx.Get(from)
		require.NoError(t, err)
		assert.Nil(t, v, "source must be deleted")
		v, err = tx.Get(to)
		require.NoError(t, err)
		assert.Equal(t, []byte("payload"), v)
		return nil
	}))

	// Moving a missing key is a no-op.
	require.NoError(t, ms.Update(func(tx Tx) error {
		return moveSignal(tx, keys.Signal(uuid.New(), uuid.New()), to)
	}))
}

func TestMoveSignal_MaintainsLocationIndex(t *testing.T) {
	ms := newMemStore()
	storyA := uuid.New()
	storyB := uuid.New()
	sigID := uuid.New()

	keyA := keys.Signal(storyA, sigID)
	keyB := keys.Signal(storyB, sigID)
	keyO := keys.Outlier(sigID)

	require.NoError(t, ms.Update(func(tx Tx) error {
		return tx.Put(keyA, []byte("payload"))
	}))

	// story -> story: index follows the destination.
	require.NoError(t, ms.Update(func(tx Tx) error {
		return moveSignal(tx, keyA, keyB)
	}))
	require.NoError(t, ms.View(func(tx Tx) error {
		storyID, isOutlier, hasIndex, err := readSignalLoc(tx, sigID)
		require.NoError(t, err)
		require.True(t, hasIndex)
		assert.Equal(t, storyB, storyID)
		assert.False(t, isOutlier)
		return nil
	}))

	// story -> outlier: index records the outlier bucket.
	require.NoError(t, ms.Update(func(tx Tx) error {
		return moveSignal(tx, keyB, keyO)
	}))
	require.NoError(t, ms.View(func(tx Tx) error {
		_, isOutlier, hasIndex, err := readSignalLoc(tx, sigID)
		require.NoError(t, err)
		require.True(t, hasIndex)
		assert.True(t, isOutlier)
		return nil
	}))

	// outlier -> story: index follows the destination again.
	require.NoError(t, ms.Update(func(tx Tx) error {
		return moveSignal(tx, keyO, keyA)
	}))
	require.NoError(t, ms.View(func(tx Tx) error {
		storyID, isOutlier, hasIndex, err := readSignalLoc(tx, sigID)
		require.NoError(t, err)
		require.True(t, hasIndex)
		assert.Equal(t, storyA, storyID)
		assert.False(t, isOutlier)
		return nil
	}))
}

func TestEvictionDeletesLocationIndex(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	sigID := uuid.New()
	encoded, err := tr.cfg.Codec.Encode(Signal[string]{ID: sigID, At: time.Now(), Embedding: []float32{0, 0}})
	require.NoError(t, err)
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		if err := tx.Put(keys.Outlier(sigID), encoded); err != nil {
			return err
		}
		return writeSignalLoc(tx, sigID, uuid.Nil, true)
	}))

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		if err := tx.Delete(keys.Outlier(sigID)); err != nil {
			return err
		}
		return tx.Delete(keys.SignalLoc(sigID))
	}))

	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		_, _, hasIndex, err := readSignalLoc(tx, sigID)
		require.NoError(t, err)
		assert.False(t, hasIndex, "evicted outlier must drop its location-index entry")
		return nil
	}))
}
func TestRunBatch_StoryCreationFromOutliers(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(3)
	ch := tr.Subscribe()

	embeddings := [][]float32{
		{1.00, 0.01, 0.02},
		{0.99, 0.02, -0.01},
		{1.01, -0.01, 0.01},
		{0.98, 0.03, 0.00},
		{1.00, 0.00, 0.00},
		{0.99, -0.01, 0.01},
	}
	for i, emb := range embeddings {
		sigID := uuid.NewSHA1(TrackerNamespace, []byte(fmt.Sprintf("batch-create-%d", i)))
		_, err := tr.Ingest(context.Background(), Signal[string]{ID: sigID, At: time.Now(), Embedding: emb})
		require.NoError(t, err)
	}

	// All signals are outliers (no story exists yet).
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		var n int
		err := tx.ScanPrefix([]byte("o:"), func(key, val []byte) error { n++; return nil })
		require.NoError(t, err)
		assert.Equal(t, len(embeddings), n)
		return nil
	}))

	tr.runBatch()

	// The batch must promote the outliers into a new story.
	summary := drainBatchComplete(t, ch)
	require.NotNil(t, summary)
	assert.GreaterOrEqual(t, summary.StoriesCreated, 1)
	assert.Equal(t, len(embeddings), summary.OutliersPromoted)

	stories := 0
	for range tr.Stories(StoryStateAny) {
		stories++
	}
	assert.Equal(t, 1, stories)
}

func TestRunBatch_Merge(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(3)
	ch := tr.Subscribe()

	storyA := uuid.NewSHA1(TrackerNamespace, []byte("merge-A"))
	storyB := uuid.NewSHA1(TrackerNamespace, []byte("merge-B"))
	now := time.Now()

	// Story A: 5 signals, Story B: 3 signals, all one tight cluster. A is
	// older, so it must survive the merge.
	aEmbs := [][]float32{
		{1.00, 0.01, 0.02},
		{0.99, 0.02, -0.01},
		{1.01, -0.01, 0.01},
		{0.98, 0.03, 0.00},
		{1.00, 0.00, 0.00},
	}
	bEmbs := [][]float32{
		{1.01, 0.00, 0.01},
		{0.99, 0.01, 0.00},
		{1.00, -0.01, 0.02},
	}

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		write := func(sid uuid.UUID, created time.Time, embs [][]float32) error {
			for i, emb := range embs {
				sigID := uuid.NewSHA1(sid, []byte(fmt.Sprintf("sig-%d", i)))
				sig := Signal[string]{ID: sigID, At: now.Add(-time.Minute), Embedding: emb}
				b, err := tr.cfg.Codec.Encode(sig)
				if err != nil {
					return err
				}
				if err := tx.Put(keys.Signal(sid, sigID), b); err != nil {
					return err
				}
			}
			return tr.writeStoryMeta(tx, sid, time.Time{}, storyRecord{
				State:        StoryStateActive,
				Centroid:     []float32{1, 0, 0},
				CreatedAt:    created,
				LastSignalAt: now.Add(-time.Minute),
			})
		}
		if err := write(storyA, now.Add(-2*time.Hour), aEmbs); err != nil {
			return err
		}
		return write(storyB, now.Add(-time.Hour), bEmbs)
	}))

	tr.runBatch()

	var events []StoryEvent[string]
	deadline := time.After(2 * time.Second)
drain:
	for {
		select {
		case ev := <-ch:
			events = append(events, ev)
		case <-deadline:
			break drain
		}
	}
	summary := drainSummary(events)
	require.NotNil(t, summary)
	assert.GreaterOrEqual(t, summary.StoriesMerged, 1)

	// A survives; B is gone.
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		assert.Nil(t, mustGet(t, tx, keys.StoryMeta(storyB)), "retired story B metadata must be deleted")
		assert.NotNil(t, mustGet(t, tx, keys.StoryMeta(storyA)), "survivor story A metadata must exist")
		return nil
	}))

	var merged bool
	for _, ev := range events {
		if ev.Kind == EventStoryMerged && ev.StoryID == storyA && ev.StoryID2 == storyB {
			merged = true
		}
	}
	assert.True(t, merged, "EventStoryMerged for A←B must be emitted")
}

func TestRunBatch_NoSignals(t *testing.T) {
	tr := newTestTracker(t)
	ch := tr.Subscribe()

	tr.runBatch()

	summary := drainBatchComplete(t, ch)
	require.NotNil(t, summary)
	assert.Equal(t, 0, summary.StoriesCreated)
	assert.Equal(t, 0, summary.StoriesMerged)
	assert.Equal(t, 0, summary.SignalsReassigned)
	assert.Equal(t, 0, summary.OutliersPromoted)
}

func drainBatchComplete(t *testing.T, ch <-chan StoryEvent[string]) *BatchSummary {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev.Kind == EventBatchComplete {
				return ev.BatchSummary
			}
		case <-deadline:
			t.Fatal("no EventBatchComplete received")
			return nil
		}
	}
}

// drainSummary returns the BatchSummary from an EventBatchComplete event, or
// nil if none was collected.
func drainSummary(events []StoryEvent[string]) *BatchSummary {
	for _, ev := range events {
		if ev.Kind == EventBatchComplete {
			return ev.BatchSummary
		}
	}
	return nil
}

// mustGet is a test helper returning the value for key or failing the test.
func mustGet(t *testing.T, tx Tx, key []byte) []byte {
	t.Helper()
	v, err := tx.Get(key)
	require.NoError(t, err)
	return v
}
