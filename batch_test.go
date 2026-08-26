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

func TestParseFacetMemberKey(t *testing.T) {
	sid := uuid.New()
	story := uuid.New()
	prefix := keys.FacetPrefix(story)

	id, facet, ok := keys.ParseFacetMember(keys.FacetMember(story, sid, 2), prefix)
	assert.True(t, ok)
	assert.Equal(t, sid, id)
	assert.Equal(t, 2, facet)

	_, _, ok = keys.ParseFacetMember([]byte("s:short"), prefix)
	assert.False(t, ok)
	_, _, ok = keys.ParseFacetMember(prefix, prefix)
	assert.False(t, ok)
	// A facet key of a different story must not parse under this prefix.
	_, _, ok = keys.ParseFacetMember(keys.FacetMember(uuid.New(), sid, 0), prefix)
	assert.False(t, ok)
}

// moveFacetToStory is the one operation that relocates a facet, and the
// location index must follow it in every direction.
func TestMoveFacetToStory_MaintainsLocationIndex(t *testing.T) {
	ms := newMemStore()
	storyA, storyB := uuid.New(), uuid.New()
	sigID := uuid.New()
	const facet = 0

	// Start in the outlier bucket.
	require.NoError(t, ms.Update(func(tx Tx) error {
		if err := holdFacetOutlier(tx, sigID, facet); err != nil {
			return err
		}
		return writeSignalLocSet(tx, sigID, []keys.FacetLoc{{IsOutlier: true}})
	}))

	// outlier -> story A.
	require.NoError(t, ms.Update(func(tx Tx) error {
		return moveFacetToStory(tx, uuid.Nil, storyA, sigID, facet)
	}))
	locs, hasIndex, err := readSignalLocSet(ms, sigID)
	require.NoError(t, err)
	require.True(t, hasIndex)
	assert.Equal(t, []keys.FacetLoc{{StoryID: storyA}}, locs)
	assert.NotNil(t, mustGet(t, ms, keys.FacetMember(storyA, sigID, facet)))
	assert.Nil(t, mustGet(t, ms, keys.OutlierFacet(sigID, facet)), "outlier marker must be cleared")

	// story A -> story B: the old membership must not linger.
	require.NoError(t, ms.Update(func(tx Tx) error {
		return moveFacetToStory(tx, storyA, storyB, sigID, facet)
	}))
	locs, _, err = readSignalLocSet(ms, sigID)
	require.NoError(t, err)
	assert.Equal(t, []keys.FacetLoc{{StoryID: storyB}}, locs)
	assert.Nil(t, mustGet(t, ms, keys.FacetMember(storyA, sigID, facet)))
	assert.NotNil(t, mustGet(t, ms, keys.FacetMember(storyB, sigID, facet)))
}

// Two facets of one signal converging on the same survivor must both survive:
// the key carries the facet index, so they cannot collide.
func TestMigrateFacets_KeepsBothFacetsOfOneSignal(t *testing.T) {
	ms := newMemStore()
	retired, survivor := uuid.New(), uuid.New()
	sigID := uuid.New()

	require.NoError(t, ms.Update(func(tx Tx) error {
		if err := placeFacet(tx, retired, sigID, 0); err != nil {
			return err
		}
		if err := placeFacet(tx, survivor, sigID, 1); err != nil {
			return err
		}
		return writeSignalLocSet(tx, sigID, []keys.FacetLoc{{StoryID: retired}, {StoryID: survivor}})
	}))

	require.NoError(t, ms.Update(func(tx Tx) error {
		return migrateFacets(tx, retired, survivor)
	}))

	assert.NotNil(t, mustGet(t, ms, keys.FacetMember(survivor, sigID, 0)))
	assert.NotNil(t, mustGet(t, ms, keys.FacetMember(survivor, sigID, 1)))
	assert.Nil(t, mustGet(t, ms, keys.FacetMember(retired, sigID, 0)))

	locs, _, err := readSignalLocSet(ms, sigID)
	require.NoError(t, err)
	assert.Equal(t, []keys.FacetLoc{{StoryID: survivor}, {StoryID: survivor}}, locs)
}

// Eviction must clear the outlier marker, the location index, and — because
// nothing else holds it — the canonical record.
func TestEvictionDropsFacetsIndexAndRecord(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	sigID := uuid.New()
	sig := Signal[string]{ID: sigID, At: time.Now(), Embeddings: []Embedding{{0, 0}}}
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return seedOutlier(tx, tr, sig)
	}))

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return evictOutlierFacets(tx, sigID)
	}))

	_, hasIndex, err := readSignalLocSet(tr.cfg.Store, sigID)
	require.NoError(t, err)
	assert.False(t, hasIndex, "evicted outlier must drop its location-index entry")
	assert.Nil(t, mustGet(t, tr.cfg.Store, keys.OutlierFacet(sigID, 0)))
	assert.Nil(t, mustGet(t, tr.cfg.Store, keys.CanonicalSignal(sigID)), "no facet remains, so the record goes too")
}

// The lifetime rule: a signal with a placed facet keeps its canonical record
// even when its unplaced facets age out.
func TestEviction_KeepsRecordWhileAFacetIsPlaced(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	storyID := uuid.New()
	sigID := uuid.New()
	sig := Signal[string]{ID: sigID, At: time.Now(), Embeddings: []Embedding{{1, 0}, {0, 1}}}
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		if err := tr.writeCanonicalSignal(tx, sig); err != nil {
			return err
		}
		if err := placeFacet(tx, storyID, sigID, 0); err != nil {
			return err
		}
		if err := holdFacetOutlier(tx, sigID, 1); err != nil {
			return err
		}
		return writeSignalLocSet(tx, sigID, []keys.FacetLoc{{StoryID: storyID}, {IsOutlier: true}})
	}))

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return evictOutlierFacets(tx, sigID)
	}))

	assert.NotNil(t, mustGet(t, tr.cfg.Store, keys.CanonicalSignal(sigID)), "a placed facet keeps the record alive")
	assert.NotNil(t, mustGet(t, tr.cfg.Store, keys.FacetMember(storyID, sigID, 0)), "placed facet is untouched")
	assert.Nil(t, mustGet(t, tr.cfg.Store, keys.OutlierFacet(sigID, 1)), "unplaced facet is evicted")

	locs, hasIndex, err := readSignalLocSet(tr.cfg.Store, sigID)
	require.NoError(t, err)
	require.True(t, hasIndex)
	assert.Equal(t, []keys.FacetLoc{{StoryID: storyID}, {}}, locs)
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
		_, err := tr.Ingest(context.Background(), Signal[string]{ID: sigID, At: time.Now(), Embeddings: []Embedding{emb}})
		require.NoError(t, err)
	}

	// All signals are outliers (no story exists yet).
	var n int
	err := tr.cfg.Store.ScanPrefix([]byte("o:"), func(key, val []byte) error { n++; return nil })
	require.NoError(t, err)
	assert.Equal(t, len(embeddings), n)

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
				sig := Signal[string]{ID: sigID, At: now.Add(-time.Minute), Embeddings: []Embedding{emb}}
				if err := seedMember(tx, tr, sid, sig); err != nil {
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
	assert.Nil(t, mustGet(t, tr.cfg.Store, keys.StoryMeta(storyB)), "retired story B metadata must be deleted")
	assert.NotNil(t, mustGet(t, tr.cfg.Store, keys.StoryMeta(storyA)), "survivor story A metadata must exist")

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
// It takes a Reader so the same call works against a store and inside a write
// transaction.
func mustGet(t *testing.T, r Reader, key []byte) []byte {
	t.Helper()
	v, err := r.Get(key)
	require.NoError(t, err)
	return v
}
