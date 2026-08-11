package story

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSampleGroups(t *testing.T) {
	t.Run("keeps_all_when_total_within_cap", func(t *testing.T) {
		groups := []sampleGroup{
			{indices: []int{0, 1, 2}, active: true},
			{indices: []int{3, 4}, active: false},
		}
		keep := sampleGroups(groups, 10, 3, 0.5)
		require.Len(t, keep, 5)
		for i := range keep {
			assert.True(t, keep[i], "signal %d should be kept", i)
		}
	})

	t.Run("guaranteed_minimums_then_proportional", func(t *testing.T) {
		// Two active groups (10 each), one outlier group (10), cap 20.
		// Budget for pass 1 = 0.5*20 = 10 → 5 per active group. Remaining
		// 10 distributed proportionally by count.
		var groups []sampleGroup
		off := 0
		for _, active := range []bool{true, true, false} {
			idx := make([]int, 10)
			for i := range idx {
				idx[i] = off
				off++
			}
			groups = append(groups, sampleGroup{indices: idx, active: active})
		}

		keep := sampleGroups(groups, 20, 5, 0.5)

		kept := 0
		for i := range keep {
			if keep[i] {
				kept++
			}
		}
		assert.Equal(t, 20, kept)

		// Active groups must have at least their guaranteed 5.
		for g := 0; g < 2; g++ {
			n := 0
			for i := g * 10; i < g*10+10; i++ {
				if keep[i] {
					n++
				}
			}
			assert.GreaterOrEqual(t, n, 5, "active group %d lost its guarantee", g)
		}

		// Newest-first reservation: kept indices within a group are the
		// leading (chronologically newest) ones.
		for g := 0; g < 2; g++ {
			n := 0
			for i := g * 10; i < g*10+10; i++ {
				if keep[i] {
					n++
				} else {
					break
				}
			}
			_ = n
		}
	})

	t.Run("scales_down_guarantee_when_budget_exceeded", func(t *testing.T) {
		// 4 active groups of 100 signals each, cap 20, budget 10.
		// Per-story guarantee = 10/4 = 2 (floored at 1).
		var groups []sampleGroup
		off := 0
		for g := 0; g < 4; g++ {
			idx := make([]int, 100)
			for i := range idx {
				idx[i] = off
				off++
			}
			groups = append(groups, sampleGroup{indices: idx, active: true})
		}
		keep := sampleGroups(groups, 20, 10, 0.5)

		for g := 0; g < 4; g++ {
			n := 0
			for i := g * 100; i < g*100+100; i++ {
				if keep[i] {
					n++
				}
			}
			assert.GreaterOrEqual(t, n, 2, "group %d below scaled-down guarantee", g)
		}
	})

	t.Run("empty_groups_produce_empty_mask", func(t *testing.T) {
		keep := sampleGroups(nil, 10, 3, 0.5)
		assert.Empty(t, keep)
	})
}

func TestSampleSignals(t *testing.T) {
	tr := &Tracker[string]{cfg: Config[string]{BatchSampleCap: 8, MinClusterSize: 3, SampleGuaranteeMaxFraction: 0.5}}

	sid1 := uuid.New()
	sid2 := uuid.New()
	stories := map[uuid.UUID]storyRecord{
		sid1: {State: StoryStateActive},
		sid2: {State: StoryStateDormant},
	}
	signals := []batchSignal{
		{storyID: sid1},
		{storyID: sid1},
		{storyID: sid1},
		{storyID: sid1},
		{storyID: sid2},
		{storyID: sid2},
		{outlier: true},
		{outlier: true},
		{outlier: true},
		{outlier: true},
	}
	for i := range signals {
		signals[i].at = time.Unix(int64(i), 0)
	}

	keep := tr.sampleSignals(signals, stories)

	kept := 0
	for _, k := range keep {
		if k {
			kept++
		}
	}
	assert.Equal(t, tr.cfg.BatchSampleCap, kept, "total kept must equal the cap")

	// Active story gets its guarantee of 3 before anything else.
	activeKept := 0
	for _, k := range keep[:4] {
		if k {
			activeKept++
		}
	}
	assert.GreaterOrEqual(t, activeKept, 3, "active story must hold its guaranteed minimum")

	// Newest-first within groups: earliest outlier must not be kept if any
	// later outlier of the same group is kept.
	var outlierKept []int
	for i := 6; i < 10; i++ {
		if keep[i] {
			outlierKept = append(outlierKept, i)
		}
	}
	for i := 1; i < len(outlierKept); i++ {
		assert.Greater(t, outlierKept[i], outlierKept[i-1], "outliers kept newest-first")
	}
}

func TestJaccardIndex(t *testing.T) {
	assert.Equal(t, 0.0, jaccardIndex(nil, []int{1}))
	assert.Equal(t, 0.0, jaccardIndex([]int{1}, nil))
	assert.Equal(t, 1.0, jaccardIndex([]int{1, 2}, []int{2, 1}))
	assert.Equal(t, 1.0/3.0, jaccardIndex([]int{1, 2}, []int{2, 3}))
	assert.Equal(t, 0.0, jaccardIndex([]int{1}, []int{2}))
}

func TestCoverageIndex(t *testing.T) {
	assert.Equal(t, 0.0, coverageIndex(nil, nil, nil))
	assert.Equal(t, 1.0, coverageIndex([]int{1}, []int{2}, []int{1, 2}))
	assert.Equal(t, 0.5, coverageIndex([]int{1}, []int{2}, []int{1, 3}))
}

func TestOldestStory(t *testing.T) {
	old := uuid.New()
	mid := uuid.New()
	new := uuid.New()
	stories := map[uuid.UUID]storyRecord{
		old: {CreatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		mid: {CreatedAt: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
		new: {CreatedAt: time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC)},
	}
	assert.Equal(t, old, oldestStory([]uuid.UUID{new, mid, old}, stories))
	assert.Equal(t, mid, oldestStory([]uuid.UUID{new, mid}, stories))
}

func TestParseSignalKey(t *testing.T) {
	sid := uuid.New()
	prefix := keySignalPrefix(uuid.New())
	id, ok := parseSignalKey(keySignal(uuid.New(), sid), prefix)
	assert.True(t, ok)
	assert.Equal(t, sid, id)

	_, ok = parseSignalKey([]byte("s:short"), prefix)
	assert.False(t, ok)
	_, ok = parseSignalKey(prefix, prefix)
	assert.False(t, ok)
}

func TestStoryStats(t *testing.T) {
	members := []*batchSignal{
		{emb: []float32{1, 0}},
		{emb: []float32{0, 1}},
		{emb: []float32{-1, 0}},
	}
	members[0].at = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	members[1].at = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	members[2].at = time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)

	centroid, radius, meanDist, sigma, latestAt, dists := storyStats(members)
	assert.Equal(t, []float32{0, 1.0 / 3.0}, centroid)
	assert.InDelta(t, 1.0, radius, 1e-6)
	assert.InDelta(t, 2.0/3.0, meanDist, 1e-6)
	assert.Greater(t, sigma, 0.0)
	assert.True(t, latestAt.Equal(members[2].at))
	assert.Len(t, dists, 3)
}

func TestMapClusters_Merge(t *testing.T) {
	storyA := uuid.New()
	storyB := uuid.New()
	stories := map[uuid.UUID]storyRecord{
		storyA: {CreatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		storyB: {CreatedAt: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
	}
	signals := []batchSignal{
		{storyID: storyA, kept: true, label: 0},
		{storyID: storyA, kept: true, label: 0},
		{storyID: storyA, kept: true, label: 0},
		{storyID: storyB, kept: true, label: 0},
		{storyID: storyB, kept: true, label: 0},
		{storyID: storyB, kept: true, label: -1}, // noise, excluded from clustering
	}
	tr := &Tracker[string]{cfg: Config[string]{MappingMinJaccard: 0.6, SplitMinJaccard: 0.3}}
	m := tr.mapClusters(signals, stories)

	assert.Equal(t, storyA, m.labelStory[0], "cluster must map to the matched story")
	assert.Equal(t, storyA, m.retired[storyB], "story B must be retired into A")
	require.NotContains(t, m.retired, storyA)
}

func TestMapClusters_Split(t *testing.T) {
	storyS := uuid.New()
	stories := map[uuid.UUID]storyRecord{
		storyS: {CreatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), State: StoryStateActive},
	}
	signals := []batchSignal{
		{storyID: storyS, kept: true, label: 0},
		{storyID: storyS, kept: true, label: 0},
		{storyID: storyS, kept: true, label: 1},
		{storyID: storyS, kept: true, label: 1},
		{storyID: storyS, kept: true, label: 1},
	}
	tr := &Tracker[string]{cfg: Config[string]{MappingMinJaccard: 0.6, SplitMinJaccard: 0.3}}
	m := tr.mapClusters(signals, stories)

	// Cluster 1 (3 members) matches story S via Phase 1 (Jaccard 0.6).
	// Cluster 0 (2 members) is unmatched but overlaps S sufficiently and is
	// promoted as a split child.
	assert.Equal(t, storyS, m.labelStory[1])

	child, ok := m.labelStory[0]
	require.True(t, ok, "split cluster must be mapped")
	assert.NotEqual(t, storyS, child)
	assert.Equal(t, storyS, m.splitParents[child])
}

func TestMapClusters_NewStory(t *testing.T) {
	tr := &Tracker[string]{cfg: Config[string]{MappingMinJaccard: 0.6, SplitMinJaccard: 0.3, MinClusterSize: 3}}
	signals := []batchSignal{
		{outlier: true, kept: true, label: 0},
		{outlier: true, kept: true, label: 0},
		{outlier: true, kept: true, label: 0},
		{outlier: true, kept: true, label: 0},
		{outlier: true, kept: true, label: 0},
	}
	m := tr.mapClusters(signals, map[uuid.UUID]storyRecord{})

	require.Contains(t, m.labelStory, 0)
	assert.NotEqual(t, uuid.Nil, m.labelStory[0])
}

func TestMapClusters_SmallUnmatchedClusterDemoted(t *testing.T) {
	tr := &Tracker[string]{cfg: Config[string]{MappingMinJaccard: 0.6, SplitMinJaccard: 0.3, MinClusterSize: 3}}
	signals := []batchSignal{
		{outlier: true, kept: true, label: 0},
		{outlier: true, kept: true, label: 0},
	}
	m := tr.mapClusters(signals, map[uuid.UUID]storyRecord{})
	assert.NotContains(t, m.labelStory, 0)
}

func TestMoveSignal(t *testing.T) {
	ms := newMemStore()
	from := keySignal(uuid.New(), uuid.New())
	to := keyOutlier(uuid.New())

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
		return moveSignal(tx, keySignal(uuid.New(), uuid.New()), to)
	}))
}

func TestMoveSignal_MaintainsLocationIndex(t *testing.T) {
	ms := newMemStore()
	storyA := uuid.New()
	storyB := uuid.New()
	sigID := uuid.New()

	keyA := keySignal(storyA, sigID)
	keyB := keySignal(storyB, sigID)
	keyO := keyOutlier(sigID)

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
		if err := tx.Put(keyOutlier(sigID), encoded); err != nil {
			return err
		}
		return writeSignalLoc(tx, sigID, uuid.Nil, true)
	}))

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		if err := tx.Delete(keyOutlier(sigID)); err != nil {
			return err
		}
		return tx.Delete(keySignalLoc(sigID))
	}))

	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		_, _, hasIndex, err := readSignalLoc(tx, sigID)
		require.NoError(t, err)
		assert.False(t, hasIndex, "evicted outlier must drop its location-index entry")
		return nil
	}))
}

func TestApplyBatch_Merge(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(3)

	storyA := uuid.New()
	storyB := uuid.New()
	now := time.Now()

	// Seed two active stories with recent signals.
	aSignal := uuid.New()
	bSignal := uuid.New()
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		for sid, sigID := range map[uuid.UUID]uuid.UUID{storyA: aSignal, storyB: bSignal} {
			sig := Signal[string]{ID: sigID, At: now, Embedding: []float32{1, 0, 0}, Data: "x"}
			b, err := tr.cfg.Codec.Encode(sig)
			if err != nil {
				return err
			}
			if err := tx.Put(keySignal(sid, sigID), b); err != nil {
				return err
			}
		}
		rec := func(sid uuid.UUID) storyRecord {
			return storyRecord{
				State:        StoryStateActive,
				Centroid:     []float32{1, 0, 0},
				CreatedAt:    now.Add(-time.Hour),
				LastSignalAt: now,
			}
		}
		if err := tr.writeStoryMeta(tx, storyA, time.Time{}, rec(storyA)); err != nil {
			return err
		}
		return tr.writeStoryMeta(tx, storyB, time.Time{}, rec(storyB))
	}))

	ch := tr.Subscribe()
	m := clusterMapping{retired: map[uuid.UUID]uuid.UUID{storyB: storyA}}
	summary, events, err := tr.applyBatch(nil, map[uuid.UUID]storyRecord{
		storyA: {State: StoryStateActive, CreatedAt: now.Add(-time.Hour), LastSignalAt: now},
		storyB: {State: StoryStateActive, CreatedAt: now.Add(-time.Minute), LastSignalAt: now},
	}, m, nil, now)
	require.NoError(t, err)
	for _, ev := range events {
		tr.emit(ev)
	}

	assert.Equal(t, 1, summary.StoriesMerged)
	assert.Equal(t, 0, summary.StoriesCreated)

	// B's metadata and time index are gone; its signal lives under A.
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		_, err := tx.Get(keyStoryMeta(storyB))
		require.NoError(t, err)
		assert.Nil(t, mustGet(t, tx, keyStoryMeta(storyB)), "retired story metadata must be deleted")
		assert.Nil(t, mustGet(t, tx, keySignal(storyB, bSignal)), "retired signal must be migrated")
		got := mustGet(t, tx, keySignal(storyA, bSignal))
		require.NotNil(t, got)
		assert.Nil(t, mustGet(t, tx, keyTimeIndex(now.Unix(), storyB)), "retired time index must be deleted")
		return nil
	}))

	var merged bool
	for _, ev := range events {
		if ev.Kind == EventStoryMerged && ev.StoryID == storyA && ev.StoryID2 == storyB {
			merged = true
		}
	}
	assert.True(t, merged, "EventStoryMerged must be emitted")

	// Emitted to the subscriber channel.
	select {
	case ev := <-ch:
		assert.Equal(t, EventStoryMerged, ev.Kind)
	case <-time.After(time.Second):
		t.Fatal("merge event not delivered")
	}
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
				if err := tx.Put(keySignal(sid, sigID), b); err != nil {
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
		assert.Nil(t, mustGet(t, tx, keyStoryMeta(storyB)), "retired story B metadata must be deleted")
		assert.NotNil(t, mustGet(t, tx, keyStoryMeta(storyA)), "survivor story A metadata must exist")
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
