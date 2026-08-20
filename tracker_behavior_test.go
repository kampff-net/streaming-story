package story

import (
	"context"
	"testing"
	"time"

	"go.kvsh.ch/streaming-story/internal/keys"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalcThreshold(t *testing.T) {
	mk := func(state StoryState, mc, sc int, md, sigma, fmd, fsig float64) StoryMeta {
		return StoryMeta{
			State:              state,
			MeanDistance:       md,
			Sigma:              sigma,
			SignalCount:        sc,
			FrozenMeanDistance: fmd,
			FrozenSigma:        fsig,
		}
	}

	t.Run("cold_start_uses_sigma_global", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{AssignmentK: 2.0, SigmaFloor: 0.1, ColdStartMinSignals: 5}}
		tr.sigmaGlobal = 0.5
		meta := mk(StoryStateActive, 0, 3, 0.4, 0.2, 0, 0)
		assert.InDelta(t, 1.0, tr.calcThreshold(meta), 1e-9)
	})

	t.Run("mature_story_uses_live_stats", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{AssignmentK: 2.0, SigmaFloor: 0.1, ColdStartMinSignals: 5}}
		tr.sigmaGlobal = 0.5
		meta := mk(StoryStateActive, 0, 6, 0.3, 0.1, 0, 0)
		assert.InDelta(t, 0.5, tr.calcThreshold(meta), 1e-9)
	})

	t.Run("sigma_is_floored", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{AssignmentK: 2.0, SigmaFloor: 0.1, ColdStartMinSignals: 5}}
		tr.sigmaGlobal = 1.0 // floor = 0.1
		meta := mk(StoryStateActive, 0, 6, 0.3, 0.001, 0, 0)
		assert.InDelta(t, 0.5, tr.calcThreshold(meta), 1e-9)
	})

	t.Run("dormant_uses_frozen_stats", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{AssignmentK: 2.0, SigmaFloor: 0.1, ColdStartMinSignals: 5}}
		tr.sigmaGlobal = 1.0
		meta := mk(StoryStateDormant, 0, 0, 0, 0, 0.4, 0.05) // frozen sigma floored to 0.1
		assert.InDelta(t, 0.6, tr.calcThreshold(meta), 1e-9)
	})

	t.Run("dormant_without_frozen_falls_back_to_sigma_global", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{AssignmentK: 2.0, SigmaFloor: 0.1, ColdStartMinSignals: 5}}
		tr.sigmaGlobal = 0.5
		meta := mk(StoryStateDormant, 0, 0, 0, 0, 0.2, 0)
		assert.InDelta(t, 1.2, tr.calcThreshold(meta), 1e-9)
	})

	t.Run("unmeasured_sigma_global_uses_InitialSigmaGlobal", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{
			AssignmentK: 2.0, SigmaFloor: 0.1, ColdStartMinSignals: 5,
			InitialSigmaGlobal: 0.25,
		}}
		meta := mk(StoryStateActive, 0, 3, 0, 0, 0, 0)
		assert.InDelta(t, 0.5, tr.calcThreshold(meta), 1e-9)
	})

	t.Run("InitialSigmaGlobal_is_configurable", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{
			AssignmentK: 2.0, SigmaFloor: 0.1, ColdStartMinSignals: 5,
			InitialSigmaGlobal: 0.6,
		}}
		meta := mk(StoryStateActive, 0, 3, 0, 0, 0, 0)
		assert.InDelta(t, 1.2, tr.calcThreshold(meta), 1e-9)
	})

	t.Run("measured_sigma_global_ignores_InitialSigmaGlobal", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{
			AssignmentK: 2.0, SigmaFloor: 0.1, ColdStartMinSignals: 5,
			InitialSigmaGlobal: 9,
		}}
		tr.sigmaGlobal = 0.5
		meta := mk(StoryStateActive, 0, 3, 0, 0, 0, 0)
		assert.InDelta(t, 1.0, tr.calcThreshold(meta), 1e-9)
	})
}

// seedStory writes a story record plus its time index via writeStoryMeta, and updates the in-memory index.
func seedStory(t *testing.T, tr *Tracker[string], id uuid.UUID, rec storyRecord) {
	t.Helper()
	if len(rec.Centroid) > 0 && tr.dim.Load() == 0 {
		tr.dim.Store(int32(len(rec.Centroid)))
	}
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return tr.writeStoryMeta(tx, id, time.Time{}, rec)
	}))
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		idx, err := tr.buildActiveStoryIndex(tx)
		if err != nil {
			return err
		}
		tr.storyIndex.Store(idx)
		return nil
	}))
}

func TestTracker_Ingest_ReingestIsIdempotent(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	storyID := uuid.NewSHA1(TrackerNamespace, []byte("idem-story"))
	seedStory(t, tr, storyID, storyRecord{
		State:        StoryStateActive,
		Centroid:     []float32{1, 0},
		CreatedAt:    time.Now().Add(-time.Hour),
		LastSignalAt: time.Now().Add(-time.Minute),
	})

	ch := tr.Subscribe()
	sig := Signal[string]{ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{[]float32{0.98, 0.02}}}

	assigned, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyID}, assigned)

	// Re-ingesting the identical signal is a no-op: same story, no duplicate
	// emit, and exactly one stored copy.
	assigned2, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyID}, assigned2)

	select {
	case ev := <-ch:
		assert.Equal(t, EventDraftAssigned, ev.Kind)
	case <-time.After(time.Second):
		t.Fatal("no draft-assigned event")
	}
	select {
	case ev := <-ch:
		t.Fatalf("unexpected second event: %+v", ev)
	default:
	}

	var stored int
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		return tx.ScanPrefix(keys.FacetPrefix(storyID), func(key, val []byte) error {
			stored++
			return nil
		})
	}))
	assert.Equal(t, 1, stored)
}

func TestTracker_Ingest_CrossStoryMoveReingestDoesNotDuplicate(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	storyA := uuid.NewSHA1(TrackerNamespace, []byte("cross-story-a"))
	storyB := uuid.NewSHA1(TrackerNamespace, []byte("cross-story-b"))
	seedStory(t, tr, storyA, storyRecord{
		State:        StoryStateActive,
		Centroid:     []float32{1, 0},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		LastSignalAt: time.Now().Add(-time.Hour),
	})

	sig := Signal[string]{ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{[]float32{0.98, 0.02}}}
	assigned, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyA}, assigned)

	// A batch run moves the signal from storyA to storyB (merge/re-assign).
	seedStory(t, tr, storyB, storyRecord{
		State:        StoryStateActive,
		Centroid:     []float32{1, 0},
		CreatedAt:    time.Now().Add(-time.Hour),
		LastSignalAt: time.Now(),
	})
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return moveFacetToStory(tx, storyA, storyB, sig.ID, 0)
	}))

	// Re-ingestion must find the copy's batch-moved location and be a no-op:
	// it returns storyB, emits nothing, and leaves exactly one copy under B.
	assigned2, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyB}, assigned2)

	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		assert.Nil(t, mustGet(t, tx, keys.FacetMember(storyA, sig.ID, 0)), "no membership may remain under the old story")
		assert.NotNil(t, mustGet(t, tx, keys.FacetMember(storyB, sig.ID, 0)), "the moved facet must stay put")

		var markers int
		for _, prefix := range [][]byte{keys.FacetPrefix(storyA), keys.FacetPrefix(storyB)} {
			require.NoError(t, tx.ScanPrefix(prefix, func(key, val []byte) error {
				markers++
				return nil
			}))
		}
		assert.Equal(t, 1, markers, "exactly one membership must exist across both stories")
		return nil
	}))
}

func TestTracker_Ingest_DeletesOutlierCopyOnAssignment(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	storyID := uuid.NewSHA1(TrackerNamespace, []byte("outlier-cleanup"))
	seedStory(t, tr, storyID, storyRecord{
		State:        StoryStateActive,
		Centroid:     []float32{1, 0},
		CreatedAt:    time.Now().Add(-time.Hour),
		LastSignalAt: time.Now().Add(-time.Minute),
	})

	sig := Signal[string]{ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{{0.98, 0.02}}}
	// Simulate the same signal already sitting in the outlier bucket.
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return seedOutlier(tx, tr, sig)
	}))

	assigned, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyID}, assigned)

	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		assert.Nil(t, mustGet(t, tx, keys.OutlierFacet(sig.ID, 0)), "stale outlier marker must be removed")
		assert.NotNil(t, mustGet(t, tx, keys.FacetMember(storyID, sig.ID, 0)))
		return nil
	}))
}

func TestTracker_Ingest_MonotonicLastSignalAt(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	storyID := uuid.NewSHA1(TrackerNamespace, []byte("monotonic"))
	older := time.Now().Add(-time.Hour)
	seedStory(t, tr, storyID, storyRecord{
		State:        StoryStateActive,
		Centroid:     []float32{1, 0},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		LastSignalAt: older,
	})

	// A newer signal advances LastSignalAt; an older one must not regress it.
	_, err := tr.Ingest(context.Background(), Signal[string]{
		ID: uuid.New(), At: older.Add(-time.Hour), Embeddings: []Embedding{[]float32{0.98, 0.02}},
	})
	require.NoError(t, err)

	meta, err := tr.Story(storyID)
	require.NoError(t, err)
	assert.True(t, meta.LastSignalAt.Equal(older), "out-of-order signal must not regress LastSignalAt")
}

func TestTracker_Ingest_ReactivateClearsStats(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	storyID := uuid.NewSHA1(TrackerNamespace, []byte("reactivate"))
	seedStory(t, tr, storyID, storyRecord{
		State:              StoryStateDormant,
		Centroid:           []float32{1, 0},
		CreatedAt:          time.Now().Add(-2 * time.Hour),
		LastSignalAt:       time.Now().Add(-10 * time.Hour),
		StatsAt:            time.Now().Add(-10 * time.Hour),
		FrozenMeanDistance: 0.4,
		FrozenSigma:        0.1,
	})

	var metaBytesBefore []byte
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		b, err := tx.Get(keys.StoryMeta(storyID))
		metaBytesBefore = b
		return err
	}))

	assigned, err := tr.Ingest(context.Background(), Signal[string]{
		ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{[]float32{0.98, 0.02}},
	})
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyID}, assigned)

	meta, err := tr.Story(storyID)
	require.NoError(t, err)
	assert.Equal(t, StoryStateActive, meta.State)
	assert.False(t, meta.ReactivatedAt.IsZero())
	assert.True(t, meta.ReactivatedAt.After(meta.StatsAt))

	// Threshold calculation treats reactivated story as cold-start:
	tr.calibMu.Lock()
	tr.sigmaGlobal = 0.2
	tr.calibMu.Unlock()
	thresh := tr.calcThreshold(meta)
	assert.Equal(t, tr.cfg.AssignmentK*0.2, thresh)

	// Ensure s:{id}:m is byte-identical: Ingest never touches s:m
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		b, err := tx.Get(keys.StoryMeta(storyID))
		require.NoError(t, err)
		assert.Equal(t, metaBytesBefore, b, "s:m must be byte-identical before and after ingest")
		return nil
	}))
}

func TestTracker_Stories_Iterator(t *testing.T) {
	tr := newTestTracker(t)

	activeID := uuid.NewSHA1(TrackerNamespace, []byte("iter-active"))
	dormantID := uuid.NewSHA1(TrackerNamespace, []byte("iter-dormant"))
	suppressedID := uuid.NewSHA1(TrackerNamespace, []byte("iter-suppressed"))
	seedStory(t, tr, activeID, storyRecord{State: StoryStateActive, Centroid: []float32{1}, CreatedAt: time.Now()})
	seedStory(t, tr, dormantID, storyRecord{State: StoryStateDormant, Centroid: []float32{1}, CreatedAt: time.Now()})
	seedStory(t, tr, suppressedID, storyRecord{
		State: StoryStateSuppressed, WasSuppressed: true, SuppressionReason: "spam",
		Centroid: []float32{1}, CreatedAt: time.Now(),
	})

	ids := map[uuid.UUID]bool{}
	for meta := range tr.Stories(StoryStateAny) {
		ids[meta.ID] = true
	}
	assert.Equal(t, 3, len(ids))

	ids = map[uuid.UUID]bool{}
	for meta := range tr.Stories(StoryStateActive) {
		ids[meta.ID] = true
	}
	assert.Len(t, ids, 1, "Stories(StoryStateActive) must exclude suppressed and dormant stories")
	assert.True(t, ids[activeID])

	ids = map[uuid.UUID]bool{}
	for meta := range tr.Stories(StoryStateSuppressed) {
		ids[meta.ID] = true
	}
	assert.Len(t, ids, 1)
	assert.True(t, ids[suppressedID])
}

func TestTracker_SignalsOf_Iterator(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	storyID := uuid.NewSHA1(TrackerNamespace, []byte("signals-of"))
	seedStory(t, tr, storyID, storyRecord{
		State:        StoryStateActive,
		Centroid:     []float32{1, 0},
		CreatedAt:    time.Now().Add(-time.Hour),
		LastSignalAt: time.Now().Add(-time.Minute),
	})

	for i := 0; i < 3; i++ {
		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{[]float32{0.98, 0.02}},
		})
		require.NoError(t, err)
	}

	var count int
	for sig, err := range tr.SignalsOf(storyID) {
		require.NoError(t, err)
		assert.Len(t, sig.Embeddings[0], 2)
		count++
	}
	assert.Equal(t, 3, count)
}

func TestTracker_Ingest_ExcludesStaleStories(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)
	tr.cfg.ActiveContextWindow = 24 * time.Hour

	// Story whose last signal is older than the active context window.
	staleID := uuid.NewSHA1(TrackerNamespace, []byte("stale"))
	seedStory(t, tr, staleID, storyRecord{
		State:        StoryStateActive,
		Centroid:     []float32{1, 0},
		CreatedAt:    time.Now().Add(-48 * time.Hour),
		LastSignalAt: time.Now().Add(-48 * time.Hour),
	})

	// The stale story must not anchor the signal: it becomes an outlier.
	assigned, err := tr.Ingest(context.Background(), Signal[string]{
		ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{[]float32{0.98, 0.02}},
	})
	require.NoError(t, err)
	assert.Empty(t, assigned, "stale story outside ActiveContextWindow must not anchor")
}

func TestTracker_Subscribe_AfterCloseReturnsClosedChannel(t *testing.T) {
	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		BatchInterval: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, tr.Close())

	ch := tr.Subscribe()
	_, ok := <-ch
	assert.False(t, ok, "Subscribe after Close must return a closed channel")
}

func TestTracker_Close_IsIdempotent(t *testing.T) {
	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		BatchInterval: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, tr.Close())
	require.NoError(t, tr.Close(), "second Close must return nil")
}

func TestWriteStoryMeta_TimeIndexMaintenance(t *testing.T) {
	tr := newTestTracker(t)
	storyID := uuid.NewSHA1(TrackerNamespace, []byte("timeindex"))

	first := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	second := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	seedStory(t, tr, storyID, storyRecord{State: StoryStateActive, LastSignalAt: first, CreatedAt: first})
	assertTimeIndex(t, tr, storyID, first)

	// Updating LastSignalAt must move the index entry.
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return tr.writeStoryMeta(tx, storyID, first, storyRecord{State: StoryStateActive, LastSignalAt: second, CreatedAt: first})
	}))
	assertTimeIndex(t, tr, storyID, second)

	// No duplicate index entries for the same story.
	var n int
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		return tx.ScanPrefix([]byte("t:"), func(key, val []byte) error {
			n++
			return nil
		})
	}))
	assert.Equal(t, 1, n)
}

func assertTimeIndex(t *testing.T, tr *Tracker[string], storyID uuid.UUID, at time.Time) {
	t.Helper()
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		v := mustGet(t, tx, keys.TimeIndex(at.Unix(), storyID))
		require.NotNil(t, v, "time index entry missing for %v", at)
		return nil
	}))
}

func TestPersistStory_LifecycleTransitions(t *testing.T) {
	tr := newTestTracker(t)
	now := time.Now()

	t.Run("active_to_dormant_freezes_stats", func(t *testing.T) {
		sid := uuid.New()
		prev := storyRecord{
			State:        StoryStateActive,
			Centroid:     []float32{1, 0},
			CreatedAt:    now.Add(-2 * time.Hour),
			LastSignalAt: now.Add(-8 * 24 * time.Hour), // beyond SilenceWindow
			MeanDistance: 0.3,
			Sigma:        0.1,
			SignalCount:  10,
		}
		var summary BatchSummary
		var events []StoryEvent[string]
		// Lifecycle is driven by membership now: an empty story is retired
		// rather than transitioned, so the subject needs a member whose age
		// puts it past the window under test.
		members := []*batchFacet{{id: uuid.New(), at: prev.LastSignalAt, emb: []float32{1, 0}}}
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tr.recentreStory(tx, sid, prev, true, members, &emaAccum{}, &summary, &events, now)
		}))

		require.Len(t, events, 1)
		assert.Equal(t, EventStoryDormant, events[0].Kind)
		assert.Equal(t, sid, events[0].StoryID)

		meta, err := tr.Story(sid)
		require.NoError(t, err)
		assert.Equal(t, StoryStateDormant, meta.State)
		assert.Equal(t, 0.3, meta.FrozenMeanDistance)
		assert.Equal(t, 0.1, meta.FrozenSigma)
	})

	t.Run("dormant_to_archived", func(t *testing.T) {
		sid := uuid.New()
		prev := storyRecord{
			State:              StoryStateDormant,
			Centroid:           []float32{1, 0},
			CreatedAt:          now.Add(-40 * 24 * time.Hour),
			LastSignalAt:       now.Add(-32 * 24 * time.Hour), // beyond ArchiveWindow
			FrozenMeanDistance: 0.3,
			FrozenSigma:        0.1,
		}
		var summary BatchSummary
		var events []StoryEvent[string]
		// Lifecycle is driven by membership now: an empty story is retired
		// rather than transitioned, so the subject needs a member whose age
		// puts it past the window under test.
		members := []*batchFacet{{id: uuid.New(), at: prev.LastSignalAt, emb: []float32{1, 0}}}
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tr.recentreStory(tx, sid, prev, true, members, &emaAccum{}, &summary, &events, now)
		}))

		require.Len(t, events, 1)
		assert.Equal(t, EventStoryArchived, events[0].Kind)
	})

	t.Run("dormant_stays_dormant_no_event", func(t *testing.T) {
		sid := uuid.New()
		prev := storyRecord{
			State:              StoryStateDormant,
			CreatedAt:          now.Add(-2 * time.Hour),
			LastSignalAt:       now.Add(-8 * 24 * time.Hour),
			FrozenMeanDistance: 0.3,
			FrozenSigma:        0.1,
		}
		var summary BatchSummary
		var events []StoryEvent[string]
		members := []*batchFacet{{id: uuid.New(), at: prev.LastSignalAt, emb: []float32{1, 0}}}
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tr.recentreStory(tx, sid, prev, true, members, &emaAccum{}, &summary, &events, now)
		}))
		assert.Empty(t, events)
	})
}

// TestCalcThreshold_ClampedToAssignThreshold covers the ceiling on the
// adaptive rule. Without it a story that has drifted wide keeps widening its
// own catchment, which is how one story ends up absorbing unrelated coverage.
func TestCalcThreshold_ClampedToAssignThreshold(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.AssignThreshold = 0.28
	tr.sigmaGlobal = 0.5

	wide := StoryMeta{
		State:        StoryStateActive,
		SignalCount:  100,
		MeanDistance: 0.9,
		Sigma:        0.4,
	}
	assert.InDelta(t, 0.28, tr.calcThreshold(wide), 1e-9,
		"an adaptive threshold above AssignThreshold must be clamped")

	tight := StoryMeta{
		State:        StoryStateActive,
		SignalCount:  100,
		MeanDistance: 0.05,
		Sigma:        0.01,
	}
	assert.Less(t, tr.calcThreshold(tight), 0.28,
		"a threshold below the ceiling must pass through unchanged")
}

func TestCollectBatch_OutlierEviction(t *testing.T) {
	tr := newTestTracker(t)
	tr.lastBatch = time.Now()
	tr.dim.Store(2)

	keep := Signal[string]{ID: uuid.New(), At: tr.lastBatch, Embeddings: []Embedding{[]float32{1, 0}}}
	expire := Signal[string]{ID: uuid.New(), At: tr.lastBatch.Add(-3 * tr.cfg.OutlierTTL), Embeddings: []Embedding{[]float32{1, 0}}}

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		for _, sig := range []Signal[string]{keep, expire} {
			if err := seedOutlier(tx, tr, sig); err != nil {
				return err
			}
		}
		return nil
	}))

	var signals []batchFacet
	var evict []uuid.UUID
	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		var err error
		signals, _, evict, _, _, err = tr.collectBatch(tx, time.Now())
		return err
	}))

	require.Len(t, evict, 1)
	assert.Equal(t, expire.ID, evict[0])
	require.Len(t, signals, 1)
	assert.True(t, signals[0].outlier)
	assert.Equal(t, keep.ID, signals[0].id)
}

func TestTracker_saveCalibState_RoundTrip(t *testing.T) {
	ms := newMemStore()
	tr := &Tracker[string]{cfg: Config[string]{Store: ms}}
	tr.dim.Store(4)
	tr.sigmaGlobal = 0.7
	tr.lastBatch = time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)

	require.NoError(t, ms.Update(func(tx Tx) error { return tr.saveCalibState(tx) }))

	var b []byte
	require.NoError(t, ms.View(func(tx Tx) error {
		var err error
		b, err = tx.Get(keys.CalibState())
		return err
	}))
	var s calibState
	require.NoError(t, cborStrictDecMode.Unmarshal(b, &s))
	assert.Equal(t, 4, s.Dim)
	assert.Equal(t, 0.7, s.SigmaGlobal)
	assert.True(t, s.LastBatchAt.Equal(tr.lastBatch))
}

// --- multi-facet draft placement (spec 007 §2.3.2) ---

// The point of the whole design: a signal whose facets point at different
// stories joins both, instead of averaging into the gap between them and
// landing in the outlier bucket.
func TestTracker_Ingest_FacetsReachDifferentStories(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	now := time.Now()
	storyX := uuid.NewSHA1(TrackerNamespace, []byte("facet-story-x"))
	storyY := uuid.NewSHA1(TrackerNamespace, []byte("facet-story-y"))
	seedStory(t, tr, storyX, storyRecord{
		State: StoryStateActive, Centroid: []float32{1, 0},
		CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-time.Hour),
	})
	seedStory(t, tr, storyY, storyRecord{
		State: StoryStateActive, Centroid: []float32{0, 1},
		CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-time.Hour),
	})

	sig := Signal[string]{
		ID: uuid.New(), At: now,
		Embeddings: []Embedding{{0.99, 0.01}, {0.01, 0.99}},
		Data:       "two-subjects",
	}
	assigned, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, storyIDSet(storyX, storyY), assigned,
		"a two-facet signal must join both stories its facets match")

	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		assert.NotNil(t, mustGet(t, tx, keys.FacetMember(storyX, sig.ID, 0)))
		assert.NotNil(t, mustGet(t, tx, keys.FacetMember(storyY, sig.ID, 1)))
		// Each facet belongs to exactly one story (invariant 1).
		assert.Nil(t, mustGet(t, tx, keys.FacetMember(storyY, sig.ID, 0)))
		assert.Nil(t, mustGet(t, tx, keys.FacetMember(storyX, sig.ID, 1)))
		return nil
	}))
}

// The averaged single vector this design replaces would fall between the two
// centroids and match neither. Ingested as one facet, it is orphaned; ingested
// as two facets, it is placed. Same input, both ways, one assertion.
func TestTracker_Ingest_AveragedVectorOrphansButFacetsPlace(t *testing.T) {
	// Two orthogonal centroids. Their bisector sits 0.293 from each, so an
	// assignment radius below that is exactly the geometry where an averaged
	// vector falls between two stories and into neither.
	newSeeded := func() *Tracker[string] {
		tr, err := NewTracker[string](Config[string]{
			Store:           newMemStore(),
			BatchInterval:   time.Hour,
			AssignThreshold: 0.20,
			MergeThreshold:  0.10,
			SplitThreshold:  0.15,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })
		tr.dim.Store(2)

		now := time.Now()
		for name, c := range map[string][]float32{"a": {1, 0}, "b": {0, 1}} {
			seedStory(t, tr, uuid.NewSHA1(TrackerNamespace, []byte("orphan-"+name)), storyRecord{
				State: StoryStateActive, Centroid: c,
				CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-time.Hour),
			})
		}
		return tr
	}

	// One averaged facet: equidistant from both centroids, inside neither.
	single := newSeeded()
	got, err := single.Ingest(context.Background(), Signal[string]{
		ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{{0.707, 0.707}},
	})
	require.NoError(t, err)
	assert.Empty(t, got, "the averaged vector matches no story: this is the orphan case")

	// The same item, decomposed.
	split := newSeeded()
	got, err = split.Ingest(context.Background(), Signal[string]{
		ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{{1, 0}, {0, 1}},
	})
	require.NoError(t, err)
	assert.Len(t, got, 2, "decomposed into facets, the same item reaches both stories")
}

// Several facets landing in one story is still one signal joining one story,
// so exactly one event is emitted — not one per facet.
func TestTracker_Ingest_EmitsOneEventPerStoryNotPerFacet(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	now := time.Now()
	storyID := uuid.NewSHA1(TrackerNamespace, []byte("dedupe-events"))
	seedStory(t, tr, storyID, storyRecord{
		State: StoryStateActive, Centroid: []float32{1, 0},
		CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-time.Hour),
	})

	ch := tr.Subscribe()
	assigned, err := tr.Ingest(context.Background(), Signal[string]{
		ID: uuid.New(), At: now,
		Embeddings: []Embedding{{1, 0}, {0.99, 0.01}, {0.98, 0.02}},
	})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{storyID}, assigned)

	select {
	case ev := <-ch:
		assert.Equal(t, EventDraftAssigned, ev.Kind)
		assert.Equal(t, storyID, ev.StoryID)
	case <-time.After(time.Second):
		t.Fatal("no draft-assigned event")
	}
	select {
	case ev := <-ch:
		t.Fatalf("three facets in one story must emit one event, got a second: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

// A signal can be partly placed: the facets that match are placed, the rest
// wait in the outlier bucket. Before facets this was all-or-nothing.
func TestTracker_Ingest_PartiallyPlacedSignal(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	now := time.Now()
	storyID := uuid.NewSHA1(TrackerNamespace, []byte("partial"))
	seedStory(t, tr, storyID, storyRecord{
		State: StoryStateActive, Centroid: []float32{1, 0},
		CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-time.Hour),
	})

	sig := Signal[string]{
		ID: uuid.New(), At: now,
		Embeddings: []Embedding{{1, 0}, {-1, 0}}, // second faces the other way
	}
	assigned, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyID}, assigned)

	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		assert.NotNil(t, mustGet(t, tx, keys.FacetMember(storyID, sig.ID, 0)), "matching facet is placed")
		assert.NotNil(t, mustGet(t, tx, keys.OutlierFacet(sig.ID, 1)), "non-matching facet waits as an outlier")

		locs, hasIndex, err := readSignalLocSet(tx, sig.ID)
		require.NoError(t, err)
		require.True(t, hasIndex)
		assert.Equal(t, []keys.FacetLoc{{StoryID: storyID}, {IsOutlier: true}}, locs)
		return nil
	}))
}

// Re-ingest is a no-op at the signal level once any facet is placed: a late
// duplicate must not partially overwrite what a batch run decided.
func TestTracker_Ingest_ReingestNoOpWhenAnyFacetPlaced(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	now := time.Now()
	storyID := uuid.NewSHA1(TrackerNamespace, []byte("partial-noop"))
	seedStory(t, tr, storyID, storyRecord{
		State: StoryStateActive, Centroid: []float32{1, 0},
		CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-time.Hour),
	})

	sig := Signal[string]{
		ID: uuid.New(), At: now,
		Embeddings: []Embedding{{1, 0}, {-1, 0}},
	}
	first, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)

	ch := tr.Subscribe()
	second, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)
	assert.Equal(t, first, second, "re-ingest must report the same placement")

	select {
	case ev := <-ch:
		t.Fatalf("re-ingest of a placed signal must emit nothing, got: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

// Facets must agree on dimensionality with each other, not only with the corpus.
func TestTracker_Ingest_RejectsRaggedFacets(t *testing.T) {
	tr := newTestTracker(t)
	_, err := tr.Ingest(context.Background(), Signal[string]{
		ID: uuid.New(), At: time.Now(),
		Embeddings: []Embedding{{1, 0}, {1, 0, 0}},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDimensionMismatch)
}

func TestTracker_Ingest_RejectsNoFacets(t *testing.T) {
	tr := newTestTracker(t)
	_, err := tr.Ingest(context.Background(), Signal[string]{ID: uuid.New(), At: time.Now()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one facet")
}
