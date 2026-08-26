package story

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kvsh.ch/streaming-story/internal/keys"
)

// TestApplyMaintenance_SweeperPreservesSuppressed covers spec 009's
// maintenance sweeper invariant: a suppressed story with recent signal
// activity must not be reset to Active the way an ordinary story would.
func TestApplyMaintenance_SweeperPreservesSuppressed(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(3)
	now := time.Now()

	storyID := uuid.NewSHA1(TrackerNamespace, []byte("sweeper-suppressed"))
	embs := [][]float32{{1.00, 0.01, 0.02}, {0.99, 0.02, -0.01}, {1.01, -0.01, 0.01}}

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		for i, emb := range embs {
			sigID := uuid.NewSHA1(storyID, []byte(fmt.Sprintf("sig-%d", i)))
			sig := Signal[string]{ID: sigID, At: now.Add(-time.Minute), Embeddings: []Embedding{emb}}
			if err := seedMember(tx, tr, storyID, sig); err != nil {
				return err
			}
		}
		return tr.writeStoryMeta(tx, storyID, time.Time{}, storyRecord{
			State: StoryStateSuppressed, WasSuppressed: true, SuppressionReason: "spam channel",
			Centroid: []float32{1, 0, 0}, CreatedAt: now.Add(-time.Hour), LastSignalAt: now.Add(-time.Minute),
		})
	}))

	tr.runBatch()

	meta, err := tr.Story(storyID)
	require.NoError(t, err)
	assert.Equal(t, StoryStateSuppressed, meta.State, "recent signal activity must not clear suppression")
	assert.Equal(t, "spam channel", meta.SuppressionReason)
}

// TestApplyMaintenance_MergeUnsuppressesSurvivor covers spec 009's one
// exception to "suppressed stays suppressed": a merge is a stronger signal
// than an ordinary new member, so it does unsuppress.
func TestApplyMaintenance_MergeUnsuppressesSurvivor(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(3)
	ch := tr.Subscribe()
	now := time.Now()

	// Same geometry as TestRunBatch_Merge: two tight, overlapping clusters
	// that the default MergeThreshold folds into one. A is older and
	// survives; B starts Suppressed.
	storyA := uuid.NewSHA1(TrackerNamespace, []byte("merge-suppressed-A"))
	storyB := uuid.NewSHA1(TrackerNamespace, []byte("merge-suppressed-B"))
	aEmbs := [][]float32{
		{1.00, 0.01, 0.02}, {0.99, 0.02, -0.01}, {1.01, -0.01, 0.01}, {0.98, 0.03, 0.00}, {1.00, 0.00, 0.00},
	}
	bEmbs := [][]float32{
		{1.01, 0.00, 0.01}, {0.99, 0.01, 0.00}, {1.00, -0.01, 0.02},
	}

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		write := func(sid uuid.UUID, created time.Time, embs [][]float32, rec storyRecord) error {
			for i, emb := range embs {
				sigID := uuid.NewSHA1(sid, []byte(fmt.Sprintf("sig-%d", i)))
				sig := Signal[string]{ID: sigID, At: now.Add(-time.Minute), Embeddings: []Embedding{emb}}
				if err := seedMember(tx, tr, sid, sig); err != nil {
					return err
				}
			}
			rec.Centroid = []float32{1, 0, 0}
			rec.CreatedAt = created
			rec.LastSignalAt = now.Add(-time.Minute)
			return tr.writeStoryMeta(tx, sid, time.Time{}, rec)
		}
		if err := write(storyA, now.Add(-2*time.Hour), aEmbs, storyRecord{State: StoryStateActive}); err != nil {
			return err
		}
		return write(storyB, now.Add(-time.Hour), bEmbs, storyRecord{
			State: StoryStateSuppressed, WasSuppressed: true, SuppressionReason: "spam channel",
		})
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
	require.GreaterOrEqual(t, summary.StoriesMerged, 1)

	assert.Nil(t, mustGet(t, tr.cfg.Store, keys.StoryMeta(storyB)), "retired story B metadata must be deleted")

	meta, err := tr.Story(storyA)
	require.NoError(t, err)
	assert.Equal(t, StoryStateActive, meta.State, "merge must unsuppress the survivor")
	assert.True(t, meta.WasSuppressed, "WasSuppressed is historical and must carry over from the retired side")
	assert.Equal(t, "spam channel", meta.SuppressionReason, "reason carries over from the suppressed side when the survivor has none")
}

// TestApplyMaintenance_AdmissionIntoSuppressedStoryEmitsEvent covers spec
// 009's batch-side counterpart to the Draft phase: an outlier admitted into
// an existing suppressed story stays suppressed and emits
// EventSuppressedStorySignal alongside the ordinary reassignment event.
func TestApplyMaintenance_AdmissionIntoSuppressedStoryEmitsEvent(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(3)
	ch := tr.Subscribe()
	now := time.Now()

	storyID := uuid.NewSHA1(TrackerNamespace, []byte("admit-suppressed"))
	memberEmbs := [][]float32{{1.00, 0.01, 0.02}, {0.99, 0.02, -0.01}, {1.01, -0.01, 0.01}}
	var outlierID uuid.UUID

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		for i, emb := range memberEmbs {
			sigID := uuid.NewSHA1(storyID, []byte(fmt.Sprintf("sig-%d", i)))
			sig := Signal[string]{ID: sigID, At: now.Add(-time.Minute), Embeddings: []Embedding{emb}}
			if err := seedMember(tx, tr, storyID, sig); err != nil {
				return err
			}
		}
		if err := tr.writeStoryMeta(tx, storyID, time.Time{}, storyRecord{
			State: StoryStateSuppressed, WasSuppressed: true, SuppressionReason: "spam channel",
			Centroid: []float32{1, 0, 0}, CreatedAt: now.Add(-time.Hour), LastSignalAt: now.Add(-time.Minute),
		}); err != nil {
			return err
		}

		outlierID = uuid.New()
		outlier := Signal[string]{ID: outlierID, At: now.Add(-time.Minute), Embeddings: []Embedding{{1.00, 0.00, 0.03}}}
		return seedOutlier(tx, tr, outlier)
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
	assert.Equal(t, 1, summary.OutliersAdmitted)

	var sawReassigned, sawSuppressedSignal bool
	for _, ev := range events {
		if ev.StoryID != storyID || ev.SignalID != outlierID {
			continue
		}
		switch ev.Kind {
		case EventSignalReassigned:
			sawReassigned = true
		case EventSuppressedStorySignal:
			sawSuppressedSignal = true
		}
	}
	assert.True(t, sawReassigned, "admission must still emit the ordinary reassignment event")
	assert.True(t, sawSuppressedSignal, "admission into a suppressed story must emit EventSuppressedStorySignal")

	meta, err := tr.Story(storyID)
	require.NoError(t, err)
	assert.Equal(t, StoryStateSuppressed, meta.State, "admitting a new member must not reactivate the story")
}
