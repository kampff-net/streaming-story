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

// The four stability invariants of spec 006 §2.1 as executable tests. Each
// one is a property the previous HDBSCAN pipeline violated, so a regression
// here means the design has been undone rather than merely changed.

// storySnapshot captures which signals belong to which story, for comparing
// the store across runs.
func storySnapshot(t *testing.T, tr *Tracker[string]) map[uuid.UUID][]uuid.UUID {
	t.Helper()
	out := make(map[uuid.UUID][]uuid.UUID)
	for meta := range tr.Stories(StoryStateAny) {
		var ids []uuid.UUID
		for sig, err := range tr.SignalsOf(meta.ID) {
			require.NoError(t, err)
			ids = append(ids, sig.ID)
		}
		out[meta.ID] = ids
	}
	return out
}

func ingestAt(t *testing.T, tr *Tracker[string], name string, angle float64, at time.Time) uuid.UUID {
	t.Helper()
	id := uuid.NewSHA1(TrackerNamespace, []byte(name))
	_, err := tr.Ingest(context.Background(), Signal[string]{
		ID: id, At: at, Embedding: unitAt(angle), Data: name,
	})
	require.NoError(t, err)
	return id
}

// Invariant 3: deterministic. The same store state must yield the same
// outcome, so a second run over unchanged data changes nothing.
func TestStability_IdempotentRerun(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	now := time.Now()
	for i := range 6 {
		ingestAt(t, tr, fmt.Sprintf("idem-a-%d", i), 0.0+float64(i)*0.01, now)
	}
	for i := range 6 {
		ingestAt(t, tr, fmt.Sprintf("idem-b-%d", i), 2.0+float64(i)*0.01, now)
	}

	tr.runBatch()
	first := storySnapshot(t, tr)
	require.NotEmpty(t, first)

	tr.runBatch()
	second := storySnapshot(t, tr)

	assert.Equal(t, len(first), len(second), "a second run must not change the story count")
	for id, sigs := range first {
		assert.ElementsMatch(t, sigs, second[id], "story %s changed membership on a no-op run", id)
	}
}

// Invariant 2: local. Signals arriving for one story must not move another
// story's membership. The old global density landscape made this impossible.
func TestStability_NewSignalsDoNotDisturbOtherStories(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)
	now := time.Now()

	for i := range 6 {
		ingestAt(t, tr, fmt.Sprintf("local-x-%d", i), 0.0+float64(i)*0.01, now)
	}
	tr.runBatch()

	before := storySnapshot(t, tr)
	require.Len(t, before, 1)
	var storyX uuid.UUID
	for id := range before {
		storyX = id
	}

	// A completely unrelated burst, far from story X.
	for i := range 6 {
		ingestAt(t, tr, fmt.Sprintf("local-y-%d", i), 2.5+float64(i)*0.01, now)
	}
	tr.runBatch()

	after := storySnapshot(t, tr)
	assert.ElementsMatch(t, before[storyX], after[storyX],
		"story X membership changed because unrelated signals arrived")
}

// Invariant 4: non-chaining. A ladder of signals, each near its neighbour but
// spanning far more than any threshold end to end, must not collapse into one
// story. This is the exact shape that produced a 324-signal component under
// transitive linkage.
func TestStability_ChainedCorpusProducesNoBlob(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)
	now := time.Now()

	const n = 40
	for i := range n {
		ingestAt(t, tr, fmt.Sprintf("chain-%d", i), float64(i)*0.06, now)
	}
	tr.runBatch()

	snap := storySnapshot(t, tr)
	// Guard against passing vacuously: the assertion below is only meaningful
	// if the corpus actually formed stories.
	require.GreaterOrEqual(t, len(snap), 2, "chain produced no separable stories")
	for id, sigs := range snap {
		assert.Less(t, len(sigs), n/2,
			"story %s swallowed %d of %d signals: the chain collapsed", id, len(sigs), n)
	}
}

// Invariant 1: bounded revision. Membership changes only through split or
// merge, and a settled corpus must stop changing. Fifty consecutive runs over
// unchanging data must not produce a split/merge cycle -- the failure mode the
// hysteresis band exists to prevent.
func TestStability_NoSplitMergeCycle(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)
	now := time.Now()
	ch := tr.Subscribe()

	// Three groups placed so that some pairs sit near the band edges.
	for g, center := range []float64{0.0, 0.35, 1.4} {
		for i := range 6 {
			ingestAt(t, tr, fmt.Sprintf("cycle-%d-%d", g, i), center+float64(i)*0.012, now)
		}
	}

	// Settle.
	tr.runBatch()
	drain(ch)

	baseline := storySnapshot(t, tr)
	require.NotEmpty(t, baseline, "nothing settled, so drift cannot be observed")
	var splits, merges int
	for range 50 {
		tr.runBatch()
		for _, ev := range drain(ch) {
			switch ev.Kind {
			case EventStorySplit:
				splits++
			case EventStoryMerged:
				merges++
			}
		}
	}

	assert.Zero(t, splits, "a settled corpus split again")
	assert.Zero(t, merges, "a settled corpus merged again")

	final := storySnapshot(t, tr)
	assert.Equal(t, len(baseline), len(final), "story count drifted over 50 runs")
	for id, sigs := range baseline {
		assert.ElementsMatch(t, sigs, final[id], "story %s drifted over 50 runs", id)
	}
}

// drain collects everything currently buffered on the subscription without
// blocking.
func drain(ch <-chan StoryEvent[string]) []StoryEvent[string] {
	var out []StoryEvent[string]
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}
