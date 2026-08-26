package story

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The stability invariants in stability_test.go run on synthetic fixtures where
// every signal arrives at once. This file runs the case the library actually
// serves: a seed corpus, then arrivals in small increments, with a maintenance
// pass after each. Requires a real corpus, so it is gated on CORPUS.

// membership maps each assigned signal to the story holding it.
func membership(t *testing.T, tr *Tracker[string]) map[uuid.UUID]uuid.UUID {
	t.Helper()
	out := make(map[uuid.UUID]uuid.UUID)
	for meta, err := range tr.Stories(StoryStateAny) {
		require.NoError(t, err)
		for sig, err := range tr.SignalsOf(meta.ID) {
			require.NoError(t, err)
			out[sig.ID] = meta.ID
		}
	}
	return out
}

// churn counts the signals that were assigned in before and sit under a
// different story in after. Signals newly assigned, or dropped, are not churn:
// only a signal whose story changed underneath it is.
func churn(before, after map[uuid.UUID]uuid.UUID) (moved, carried int) {
	for sig, story := range before {
		next, ok := after[sig]
		if !ok {
			continue
		}
		carried++
		if next != story {
			moved++
		}
	}
	return moved, carried
}

// TestStreaming_IncrementalArrivalsAreStable feeds a 300-signal seed and then
// 50 signals at a time, checking after every increment that existing stories
// keep their signals and that no story swallows the corpus.
//
// This is where outlier admission earns its place: a signal arriving before the
// batch that creates its story lands in the outlier bucket, and only the
// maintenance pass can place it. Without admission the bucket grows without
// bound while stories stay frozen at their seed membership.
func TestStreaming_IncrementalArrivalsAreStable(t *testing.T) {
	path := os.Getenv("CORPUS")
	if path == "" {
		t.Skip("set CORPUS")
	}
	pts := loadCorpus(t, path)
	require.Greater(t, len(pts), 450, "corpus too small for a streaming run")

	// Several arrival shapes, because the shape is the thing under test: a
	// larger seed gives the first pass more structure to find, and a smaller
	// increment gives each later pass less evidence to act on.
	for _, tc := range []struct{ seed, step int }{
		{300, 50},
		{400, 50},
		{400, 20},
		{400, 5},
	} {
		t.Run(fmt.Sprintf("seed%d_step%d", tc.seed, tc.step), func(t *testing.T) {
			streamCorpus(t, pts, tc.seed, tc.step)
		})
	}
}

func streamCorpus(t *testing.T, pts [][]float32, seed, step int) {
	tr := newTestTracker(t)
	tr.dim.Store(int32(len(pts[0])))
	now := time.Now()

	ingest := func(from, to int) {
		for i := from; i < to && i < len(pts); i++ {
			id := uuid.NewSHA1(TrackerNamespace, []byte(fmt.Sprintf("corpus-%d", i)))
			_, err := tr.Ingest(context.Background(), Signal[string]{
				ID: id, At: now, Embeddings: []Embedding{pts[i]}, Data: fmt.Sprintf("s%d", i),
			})
			require.NoError(t, err)
		}
	}

	ingest(0, seed)
	tr.runBatchCore()

	prev := membership(t, tr)
	require.NotEmpty(t, prev, "seed produced no stories")
	seedStories := len(distinct(prev))

	totalMoved, totalCarried := 0, 0
	for from := seed; from < len(pts); from += step {
		ingest(from, from+step)
		summary := tr.runBatchCore()
		next := membership(t, tr)

		moved, carried := churn(prev, next)
		totalMoved += moved
		totalCarried += carried

		sizes := storySizes(next)
		t.Logf("after %3d signals: stories=%2d assigned=%3d largest=%3d moved=%2d/%3d promo=%d adm=%d spl=%d mrg=%d",
			min(from+step, len(pts)), len(sizes), len(next), first(sizes), moved, carried,
			summary.OutliersPromoted, summary.OutliersAdmitted,
			summary.StoriesSplit, summary.StoriesMerged)

		// A single increment must not reshuffle the corpus. Some movement is
		// legitimate -- a merge or split is exactly how a story revises itself --
		// but a quarter of the population changing hands means the geometry is
		// not converging.
		assert.LessOrEqual(t, float64(moved), 0.25*float64(carried),
			"increment at %d moved %d of %d already-assigned signals", from, moved, carried)

		// No story may swallow the corpus: the chaining failure mode shows up
		// here first, as one story growing without bound.
		assert.Less(t, first(sizes), len(next)/2,
			"largest story holds %d of %d assigned signals", first(sizes), len(next))

		prev = next
	}

	// Stories accumulate as new topics arrive; they must not evaporate into a
	// handful of blobs.
	final := len(distinct(prev))
	assert.GreaterOrEqual(t, final, seedStories/2,
		"story count fell from %d to %d over the stream", seedStories, final)

	t.Logf("total churn over stream: %d of %d carried assignments (%.1f%%)",
		totalMoved, totalCarried, 100*float64(totalMoved)/float64(max(totalCarried, 1)))

	// A store settles, but not necessarily on the pass that follows an arrival.
	// Promotion creates stories and moves sigma_global, so outliers that were
	// not covered when that pass began get their chance against the new stories
	// on the next one. What must hold is that the process converges quickly and
	// then stops dead: absorption only ever adds assignments, and no pass after
	// the fixpoint changes anything at all.
	const maxPasses = 4
	passes := 0
	for {
		before := membership(t, tr)
		tr.runBatchCore()
		after := membership(t, tr)

		moved, _ := churn(before, after)
		assert.Zero(t, moved, "a settling pass moved a signal between stories")
		assert.GreaterOrEqual(t, len(after), len(before),
			"a settling pass unassigned signals: %d -> %d", len(before), len(after))

		if len(after) == len(before) {
			break
		}
		passes++
		require.LessOrEqual(t, passes, maxPasses,
			"store still absorbing outliers after %d extra passes (%d assigned)",
			passes, len(after))
	}
	t.Logf("settled after %d extra pass(es)", passes)

	// Past the fixpoint, nothing moves at all, however many passes run.
	settled := membership(t, tr)
	for range 3 {
		tr.runBatchCore()
		again := membership(t, tr)
		moved, _ := churn(settled, again)
		assert.Zero(t, moved, "a pass past the fixpoint moved signals")
		assert.Equal(t, len(settled), len(again), "a pass past the fixpoint changed the assigned count")
	}
}

// distinct returns the set of story IDs in a membership map.
func distinct(m map[uuid.UUID]uuid.UUID) map[uuid.UUID]struct{} {
	out := make(map[uuid.UUID]struct{})
	for _, story := range m {
		out[story] = struct{}{}
	}
	return out
}

// storySizes returns story sizes, largest first.
func storySizes(m map[uuid.UUID]uuid.UUID) []int {
	counts := make(map[uuid.UUID]int)
	for _, story := range m {
		counts[story]++
	}
	out := make([]int, 0, len(counts))
	for _, n := range counts {
		out = append(out, n)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(out)))
	return out
}

// first returns the first element, or 0 for an empty slice.
func first(s []int) int {
	if len(s) == 0 {
		return 0
	}
	return s[0]
}
