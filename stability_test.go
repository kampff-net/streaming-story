package story

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
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
	for meta, err := range tr.Stories(StoryStateAny) {
		require.NoError(t, err)
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
		ID: id, At: at, Embeddings: []Embedding{unitAt(angle)}, Data: name,
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

// --- spec 007 controlling tests ---

// corpusSnapshot renders the settled story structure as sorted text, so two
// runs can be compared byte for byte.
func corpusSnapshot(t *testing.T, tr *Tracker[string]) string {
	t.Helper()
	var lines []string
	for meta, err := range tr.Stories(StoryStateAny) {
		require.NoError(t, err)
		var ids []string
		for sig, err := range tr.SignalsOf(meta.ID) {
			require.NoError(t, err)
			ids = append(ids, sig.ID.String())
		}
		sort.Strings(ids)
		lines = append(lines, fmt.Sprintf("%s %v", meta.ID, ids))
	}
	sort.Strings(lines)
	out := fmt.Sprintf("%d stories\n", len(lines))
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}

// runCorpus ingests the reference corpus at the given facet count and settles
// it. facets=1 reproduces the pre-facet single-vector path.
func runCorpus(t *testing.T, path string, facets int) *Tracker[string] {
	t.Helper()
	pts := loadCorpus(t, path)
	require.NotEmpty(t, pts)

	tr := newTestTracker(t)
	tr.dim.Store(int32(len(pts[0])))

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, emb := range pts {
		id := uuid.NewSHA1(TrackerNamespace, []byte(fmt.Sprintf("corpus-%d", i)))
		embs := make([]Embedding, facets)
		for f := range facets {
			embs[f] = emb
		}
		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID: id, At: base.Add(time.Duration(i) * time.Second),
			Embeddings: embs, Data: fmt.Sprintf("s%d", i),
		})
		require.NoError(t, err)
	}
	for range 3 {
		tr.runBatch()
	}
	return tr
}

// membershipsOf reduces a snapshot to the set of member lists, discarding the
// story IDs that hold them.
func membershipsOf(snapshot string) []string {
	var out []string
	for _, line := range strings.Split(snapshot, "\n") {
		if i := strings.Index(line, " ["); i >= 0 {
			out = append(out, line[i+1:])
		}
	}
	sort.Strings(out)
	return out
}

// The controlling backward-compatibility test: at one facet per signal, the
// facet-granular tracker must reproduce the spec 006 *clustering* exactly —
// the same number of stories holding the same members. REF006 names a snapshot
// captured from the pre-facet code over the same corpus; the committed copy is
// testdata/spec006_corpus_snapshot.txt.
//
// Story IDs are deliberately excluded from the comparison, and this is the one
// place the change is not backward-compatible. deriveStoryID now folds each
// founding facet's index into the derived name, because without it two
// different facet sets drawn from the same signals would derive the same ID and
// a split would silently fold into its sibling. Changing the derivation input
// changes every derived ID exactly once. Nothing is lost by it: IDs stay a
// deterministic function of the input stream, so a replay still reproduces
// them, and spec 007 §2.5 already rebuilds existing stores rather than
// migrating them in place.
func TestStability_SingleFacetMatchesSpec006(t *testing.T) {
	corpus, ref := os.Getenv("CORPUS"), os.Getenv("REF006")
	if corpus == "" || ref == "" {
		t.Skip("set CORPUS and REF006")
	}
	want, err := os.ReadFile(ref)
	require.NoError(t, err)

	got := corpusSnapshot(t, runCorpus(t, corpus, 1))

	wantLines, gotLines := strings.SplitN(string(want), "\n", 2), strings.SplitN(got, "\n", 2)
	assert.Equal(t, wantLines[0], gotLines[0], "story count must match spec 006")
	assert.Equal(t, membershipsOf(string(want)), membershipsOf(got),
		"a one-facet corpus must cluster identically to spec 006")
}

// The dump round-trip: Signals() replayed into a fresh store must reproduce the
// same story IDs and the same facet-level membership. Run at more than one
// facet, since a single-facet round-trip does not exercise facet identity.
func TestStability_DumpRoundTrip(t *testing.T) {
	for _, facets := range []int{1, 3} {
		t.Run(fmt.Sprintf("facets_%d", facets), func(t *testing.T) {
			tr := newTestTracker(t)
			tr.dim.Store(2)

			now := time.Now()
			for i := range 24 {
				embs := make([]Embedding, facets)
				for f := range facets {
					embs[f] = unitAt(float64(f)*1.2 + float64(i)*0.01)
				}
				_, err := tr.Ingest(context.Background(), Signal[string]{
					ID:         uuid.NewSHA1(TrackerNamespace, []byte(fmt.Sprintf("rt-%d", i))),
					At:         now.Add(-time.Duration(i) * time.Second),
					Embeddings: embs,
					Data:       fmt.Sprintf("rt-%d", i),
				})
				require.NoError(t, err)
			}
			for range 3 {
				tr.runBatch()
			}

			// Dump, then replay into a fresh store on the same terms.
			var dump []Signal[string]
			for sig, err := range tr.Signals() {
				require.NoError(t, err)
				dump = append(dump, sig)
			}
			require.Len(t, dump, 24, "the dump must carry every signal")

			replay := newTestTracker(t)
			replay.dim.Store(2)
			for _, sig := range dump {
				_, err := replay.Ingest(context.Background(), sig)
				require.NoError(t, err)
			}
			for range 3 {
				replay.runBatch()
			}

			assert.Equal(t, corpusSnapshot(t, tr), corpusSnapshot(t, replay),
				"a replayed dump must reproduce the same stories and membership")

			// Facet-level membership, not just signal-level.
			for _, sig := range dump {
				a, err := tr.FacetsOfSignal(sig.ID)
				require.NoError(t, err)
				b, err := replay.FacetsOfSignal(sig.ID)
				require.NoError(t, err)
				assert.Equalf(t, a, b, "facet placement of %s must survive the round trip", sig.ID)
			}
		})
	}
}
