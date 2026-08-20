package story

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kvsh.ch/streaming-story/internal/keys"
)

// The read API: the canonical signal record and the whole-corpus dump.

// collectSignals drains a Signals iterator, failing on the first error.
func collectSignals(t *testing.T, tr *Tracker[string]) []Signal[string] {
	t.Helper()
	var out []Signal[string]
	for sig, err := range tr.Signals() {
		require.NoError(t, err)
		out = append(out, sig)
	}
	return out
}

// ingestN feeds n signals along a tight arc so some land in stories and the
// rest stay in the outlier bucket, then returns their IDs in ingest order.
func ingestN(t *testing.T, tr *Tracker[string], n int) []uuid.UUID {
	t.Helper()
	now := time.Now()
	ids := make([]uuid.UUID, n)
	for i := range n {
		ids[i] = uuid.NewSHA1(TrackerNamespace, []byte(string(rune('a'+i))))
		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID:         ids[i],
			At:         now.Add(-time.Duration(i) * time.Second),
			Embeddings: []Embedding{unitAt(float64(i) * 0.01)},
			Data:       string(rune('a' + i)),
		})
		require.NoError(t, err)
	}
	return ids
}

// Every ingested signal is reachable through Signals, whether or not a story
// claimed it. This is the property the dump path in spec 007 §2.5 rests on.
func TestSignals_YieldsEveryIngestedSignalOnce(t *testing.T) {
	tr := newTestTracker(t)
	ids := ingestN(t, tr, 8)

	got := collectSignals(t, tr)
	require.Len(t, got, len(ids))

	gotIDs := make([]uuid.UUID, len(got))
	for i, sig := range got {
		gotIDs[i] = sig.ID
	}
	assert.ElementsMatch(t, ids, gotIDs)
}

// A batch run places some signals and leaves others as outliers. Neither state
// may hide a signal from the dump.
func TestSignals_CoversPlacedAndOutlierSignals(t *testing.T) {
	tr := newTestTracker(t)
	ids := ingestN(t, tr, 8)
	tr.runBatch()

	assert.Len(t, collectSignals(t, tr), len(ids),
		"a batch run must not change how many signals the dump yields")
}

// The dump is lossless: every field Ingest was given comes back.
func TestSignals_RecordIsComplete(t *testing.T) {
	tr := newTestTracker(t)
	id := uuid.New()
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	in := Signal[string]{
		ID:         id,
		At:         at,
		Embeddings: []Embedding{{0.6, 0.8}},
		Data:       "payload",
	}
	_, err := tr.Ingest(context.Background(), in)
	require.NoError(t, err)

	got := collectSignals(t, tr)
	require.Len(t, got, 1)
	assert.Equal(t, in.ID, got[0].ID)
	assert.True(t, in.At.Equal(got[0].At))
	assert.Equal(t, in.Data, got[0].Data)
	assert.Equal(t, in.Embeddings, got[0].Embeddings)
}

// Signal IDs are UUIDs rendered as hex, so the canonical key space scans in ID
// order. A dump that replays in a stable order is what makes a rebuild
// reproducible.
func TestSignals_YieldsInSignalIDOrder(t *testing.T) {
	tr := newTestTracker(t)
	ingestN(t, tr, 8)

	got := collectSignals(t, tr)
	ids := make([]string, len(got))
	for i, sig := range got {
		ids[i] = sig.ID.String()
	}
	assert.True(t, sort.StringsAreSorted(ids), "dump must be ordered by signal ID: %v", ids)
}

// Re-ingesting a signal must not duplicate its canonical record.
func TestSignals_ReIngestDoesNotDuplicate(t *testing.T) {
	tr := newTestTracker(t)
	sig := Signal[string]{
		ID:         uuid.New(),
		At:         time.Now(),
		Embeddings: []Embedding{{1, 0}},
		Data:       "once",
	}
	for range 3 {
		_, err := tr.Ingest(context.Background(), sig)
		require.NoError(t, err)
	}
	assert.Len(t, collectSignals(t, tr), 1)
}

func TestSignals_EmptyStore(t *testing.T) {
	assert.Empty(t, collectSignals(t, newTestTracker(t)))
}

// Signal reads the canonical record, so an outlier is found just as a member
// is. Before spec 007 this needed the location index to say which of the two.
func TestSignal_FindsOutlierAndMemberAlike(t *testing.T) {
	tr := newTestTracker(t)
	ids := ingestN(t, tr, 8)
	tr.runBatch()

	for _, id := range ids {
		got, err := tr.Signal(id)
		require.NoErrorf(t, err, "signal %s must be readable regardless of placement", id)
		assert.Equal(t, id, got.ID)
	}
}

// --- store invariants ---

// storeInvariants asserts the two-way referential integrity spec 007 §2.3.1
// requires: every canonical record is referenced by at least one facet, and
// every facet marker has a record behind it. A violation either leaks storage
// or hides a signal from the dump.
func storeInvariants(t *testing.T, tr *Tracker[string]) {
	t.Helper()

	type facetRef struct {
		signal uuid.UUID
		facet  int
	}
	records := map[uuid.UUID]int{}    // signal -> facet count in its record
	referenced := map[uuid.UUID]int{} // signal -> markers pointing at it
	owner := map[facetRef]uuid.UUID{} // facet -> the one story holding it

	require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
		if err := tx.ScanPrefix(keys.CanonicalPrefix(), func(key, val []byte) error {
			id, ok := keys.ParseCanonicalSignal(key)
			if !ok {
				return nil
			}
			var sig Signal[string]
			err := cborDecMode.Unmarshal(val, &sig)
			require.NoError(t, err)
			records[id] = len(sig.Embeddings)
			return nil
		}); err != nil {
			return err
		}

		// Facet markers under every story.
		for meta := range tr.Stories(StoryStateAny) {
			prefix := keys.FacetPrefix(meta.ID)
			if err := tx.ScanPrefix(prefix, func(key, _ []byte) error {
				sigID, facet, ok := keys.ParseFacetMember(key, prefix)
				if !ok {
					return nil
				}
				referenced[sigID]++
				n, has := records[sigID]
				assert.Truef(t, has, "facet marker for signal %s has no canonical record", sigID)
				assert.Lessf(t, facet, n, "facet %d of signal %s is past the record's facet count", facet, sigID)

				// Invariant 1: a facet belongs to at most one story.
				ref := facetRef{sigID, facet}
				if prev, dup := owner[ref]; dup {
					t.Errorf("facet %d of signal %s is in two stories: %s and %s", facet, sigID, prev, meta.ID)
				}
				owner[ref] = meta.ID
				return nil
			}); err != nil {
				return err
			}
		}

		// Outlier markers.
		return tx.ScanPrefix(keys.OutlierPrefix(), func(key, _ []byte) error {
			sigID, facet, ok := keys.ParseOutlierFacet(key)
			if !ok {
				return nil
			}
			referenced[sigID]++
			n, has := records[sigID]
			assert.Truef(t, has, "outlier marker for signal %s has no canonical record", sigID)
			assert.Lessf(t, facet, n, "facet %d of signal %s is past the record's facet count", facet, sigID)

			// A facet is either placed or an outlier, never both.
			if story, placed := owner[facetRef{sigID, facet}]; placed {
				t.Errorf("facet %d of signal %s is both in story %s and in the outlier bucket", facet, sigID, story)
			}
			return nil
		})
	}))

	for id := range records {
		assert.NotZerof(t, referenced[id], "canonical record %s is referenced by no facet: a store leak", id)
	}
}

func TestStoreInvariants_HoldAfterIngestAndBatch(t *testing.T) {
	tr := newTestTracker(t)
	ingestN(t, tr, 12)
	storeInvariants(t, tr)

	for range 3 {
		tr.runBatch()
		storeInvariants(t, tr)
	}
}

// The same invariants over multi-facet signals. Single-facet fixtures cannot
// catch a facet index that is dropped or defaulted, because every facet index
// is 0 — so this fixture spreads each signal's facets across two lobes.
func TestStoreInvariants_HoldForMultiFacetSignals(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	now := time.Now()
	for i := range 8 {
		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID: uuid.NewSHA1(TrackerNamespace, []byte(fmt.Sprintf("mf-%d", i))),
			At: now.Add(-time.Duration(i) * time.Second),
			Embeddings: []Embedding{
				unitAt(0.0 + 0.01*float64(i)),
				unitAt(math.Pi/2 + 0.01*float64(i)),
				unitAt(math.Pi + 0.01*float64(i)),
			},
			Data: fmt.Sprintf("mf-%d", i),
		})
		require.NoError(t, err)
	}
	storeInvariants(t, tr)

	for range 3 {
		tr.runBatch()
		storeInvariants(t, tr)
	}

	// Each signal's three facets must be distinguishable in the index.
	for i := range 8 {
		id := uuid.NewSHA1(TrackerNamespace, []byte(fmt.Sprintf("mf-%d", i)))
		facets, err := tr.FacetsOfSignal(id)
		require.NoError(t, err)
		require.Lenf(t, facets, 3, "signal mf-%d must report all three facets", i)
		for f, p := range facets {
			assert.Equal(t, f, p.Facet, "facet index must be preserved in order")
		}
	}
}

// Eviction is the operation that can strand a canonical record, so it gets its
// own pass with a TTL short enough to actually fire.
func TestStoreInvariants_HoldAfterEviction(t *testing.T) {
	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		BatchInterval: time.Hour,
		OutlierTTL:    time.Nanosecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = tr.Close() })

	ingestN(t, tr, 6)
	tr.lastBatch = time.Now().Add(time.Hour)
	tr.runBatch()
	storeInvariants(t, tr)
}

// --- both directions of membership (spec 007 §2.2.3) ---

// seedTwoLobeStories ingests signals whose facets split across two lobes, then
// settles them, returning the signal IDs.
func seedTwoLobeStories(t *testing.T, tr *Tracker[string], n int) []uuid.UUID {
	t.Helper()
	now := time.Now()
	ids := make([]uuid.UUID, n)
	for i := range n {
		ids[i] = uuid.NewSHA1(TrackerNamespace, []byte(fmt.Sprintf("lobe-%d", i)))
		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID: ids[i], At: now.Add(-time.Duration(i) * time.Second),
			Embeddings: []Embedding{
				unitAt(0.0 + 0.01*float64(i)),
				unitAt(math.Pi/2 + 0.01*float64(i)),
			},
			Data: fmt.Sprintf("lobe-%d", i),
		})
		require.NoError(t, err)
	}
	tr.runBatch()
	return ids
}

// The relation must be traversable from both ends and agree with itself.
func TestReadAPI_BothDirectionsAgree(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)
	ids := seedTwoLobeStories(t, tr, 6)

	// signal -> stories
	bySignal := map[uuid.UUID][]uuid.UUID{}
	for _, id := range ids {
		stories, err := tr.StoriesOf(id)
		require.NoError(t, err)
		require.NotEmpty(t, stories, "every signal must reach at least one story")
		bySignal[id] = stories
	}

	// story -> signals, built independently
	byStory := map[uuid.UUID]map[uuid.UUID]bool{}
	for meta := range tr.Stories(StoryStateAny) {
		for sig, err := range tr.SignalsOf(meta.ID) {
			require.NoError(t, err)
			if byStory[sig.ID] == nil {
				byStory[sig.ID] = map[uuid.UUID]bool{}
			}
			byStory[sig.ID][meta.ID] = true
		}
	}

	for id, stories := range bySignal {
		require.Lenf(t, byStory[id], len(stories),
			"StoriesOf and SignalsOf must agree on signal %s", id)
		for _, s := range stories {
			assert.Truef(t, byStory[id][s], "story %s must list signal %s", s, id)
		}
	}
}

// SignalsOf yields a member once however many facets it contributed, while
// FacetsOfStory yields every facet — the multiset the geometry is computed over.
func TestReadAPI_SignalsOfDedupesFacetsOfStoryDoesNot(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	now := time.Now()
	storyID := uuid.NewSHA1(TrackerNamespace, []byte("multi-facet-member"))
	seedStory(t, tr, storyID, storyRecord{
		State: StoryStateActive, Centroid: []float32{1, 0},
		CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-time.Hour),
	})

	sig := Signal[string]{
		ID: uuid.New(), At: now,
		Embeddings: []Embedding{{1, 0}, {0.99, 0.01}, {0.98, 0.02}},
	}
	_, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)

	var members int
	for _, err := range tr.SignalsOf(storyID) {
		require.NoError(t, err)
		members++
	}
	assert.Equal(t, 1, members, "one signal is one member however many facets it contributed")

	var facets []Placement
	for p, err := range tr.FacetsOfStory(storyID) {
		require.NoError(t, err)
		facets = append(facets, p)
	}
	require.Len(t, facets, 3, "FacetsOfStory shows the true geometric membership")
	for i, p := range facets {
		assert.Equal(t, sig.ID, p.SignalID)
		assert.Equal(t, storyID, p.StoryID)
		assert.Equal(t, i, p.Facet, "facets must be ordered by index")
	}
}

// FacetsOfSignal reports unplaced facets as uuid.Nil, which is what makes a
// partially placed signal legible.
func TestReadAPI_FacetsOfSignalReportsUnplaced(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	now := time.Now()
	storyID := uuid.NewSHA1(TrackerNamespace, []byte("partial-read"))
	seedStory(t, tr, storyID, storyRecord{
		State: StoryStateActive, Centroid: []float32{1, 0},
		CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-time.Hour),
	})

	sig := Signal[string]{ID: uuid.New(), At: now, Embeddings: []Embedding{{1, 0}, {-1, 0}}}
	_, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)

	facets, err := tr.FacetsOfSignal(sig.ID)
	require.NoError(t, err)
	require.Len(t, facets, 2)
	assert.Equal(t, storyID, facets[0].StoryID)
	assert.Equal(t, uuid.Nil, facets[1].StoryID, "an unplaced facet reports no story")

	stories, err := tr.StoriesOf(sig.ID)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{storyID}, stories, "StoriesOf reports only placed facets")
}

func TestReadAPI_UnknownSignal(t *testing.T) {
	tr := newTestTracker(t)

	stories, err := tr.StoriesOf(uuid.New())
	require.NoError(t, err)
	assert.Empty(t, stories)

	facets, err := tr.FacetsOfSignal(uuid.New())
	require.NoError(t, err)
	assert.Empty(t, facets)
}

func TestReadAPI_EmptyStory(t *testing.T) {
	tr := newTestTracker(t)
	var got []Placement
	for p, err := range tr.FacetsOfStory(uuid.New()) {
		require.NoError(t, err)
		got = append(got, p)
	}
	assert.Empty(t, got)
}

func TestOutliers_YieldsUnplacedSignals(t *testing.T) {
	tr := newTestTracker(t)

	// Ingest a signal that won't land in any story (no existing stories)
	sig1 := Signal[string]{
		ID:         uuid.New(),
		At:         time.Now(),
		Embeddings: []Embedding{{1, 0}},
		Data:       "outlier-1",
	}
	_, err := tr.Ingest(context.Background(), sig1)
	require.NoError(t, err)

	var outliers []Signal[string]
	for s, err := range tr.Outliers() {
		require.NoError(t, err)
		outliers = append(outliers, s)
	}
	require.Len(t, outliers, 1)
	assert.Equal(t, sig1.ID, outliers[0].ID)
	assert.Equal(t, sig1.Data, outliers[0].Data)
}

func TestOutliers_YieldsSignalOncePerSignal(t *testing.T) {
	tr := newTestTracker(t)

	// Ingest a multi-facet signal that lands as outlier for both facets
	sig := Signal[string]{
		ID:         uuid.New(),
		At:         time.Now(),
		Embeddings: []Embedding{{1, 0}, {0, 1}},
		Data:       "multi-facet-outlier",
	}
	_, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)

	var outliers []Signal[string]
	for s, err := range tr.Outliers() {
		require.NoError(t, err)
		outliers = append(outliers, s)
	}
	require.Len(t, outliers, 1, "multi-facet outlier signal must be yielded exactly once")
	assert.Equal(t, sig.ID, outliers[0].ID)
}

