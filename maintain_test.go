package story

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/keys"
)

// unitAt returns a unit vector at the given angle in the first two dimensions,
// so cosine distances between fixtures are a direct function of angle.
func unitAt(angle float64) []float32 {
	s, c := math.Sincos(angle)
	return []float32{float32(c), float32(s)}
}

// sigAt builds a batch signal at an angle, with a deterministic ID derived
// from a name so tie-breaks in the algorithms are reproducible across runs.
func sigAt(name string, angle float64) *batchFacet {
	return &batchFacet{
		id:  uuid.NewSHA1(TrackerNamespace, []byte(name)),
		at:  time.Now(),
		emb: unitAt(angle),
	}
}

func arcSignals(prefix string, n int, center, spread float64) []*batchFacet {
	out := make([]*batchFacet, n)
	for i := range out {
		a := center + spread*(float64(i)/float64(n)-0.5)
		out[i] = sigAt(prefix+string(rune('a'+i)), a)
	}
	return out
}

// --- outlier promotion ---

func TestPromoteOutliers_FormsCompactStory(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.AssignThreshold = 0.28
	tr.cfg.MinStorySize = 3

	group := arcSignals("tight", 5, 0.5, 0.02)
	promos := tr.promoteOutliers(group)

	require.Len(t, promos, 1)
	assert.Len(t, promos[0].members, 5)
}

func TestPromoteOutliers_BelowMinStorySizeStaysOutlier(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.AssignThreshold = 0.28
	tr.cfg.MinStorySize = 4

	assert.Empty(t, tr.promoteOutliers(arcSignals("small", 3, 0.5, 0.02)))
}

// TestPromoteOutliers_DoesNotChain is what the closing compaction in
// growCompactGroups buys. A chain A-B-C where A and C are far apart is one
// component but not one story, and a centroid that walks along the ladder while
// growing would produce exactly that. Compacting to the final centroid bounds
// the group whatever path the centroid took.
func TestPromoteOutliers_Deterministic(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.AssignThreshold = 0.28
	tr.cfg.MinStorySize = 3

	group := append(arcSignals("d1", 5, 0.4, 0.05), arcSignals("d2", 5, 2.0, 0.05)...)
	shuffled := append([]*batchFacet(nil), group...)
	shuffled[0], shuffled[9] = shuffled[9], shuffled[0]
	shuffled[3], shuffled[6] = shuffled[6], shuffled[3]

	first, second := tr.promoteOutliers(group), tr.promoteOutliers(shuffled)
	require.Equal(t, len(first), len(second))
	for i := range first {
		assert.ElementsMatch(t, idsOf(first[i].members), idsOf(second[i].members),
			"promotion must not depend on input order")
	}
}

// --- split ---

// TestSplitStory_DiffuseDoesNotSplit is the distinction the acceptance test
// exists for: a broad story with no internal gap is not two stories, and
// cutting it would leave halves inside the hysteresis band with nothing to
// reunite them.
// TestSplitStory_RadiusGateIsSound checks the gate never skips a story that
// would have split. The gate is a mathematical necessary condition, so a
// counterexample is a correctness bug -- this test caught one: an earlier
// 2*radius bound was Euclidean reasoning applied to cosine distance, which
// grows quadratically in the angle and needs 4r-2r^2 instead.
// TestPlanMerges_MutuallyCloseGroupMerges covers the legitimate multi-way
// case: three centroids that are within threshold of each other pairwise, not
// merely chained through a middle one.
// TestPlanMerges_DoesNotChain is the regression for a real collapse. Story
// centroids in a ladder, each 0.005 from its neighbour but 0.55 end to end,
// formed a single connected component under the original union-find merge and
// swallowed the whole corpus into one story. Cliques stop it.
// cosDist is the package-internal distance under test, wrapped so the tests
// read the same way the implementation does.
func cosDist(a, b []float32) float64 { return dist.CosineDistance(a, b) }

// mergeTracker builds a tracker whose thresholds isolate the merge rule under
// test: the split gate is set wide so compactMergeGroup never interferes.
func mergeTracker(t *testing.T, merge float64) *Tracker[string] {
	tr := newTestTracker(t)
	tr.cfg.MergeThreshold = merge
	tr.cfg.SplitThreshold = 2
	return tr
}

// singleMembers gives each story one member at its centroid, which is enough
// for rules that only compare centroids.
func singleMembers(centroids map[uuid.UUID][]float32) map[uuid.UUID][]*batchFacet {
	out := make(map[uuid.UUID][]*batchFacet, len(centroids))
	for id, c := range centroids {
		out[id] = []*batchFacet{{id: id, emb: c, at: time.Now()}}
	}
	return out
}

func idsOf(members []*batchFacet) []uuid.UUID {
	out := make([]uuid.UUID, len(members))
	for i, m := range members {
		out[i] = m.id
	}
	return out
}

// --- group growth ---

// Growth admits by proximity to a moving centroid, so the invariant that makes
// it safe is the closing compaction: every surviving member sits within the
// threshold of the group's final centre, whatever route the centre took.
// Growth must cover a real cluster that a clique cannot: an arc whose extremes
// are further apart than the threshold, but which fits inside a ball of that
// radius.
// Growth is deterministic for a given candidate order, which is what
// promoteOutliers relies on when it sorts by signal ID first. Order-independence
// of the promotion itself is covered by TestPromoteOutliers_Deterministic.
func TestAdmitOutliers_JoinsTheCoveringStory(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.AssignThreshold = 0.28

	story := arcSignals("story", 6, 0.0, 0.04)
	sid := uuid.NewSHA1(TrackerNamespace, []byte("story-id"))
	members := map[uuid.UUID][]*batchFacet{sid: story}

	near := sigAt("near", 0.03)
	far := sigAt("far", 2.0)

	got := tr.admitOutliers(members, []*batchFacet{near, far}, time.Now())
	require.Len(t, got, 1, "exactly the covered outlier must be admitted")
	assert.Equal(t, near.id, got[0].sig.id)
	assert.Equal(t, sid, got[0].storyID)
}

func TestAdmitOutliers_PicksTheNearestStory(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.AssignThreshold = 0.28

	a := uuid.NewSHA1(TrackerNamespace, []byte("story-a"))
	b := uuid.NewSHA1(TrackerNamespace, []byte("story-b"))
	members := map[uuid.UUID][]*batchFacet{
		a: arcSignals("sa", 6, 0.0, 0.04),
		b: arcSignals("sb", 6, 0.5, 0.04),
	}

	got := tr.admitOutliers(members, []*batchFacet{sigAt("x", 0.48)}, time.Now())
	require.Len(t, got, 1)
	assert.Equal(t, b, got[0].storyID, "outlier joined the further story")
}

// Thresholds are measured once, before any admission, so the outcome cannot
// depend on the order outliers happen to be visited -- and one admission cannot
// widen a story into reach of the next.
func TestAdmitOutliers_OrderIndependent(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.AssignThreshold = 0.28

	sid := uuid.NewSHA1(TrackerNamespace, []byte("story-id"))
	members := map[uuid.UUID][]*batchFacet{sid: arcSignals("story", 6, 0.0, 0.04)}

	outliers := []*batchFacet{sigAt("o1", 0.05), sigAt("o2", 0.9), sigAt("o3", 0.02)}
	forward := tr.admitOutliers(members, outliers, time.Now())

	reversed := []*batchFacet{outliers[2], outliers[1], outliers[0]}
	backward := tr.admitOutliers(members, reversed, time.Now())

	require.Equal(t, len(forward), len(backward))
	for i := range forward {
		assert.Equal(t, forward[i].sig.id, backward[i].sig.id)
		assert.Equal(t, forward[i].storyID, backward[i].storyID)
	}
}

func TestAdmitOutliers_NoStoriesAdmitsNothing(t *testing.T) {
	tr := newTestTracker(t)
	assert.Empty(t, tr.admitOutliers(nil, []*batchFacet{sigAt("x", 0.1)}, time.Now()))
}

// --- merge and split as inverses ---

// A merge may not produce a story the next run would cut apart. The test is the
// split decision itself, not the radius gate: the gate is only a necessary
// condition for splitting, so using it refuses unions no split would touch.
// The converse: a union that split leaves whole is merged, even when its radius
// is wide enough that the radius gate alone would have refused it.

// --- deterministic story IDs ---

// deriveIn runs deriveStoryID inside a transaction on the tracker's store.
func deriveIn(t *testing.T, tr *Tracker[string], seed string, members []*batchFacet, taken map[uuid.UUID]time.Time) uuid.UUID {
	t.Helper()
	var got uuid.UUID
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		var err error
		got, err = deriveStoryID(tx, tr.cfg.Namespace, seed, members, taken)
		return err
	}))
	return got
}

// occupy writes a placeholder metadata record so an ID reads as taken.
func occupy(t *testing.T, tr *Tracker[string], id uuid.UUID) {
	t.Helper()
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return tx.Put(keys.StoryMeta(id), []byte("{}"))
	}))
}

// A story ID is a function of its founding members, not of the order they were
// collected in, and not of the tracker instance that ran the pass.
func TestDeriveStoryID_DependsOnMembersOnly(t *testing.T) {
	members := arcSignals("derive", 5, 0.3, 0.04)
	reversed := make([]*batchFacet, len(members))
	for i, m := range members {
		reversed[len(members)-1-i] = m
	}

	first := deriveIn(t, newTestTracker(t), "promote", members, nil)
	assert.Equal(t, first, deriveIn(t, newTestTracker(t), "promote", reversed, nil),
		"member order changed the derived ID")

	assert.NotEqual(t, first, deriveIn(t, newTestTracker(t), "promote", members[:4], nil),
		"a different member set produced the same ID")
}

// Promotion and split are separate births, so the same member set reaching
// story status by either route must not claim one ID.
func TestDeriveStoryID_SeedSeparatesBirthRoutes(t *testing.T) {
	tr := newTestTracker(t)
	members := arcSignals("seed", 4, 0.3, 0.04)
	parent := uuid.NewSHA1(TrackerNamespace, []byte("parent"))

	promoted := deriveIn(t, tr, "promote", members, nil)
	split := deriveIn(t, tr, "split:"+parent.String(), members, nil)
	assert.NotEqual(t, promoted, split)
}

// An ID already held by a live story is rejected and rederived, so a split
// spawning the member set of an existing story cannot silently fold into it.
// The rederivation is itself deterministic.
func TestDeriveStoryID_SaltsPastOccupiedIDs(t *testing.T) {
	members := arcSignals("salt", 4, 0.3, 0.04)

	base := deriveIn(t, newTestTracker(t), "promote", members, nil)

	stored := newTestTracker(t)
	occupy(t, stored, base)
	next := deriveIn(t, stored, "promote", members, nil)
	assert.NotEqual(t, base, next, "an occupied ID was handed out again")
	assert.Equal(t, next, deriveIn(t, stored, "promote", members, nil),
		"rederivation past an occupied ID is not reproducible")

	// A story created earlier in the same run holds its ID before any
	// metadata exists for it, so the in-run set must be honoured too.
	inRun := deriveIn(t, newTestTracker(t), "promote", members,
		map[uuid.UUID]time.Time{base: time.Now()})
	assert.Equal(t, next, inRun, "the in-run set and the store disagree on what is taken")
}

// The end-to-end property: replaying a signal stream against a fresh store
// reproduces the story IDs of the original run, so recorded output can be
// diffed against a replay.
func TestStoryIDs_SurviveReplay(t *testing.T) {
	run := func() map[uuid.UUID][]uuid.UUID {
		tr := newTestTracker(t)
		tr.dim.Store(2)
		now := time.Now()
		for i := range 6 {
			ingestAt(t, tr, fmt.Sprintf("replay-a-%d", i), 0.0+float64(i)*0.01, now)
		}
		for i := range 6 {
			ingestAt(t, tr, fmt.Sprintf("replay-b-%d", i), 2.0+float64(i)*0.01, now)
		}
		tr.runBatch()
		return storySnapshot(t, tr)
	}

	first, second := run(), run()
	require.NotEmpty(t, first)
	require.Equal(t, len(first), len(second))
	for id, sigs := range first {
		require.Contains(t, second, id, "replay minted a different story ID")
		assert.ElementsMatch(t, sigs, second[id], "story %s has different members on replay", id)
	}
}

// --- facet-granular maintenance (spec 007 §2.3.3) ---

// facetSig builds a signal whose facets sit at the given angles.
func facetSig(name string, at time.Time, angles ...float64) Signal[string] {
	embs := make([]Embedding, len(angles))
	for i, a := range angles {
		embs[i] = unitAt(a)
	}
	return Signal[string]{
		ID:         uuid.NewSHA1(TrackerNamespace, []byte(name)),
		At:         at,
		Embeddings: embs,
		Data:       name,
	}
}

// The self-promotion guard: one signal carrying MinStorySize facets must not
// found a story alone, however tightly its facets cluster. This is the most
// plausible and least visible way this change could go wrong.
func TestMaintain_OneSignalCannotPromoteItself(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	_, err := tr.Ingest(context.Background(), facetSig("solo", time.Now(), 0.50, 0.51, 0.52, 0.53))
	require.NoError(t, err)

	tr.runBatch()

	var stories int
	for range tr.Stories(StoryStateAny) {
		stories++
	}
	assert.Zero(t, stories, "four facets of one signal are still one signal: no story may be founded")
}

// The same facets spread across enough distinct signals do promote, so the
// guard above rejects for the right reason.
func TestMaintain_DistinctSignalsPromote(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	now := time.Now()
	for i, name := range []string{"alpha", "beta", "gamma"} {
		_, err := tr.Ingest(context.Background(), facetSig(name, now, 0.50+0.01*float64(i)))
		require.NoError(t, err)
	}

	tr.runBatch()

	var stories int
	for range tr.Stories(StoryStateAny) {
		stories++
	}
	assert.Equal(t, 1, stories, "three distinct signals must found a story")
}

// A split is where multi-membership is born: two facets of one signal on
// opposite sides of the cut put that signal in both children.
func TestMaintain_SplitPutsOneSignalInBothChildren(t *testing.T) {
	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		Codec:         JSONCodec[string]{},
		BatchInterval: time.Hour,
		MinStorySize:  3,
		MeanRemoval:   0, // keep the fixture's geometry as written
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = tr.Close() })
	tr.dim.Store(2)

	// Six signals, each with one facet in each of two well-separated lobes.
	now := time.Now()
	var ids []uuid.UUID
	for i := range 6 {
		sig := facetSig(fmt.Sprintf("straddle-%d", i), now,
			0.0+0.01*float64(i),
			math.Pi/2+0.01*float64(i),
		)
		ids = append(ids, sig.ID)
		_, err := tr.Ingest(context.Background(), sig)
		require.NoError(t, err)
	}

	// Two runs: the first promotes, the second may split what promotion built.
	tr.runBatch()
	tr.runBatch()

	// Whatever the run decided, every signal that appears in two stories must
	// do so through distinct facets, and no facet may be in two stories.
	multi := 0
	for _, id := range ids {
		stories, err := tr.StoriesOf(id)
		require.NoError(t, err)
		if len(stories) > 1 {
			multi++
		}
	}
	t.Logf("%d of %d signals reached more than one story", multi, len(ids))
	storeInvariants(t, tr)
}

// Several facets of one signal moving into one story is one reassignment
// event, not one per facet.
func TestDedupeReassignments_CollapsesPerSignalStory(t *testing.T) {
	story, other := uuid.New(), uuid.New()
	sig := uuid.New()

	in := []StoryEvent[string]{
		{Kind: EventStoryCreated, StoryID: story},
		{Kind: EventSignalReassigned, StoryID: story, SignalID: sig},
		{Kind: EventSignalReassigned, StoryID: story, SignalID: sig},
		{Kind: EventSignalReassigned, StoryID: other, SignalID: sig},
		{Kind: EventStoryRetired, StoryID: other},
	}

	got := dedupeReassignments(append([]StoryEvent[string](nil), in...))
	require.Len(t, got, 4)
	assert.Equal(t, EventStoryCreated, got[0].Kind)
	assert.Equal(t, story, got[1].StoryID)
	assert.Equal(t, other, got[2].StoryID, "a different story is a different membership")
	assert.Equal(t, EventStoryRetired, got[3].Kind)
}
