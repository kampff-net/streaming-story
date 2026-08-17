package story

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kvsh.ch/streaming-story/internal/dist"
)

// unitAt returns a unit vector at the given angle in the first two dimensions,
// so cosine distances between fixtures are a direct function of angle.
func unitAt(angle float64) []float32 {
	s, c := math.Sincos(angle)
	return []float32{float32(c), float32(s)}
}

// sigAt builds a batch signal at an angle, with a deterministic ID derived
// from a name so tie-breaks in the algorithms are reproducible across runs.
func sigAt(name string, angle float64) *batchSignal {
	return &batchSignal{
		id:  uuid.NewSHA1(TrackerNamespace, []byte(name)),
		at:  time.Now(),
		emb: unitAt(angle),
	}
}

func arcSignals(prefix string, n int, center, spread float64) []*batchSignal {
	out := make([]*batchSignal, n)
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
	shuffled := append([]*batchSignal(nil), group...)
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
func singleMembers(centroids map[uuid.UUID][]float32) map[uuid.UUID][]*batchSignal {
	out := make(map[uuid.UUID][]*batchSignal, len(centroids))
	for id, c := range centroids {
		out[id] = []*batchSignal{{id: id, emb: c, at: time.Now()}}
	}
	return out
}

func idsOf(members []*batchSignal) []uuid.UUID {
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
	members := map[uuid.UUID][]*batchSignal{sid: story}

	near := sigAt("near", 0.03)
	far := sigAt("far", 2.0)

	got := tr.admitOutliers(members, []*batchSignal{near, far}, time.Now())
	require.Len(t, got, 1, "exactly the covered outlier must be admitted")
	assert.Equal(t, near.id, got[0].sig.id)
	assert.Equal(t, sid, got[0].storyID)
}

func TestAdmitOutliers_PicksTheNearestStory(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.AssignThreshold = 0.28

	a := uuid.NewSHA1(TrackerNamespace, []byte("story-a"))
	b := uuid.NewSHA1(TrackerNamespace, []byte("story-b"))
	members := map[uuid.UUID][]*batchSignal{
		a: arcSignals("sa", 6, 0.0, 0.04),
		b: arcSignals("sb", 6, 0.5, 0.04),
	}

	got := tr.admitOutliers(members, []*batchSignal{sigAt("x", 0.48)}, time.Now())
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
	members := map[uuid.UUID][]*batchSignal{sid: arcSignals("story", 6, 0.0, 0.04)}

	outliers := []*batchSignal{sigAt("o1", 0.05), sigAt("o2", 0.9), sigAt("o3", 0.02)}
	forward := tr.admitOutliers(members, outliers, time.Now())

	reversed := []*batchSignal{outliers[2], outliers[1], outliers[0]}
	backward := tr.admitOutliers(members, reversed, time.Now())

	require.Equal(t, len(forward), len(backward))
	for i := range forward {
		assert.Equal(t, forward[i].sig.id, backward[i].sig.id)
		assert.Equal(t, forward[i].storyID, backward[i].storyID)
	}
}

func TestAdmitOutliers_NoStoriesAdmitsNothing(t *testing.T) {
	tr := newTestTracker(t)
	assert.Empty(t, tr.admitOutliers(nil, []*batchSignal{sigAt("x", 0.1)}, time.Now()))
}

// --- merge and split as inverses ---

// A merge may not produce a story the next run would cut apart. The test is the
// split decision itself, not the radius gate: the gate is only a necessary
// condition for splitting, so using it refuses unions no split would touch.
// The converse: a union that split leaves whole is merged, even when its radius
// is wide enough that the radius gate alone would have refused it.
