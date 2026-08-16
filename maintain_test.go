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

// TestPromoteOutliers_DoesNotChain is the reason promotion uses cliques rather
// than connected components. A chain A-B-C where A and C are far apart is one
// component but not one story; components are exactly the transitive linkage
// that produced the blob this design removes.
func TestPromoteOutliers_DoesNotChain(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.AssignThreshold = 0.28
	tr.cfg.MinStorySize = 3

	// A ladder of signals, each near its neighbour but spanning far more than
	// the threshold end to end.
	var chain []*batchSignal
	for i := range 10 {
		chain = append(chain, sigAt("chain"+string(rune('a'+i)), float64(i)*0.2))
	}

	for _, p := range tr.promoteOutliers(chain) {
		c := centroidOf(p.members)
		for _, m := range p.members {
			assert.LessOrEqual(t, cosDist(m.emb, c), tr.cfg.AssignThreshold,
				"a promoted story must be compact, not a chain")
		}
	}
}

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

func TestSplitStory_BimodalSplits(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.SplitThreshold = 0.30
	tr.cfg.MinStorySize = 3

	members := append(arcSignals("left", 5, 0.0, 0.03), arcSignals("right", 4, 1.2, 0.03)...)
	res, ok := tr.splitStory(members, radiusOf(members))

	require.True(t, ok, "two well-separated groups must split")
	assert.Len(t, res.keep, 5, "the larger part keeps the story ID")
	assert.Len(t, res.spawn, 4)
	assert.Greater(t, cosDist(centroidOf(res.keep), centroidOf(res.spawn)), tr.cfg.SplitThreshold)
}

// TestSplitStory_DiffuseDoesNotSplit is the distinction the acceptance test
// exists for: a broad story with no internal gap is not two stories, and
// cutting it would leave halves inside the hysteresis band with nothing to
// reunite them.
func TestSplitStory_DiffuseDoesNotSplit(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.SplitThreshold = 0.30
	tr.cfg.MinStorySize = 3

	members := arcSignals("diffuse", 12, 0.0, 1.0)
	require.Greater(t, maxAngularSeparation(radiusOf(members)), tr.cfg.SplitThreshold,
		"fixture must clear the radius gate, or it proves nothing")

	_, ok := tr.splitStory(members, radiusOf(members))
	assert.False(t, ok)
}

func TestSplitStory_MinStorySizeBlocksSplit(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.SplitThreshold = 0.30
	tr.cfg.MinStorySize = 4

	// Well separated, but the smaller side holds only two signals.
	members := append(arcSignals("big", 6, 0.0, 0.03), arcSignals("tiny", 2, 1.2, 0.03)...)
	_, ok := tr.splitStory(members, radiusOf(members))
	assert.False(t, ok, "a split may not produce a part below MinStorySize")
}

// TestSplitStory_RadiusGateIsSound checks the gate never skips a story that
// would have split. The gate is a mathematical necessary condition, so a
// counterexample is a correctness bug -- this test caught one: an earlier
// 2*radius bound was Euclidean reasoning applied to cosine distance, which
// grows quadratically in the angle and needs 4r-2r^2 instead.
func TestSplitStory_RadiusGateIsSound(t *testing.T) {
	tr := newTestTracker(t)
	tr.cfg.SplitThreshold = 0.30
	tr.cfg.MinStorySize = 3

	for _, spread := range []float64{0.05, 0.2, 0.5, 1.0, 1.6} {
		members := append(
			arcSignals("g1", 5, 0.0, 0.02),
			arcSignals("g2", 5, spread, 0.02)...,
		)
		r := radiusOf(members)
		if maxAngularSeparation(r) > tr.cfg.SplitThreshold {
			continue // gate open, nothing to prove
		}
		// Gate closed: assert no partition could have cleared the bar.
		best := 0.0
		for i := 1; i < len(members); i++ {
			d := cosDist(centroidOf(members[:i]), centroidOf(members[i:]))
			if d > best {
				best = d
			}
		}
		assert.LessOrEqual(t, best, tr.cfg.SplitThreshold,
			"gate closed at radius %.4f but a partition reached %.4f", r, best)
	}
}

// --- merge ---

func TestPlanMerges_OldestSurvives(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	now := time.Now()
	centroids := map[uuid.UUID][]float32{a: unitAt(0), b: unitAt(0.05)}
	created := map[uuid.UUID]time.Time{a: now, b: now.Add(-time.Hour)}

	plan := planMerges([]uuid.UUID{a, b}, centroids, created, 0.22)

	require.Len(t, plan, 1)
	assert.Equal(t, b, plan[a], "the older story must survive")
}

func TestPlanMerges_Transitive(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	now := time.Now()
	centroids := map[uuid.UUID][]float32{a: unitAt(0), b: unitAt(0.1), c: unitAt(0.2)}
	created := map[uuid.UUID]time.Time{
		a: now, b: now.Add(-time.Hour), c: now.Add(-2 * time.Hour),
	}

	plan := planMerges([]uuid.UUID{a, b, c}, centroids, created, 0.22)

	assert.Len(t, plan, 2, "A-B and B-C must collapse into one story")
	for _, retired := range []uuid.UUID{a, b} {
		assert.Equal(t, c, plan[retired])
	}
}

func TestPlanMerges_BeyondThresholdStaysApart(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	now := time.Now()
	centroids := map[uuid.UUID][]float32{a: unitAt(0), b: unitAt(1.2)}
	created := map[uuid.UUID]time.Time{a: now, b: now}

	assert.Empty(t, planMerges([]uuid.UUID{a, b}, centroids, created, 0.22))
}

func TestPlanMerges_DeterministicUnderShuffledOrder(t *testing.T) {
	ids := make([]uuid.UUID, 6)
	centroids := map[uuid.UUID][]float32{}
	created := map[uuid.UUID]time.Time{}
	now := time.Now()
	for i := range ids {
		ids[i] = uuid.NewSHA1(TrackerNamespace, []byte{byte(i)})
		centroids[ids[i]] = unitAt(float64(i) * 0.04)
		created[ids[i]] = now.Add(-time.Duration(i) * time.Minute)
	}

	forward := planMerges(ids, centroids, created, 0.22)
	reversed := make([]uuid.UUID, len(ids))
	for i, id := range ids {
		reversed[len(ids)-1-i] = id
	}
	assert.Equal(t, forward, planMerges(reversed, centroids, created, 0.22))
}

// --- helpers ---

// cosDist is the package-internal distance under test, wrapped so the tests
// read the same way the implementation does.
func cosDist(a, b []float32) float64 { return dist.CosineDistance(a, b) }

func idsOf(members []*batchSignal) []uuid.UUID {
	out := make([]uuid.UUID, len(members))
	for i, m := range members {
		out[i] = m.id
	}
	return out
}
