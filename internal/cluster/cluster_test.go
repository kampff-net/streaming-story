package cluster

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/geom"
)

// ns is a fixed namespace so fixture IDs — and therefore every tie-break — are
// reproducible across runs.
var ns = uuid.MustParse("d4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f80")

// unitAt returns a unit vector at the given angle in two dimensions, so
// distances between fixtures are a direct function of angle.
func unitAt(angle float64) []float32 {
	s, c := math.Sincos(angle)
	return []float32{float32(c), float32(s)}
}

func pointAt(name string, angle float64) Point {
	return Point{ID: uuid.NewSHA1(ns, []byte(name)), At: time.Now(), Vec: unitAt(angle)}
}

// arcPoints returns n points spread evenly around center.
func arcPoints(prefix string, n int, center, spread float64) []Point {
	out := make([]Point, n)
	for i := range out {
		a := center + spread*(float64(i)/float64(n)-0.5)
		out[i] = pointAt(prefix+string(rune('a'+i)), a)
	}
	return out
}

func params(assign, merge, split float64, minSize int) Params {
	return Params{Assign: assign, Merge: merge, Split: split, MinSize: minSize}
}

func cosDist(a, b []float32) float64 { return dist.CosineDistance(a, b) }

func idsOf(pts []Point, idx []int) []uuid.UUID {
	out := make([]uuid.UUID, len(idx))
	for i, j := range idx {
		out[i] = pts[j].ID
	}
	return out
}

// --- growth ---

func TestGrow_FormsOneCompactGroup(t *testing.T) {
	pts := arcPoints("tight", 5, 0.5, 0.02)
	groups := Grow(pts, params(0.28, 0.22, 0.30, 3))

	require.Len(t, groups, 1)
	assert.Len(t, groups[0], 5)
}

func TestGrow_BelowMinSizeFormsNothing(t *testing.T) {
	assert.Empty(t, Grow(arcPoints("small", 3, 0.5, 0.02), params(0.28, 0.22, 0.30, 4)))
}

// Growth admits by proximity to a moving centroid, so the invariant that makes it
// safe is the closing compaction: every survivor sits within the threshold of the
// group's final centre, whatever route the centre took.
func TestGrow_EveryMemberInsideThreshold(t *testing.T) {
	const thr = 0.28
	var pts []Point
	for i := range 30 {
		pts = append(pts, pointAt("grow"+string(rune('a'+i)), float64(i)*0.08))
	}

	for _, g := range Grow(pts, params(thr, 0.22, 0.30, 3)) {
		c := Centroid(pts, g)
		for _, i := range g {
			assert.LessOrEqual(t, cosDist(pts[i].Vec, c), thr,
				"member sits %.4f from the group centre", cosDist(pts[i].Vec, c))
		}
	}
}

// A chain A-B-C where A and C are far apart is one component but not one group.
// The compaction is what stops the centroid walking along the ladder.
func TestGrow_DoesNotChain(t *testing.T) {
	const thr = 0.28
	var chain []Point
	for i := range 10 {
		chain = append(chain, pointAt("chain"+string(rune('a'+i)), float64(i)*0.2))
	}

	for _, g := range Grow(chain, params(thr, 0.22, 0.30, 3)) {
		c := Centroid(chain, g)
		for _, i := range g {
			assert.LessOrEqual(t, cosDist(chain[i].Vec, c), thr,
				"a grown group must be compact, not a chain")
		}
	}
}

// Growth must cover a real cluster that a clique cannot: an arc whose extremes
// are further apart than the threshold, but which fits inside a ball of that
// radius.
func TestGrow_CoversAnArcACliqueWouldShatter(t *testing.T) {
	const thr = 0.10
	pts := arcPoints("arc", 12, 1.0, 0.9)
	require.LessOrEqual(t, Radius(pts), thr, "fixture must fit inside the radius")
	require.Greater(t, cosDist(pts[0].Vec, pts[11].Vec), thr,
		"fixture extremes must exceed the threshold, or a clique would suffice")

	best := 0
	for _, g := range Grow(pts, params(thr, 0.05, 0.30, 3)) {
		best = max(best, len(g))
	}

	bestClique := 0
	for _, g := range Cliques(len(pts), func(i, j int) bool {
		return cosDist(pts[i].Vec, pts[j].Vec) <= thr
	}, 3) {
		bestClique = max(bestClique, len(g))
	}

	assert.Greater(t, best, bestClique,
		"growth grouped %d where cliques managed %d", best, bestClique)
}

func TestGrow_RepeatableForAGivenOrder(t *testing.T) {
	pts := append(arcPoints("g1", 6, 0.4, 0.05), arcPoints("g2", 6, 2.0, 0.05)...)
	p := params(0.28, 0.22, 0.30, 3)

	first, second := Grow(pts, p), Grow(pts, p)
	require.Equal(t, len(first), len(second))
	for i := range first {
		assert.Equal(t, first[i], second[i])
	}
}

func TestCompactToRadius_DropsTheOutlyingMember(t *testing.T) {
	pts := append(arcPoints("tight", 5, 0.0, 0.02), pointAt("far", 1.2))

	got := CompactToRadius(pts, []int{0, 1, 2, 3, 4, 5}, 0.05)
	assert.NotContains(t, got, 5, "the distant member must be dropped")
	assert.Len(t, got, 5)
}

// --- split ---

func TestSplit_BimodalSplits(t *testing.T) {
	p := params(0.28, 0.22, 0.30, 3)
	pts := append(arcPoints("l", 4, 0.0, 0.05), arcPoints("r", 4, 1.2, 0.05)...)

	div, ok := Split(pts, Radius(pts), p)
	require.True(t, ok, "two clearly separated lobes must split")
	assert.GreaterOrEqual(t, len(div.Keep), p.MinSize)
	assert.GreaterOrEqual(t, len(div.Spawn), p.MinSize)
	assert.Len(t, append(append([]int{}, div.Keep...), div.Spawn...), len(pts))
	assert.GreaterOrEqual(t, len(div.Keep), len(div.Spawn), "the larger part keeps the identity")
	assert.Greater(t, cosDist(Centroid(pts, div.Keep), Centroid(pts, div.Spawn)), p.Split)
}

func TestSplit_DiffuseDoesNotSplit(t *testing.T) {
	// One evenly-filled arc: wide, but with no internal gap to cut on.
	pts := arcPoints("diffuse", 12, 0.0, 0.5)
	_, ok := Split(pts, Radius(pts), params(0.28, 0.22, 0.30, 3))
	assert.False(t, ok, "a group with no internal gap must be left whole")
}

func TestSplit_MinSizeBlocksSplit(t *testing.T) {
	pts := append(arcPoints("l", 4, 0.0, 0.05), arcPoints("r", 2, 1.2, 0.02)...)
	_, ok := Split(pts, Radius(pts), params(0.28, 0.22, 0.30, 3))
	assert.False(t, ok, "a part below MinSize must block the split")
}

// The radius gate must never skip a group that would have split. A pair at
// +/-0.5 rad sits 0.122 from the centroid but 0.46 from its opposite, which the
// Euclidean 2r bound would have dismissed.
func TestSplit_RadiusGateIsSound(t *testing.T) {
	p := params(0.28, 0.22, 0.30, 3)
	pts := append(arcPoints("l", 3, -0.5, 0.01), arcPoints("r", 3, 0.5, 0.01)...)

	r := Radius(pts)
	require.Greater(t, geom.MaxAngularSeparation(r), p.Split,
		"the quadratic gate must let this group through (radius %.4f)", r)
	require.LessOrEqual(t, 2*r, p.Split,
		"a Euclidean gate would have skipped it, which is the point of the test")

	_, ok := Split(pts, r, p)
	assert.True(t, ok, "the group splits, so the gate must not have skipped it")
}

// --- merge ---

func singleGroups(centroids map[uuid.UUID][]float32) map[uuid.UUID][]Point {
	out := make(map[uuid.UUID][]Point, len(centroids))
	for id, c := range centroids {
		out[id] = []Point{{ID: id, Vec: c}}
	}
	return out
}

func TestPlanMerges_OldestSurvives(t *testing.T) {
	older := uuid.NewSHA1(ns, []byte("older"))
	newer := uuid.NewSHA1(ns, []byte("newer"))
	now := time.Now()

	centroids := map[uuid.UUID][]float32{older: unitAt(0.0), newer: unitAt(0.01)}
	created := map[uuid.UUID]time.Time{older: now.Add(-time.Hour), newer: now}

	plan := PlanMerges(singleGroups(centroids), centroids, created, params(0.28, 0.22, 0.30, 3))
	assert.Equal(t, older, plan[newer], "the oldest story must absorb the newer one")
	assert.NotContains(t, plan, older)
}

func TestPlanMerges_BeyondThresholdStaysApart(t *testing.T) {
	a := uuid.NewSHA1(ns, []byte("a"))
	b := uuid.NewSHA1(ns, []byte("b"))
	centroids := map[uuid.UUID][]float32{a: unitAt(0.0), b: unitAt(1.2)}
	created := map[uuid.UUID]time.Time{a: time.Now(), b: time.Now()}

	assert.Empty(t, PlanMerges(singleGroups(centroids), centroids, created,
		params(0.28, 0.22, 0.30, 3)))
}

// A ladder of stories, each near its neighbour but spanning far more end to end,
// must not collapse into one. Union-find did exactly that.
func TestPlanMerges_DoesNotChain(t *testing.T) {
	const n = 12
	ids := make([]uuid.UUID, n)
	centroids := map[uuid.UUID][]float32{}
	created := map[uuid.UUID]time.Time{}
	now := time.Now()
	for i := range ids {
		ids[i] = uuid.NewSHA1(ns, []byte{byte(i)})
		centroids[ids[i]] = unitAt(float64(i) * 0.1)
		created[ids[i]] = now.Add(-time.Duration(i) * time.Minute)
	}
	require.Greater(t, cosDist(centroids[ids[0]], centroids[ids[n-1]]), 0.22,
		"fixture ends must be beyond the merge threshold")

	plan := PlanMerges(singleGroups(centroids), centroids, created, params(0.28, 0.22, 0.30, 3))

	groups := map[uuid.UUID][]uuid.UUID{}
	for _, id := range ids {
		survivor, ok := plan[id]
		if !ok {
			survivor = id
		}
		groups[survivor] = append(groups[survivor], id)
	}
	assert.Greater(t, len(groups), 1, "a chain of %d stories collapsed into %d", n, len(groups))
	for survivor, group := range groups {
		for _, x := range group {
			for _, y := range group {
				assert.LessOrEqual(t, cosDist(centroids[x], centroids[y]), 0.22,
					"story %s merged members %.4f apart", survivor, cosDist(centroids[x], centroids[y]))
			}
		}
	}
}

func TestPlanMerges_DeterministicUnderShuffledOrder(t *testing.T) {
	centroids := map[uuid.UUID][]float32{}
	created := map[uuid.UUID]time.Time{}
	now := time.Now()
	for i := range 6 {
		id := uuid.NewSHA1(ns, []byte{byte(i)})
		centroids[id] = unitAt(float64(i) * 0.04)
		created[id] = now.Add(-time.Duration(i) * time.Minute)
	}
	p := params(0.28, 0.22, 0.30, 3)

	// Map iteration order differs run to run; the plan must not.
	first := PlanMerges(singleGroups(centroids), centroids, created, p)
	for range 5 {
		assert.Equal(t, first, PlanMerges(singleGroups(centroids), centroids, created, p))
	}
}

// A merge may not produce a group the next run would cut apart. The test is the
// split decision itself, not the radius gate.
func TestCompactMergeGroup_RefusesAUnionThatWouldSplit(t *testing.T) {
	p := params(0.28, 0.22, 0.30, 3)
	a := uuid.NewSHA1(ns, []byte("m-a"))
	b := uuid.NewSHA1(ns, []byte("m-b"))
	members := map[uuid.UUID][]Point{
		a: arcPoints("ma", 6, 0.0, 0.05),
		b: arcPoints("mb", 6, 1.2, 0.05),
	}
	union := append(append([]Point{}, members[a]...), members[b]...)
	_, wouldSplit := Split(union, Radius(union), p)
	require.True(t, wouldSplit, "fixture union must be one the split step would cut")

	assert.Less(t, len(CompactMergeGroup([]uuid.UUID{a, b}, members, p)), 2,
		"a union the next run would split must not be merged")
}

// The converse: a union that split leaves whole is merged, even when its radius is
// wide enough that the radius gate alone would have refused it.
func TestCompactMergeGroup_AcceptsAUnionSplitLeavesWhole(t *testing.T) {
	p := params(0.28, 0.22, 0.30, 3)
	a := uuid.NewSHA1(ns, []byte("k-a"))
	b := uuid.NewSHA1(ns, []byte("k-b"))
	members := map[uuid.UUID][]Point{
		a: arcPoints("ka", 8, 0.0, 0.45),
		b: arcPoints("kb", 8, 0.45, 0.45),
	}
	union := append(append([]Point{}, members[a]...), members[b]...)
	_, wouldSplit := Split(union, Radius(union), p)
	require.False(t, wouldSplit, "fixture union must be one the split step leaves whole")
	require.Greater(t, geom.MaxAngularSeparation(Radius(union)), p.Split,
		"fixture must exceed the radius gate, or the test proves nothing")

	assert.Len(t, CompactMergeGroup([]uuid.UUID{a, b}, members, p), 2,
		"a union the next run leaves whole must be allowed to merge")
}

// --- cliques ---

func TestCliques_RequiresMutualAdjacency(t *testing.T) {
	// A path: 0-1-2 adjacent in sequence, but 0 and 2 are not.
	near := func(i, j int) bool { return j-i == 1 || i-j == 1 }
	for _, g := range Cliques(3, near, 2) {
		for _, x := range g {
			for _, y := range g {
				if x != y {
					assert.True(t, near(x, y), "clique members %d and %d are not adjacent", x, y)
				}
			}
		}
	}
}

func TestCliques_BelowMinSizeYieldsNothing(t *testing.T) {
	assert.Empty(t, Cliques(3, func(i, j int) bool { return false }, 2))
}

// --- facet identity and distinct-signal sizing (spec 007) ---

// facetsOf returns n facets of one signal, spread over a tight arc so they
// group readily. Every facet shares an ID and differs only in Facet.
func facetsOf(name string, n int, center, spread float64) []Point {
	id := uuid.NewSHA1(ns, []byte(name))
	out := make([]Point, n)
	for i := range out {
		a := center + spread*(float64(i)/float64(n)-0.5)
		out[i] = Point{ID: id, Facet: i, At: time.Now(), Vec: unitAt(a)}
	}
	return out
}

// The load-bearing rule of spec 007: MinSize counts signals, not points. One
// signal split into MinSize facets is still one signal and must not found a
// group by itself, however tightly its facets cluster.
func TestGrow_OneSignalCannotSatisfyMinSize(t *testing.T) {
	pts := facetsOf("solo", 5, 0.5, 0.02)
	groups := Grow(pts, params(0.28, 0.22, 0.30, 3))
	assert.Empty(t, groups, "five facets of one signal must not form a group")
}

// The same fixture plus enough distinct signals does group, so the rule above
// rejects for the right reason rather than rejecting everything.
func TestGrow_DistinctSignalsSatisfyMinSize(t *testing.T) {
	pts := append(facetsOf("multi", 2, 0.5, 0.02), arcPoints("peer", 2, 0.5, 0.02)...)
	groups := Grow(pts, params(0.28, 0.22, 0.30, 3))
	require.Len(t, groups, 1)
	assert.Len(t, groups[0], 4, "all four facets join once three signals are present")
}

// A split must leave MinSize distinct signals on each side. Facets of one
// signal landing on both sides are legitimate, so the count that matters is of
// signals per side, not of points per side.
func TestSplit_CountsDistinctSignalsPerSide(t *testing.T) {
	// Two well-separated lobes, each holding one facet of every signal. Every
	// signal therefore straddles the cut: 6 facets, but only 3 distinct signals
	// in total, and 3 on each side.
	var pts []Point
	for i, name := range []string{"alpha", "beta", "gamma"} {
		id := uuid.NewSHA1(ns, []byte(name))
		at := time.Now().Add(time.Duration(i) * time.Second)
		pts = append(pts,
			Point{ID: id, Facet: 0, At: at, Vec: unitAt(0.0 + 0.01*float64(i))},
			Point{ID: id, Facet: 1, At: at, Vec: unitAt(math.Pi/2 + 0.01*float64(i))},
		)
	}

	div, ok := Split(pts, Radius(pts), params(0.9, 0.22, 0.30, 3))
	require.True(t, ok, "two orthogonal lobes of three signals each must split")
	assert.Len(t, div.Keep, 3)
	assert.Len(t, div.Spawn, 3)

	// Every signal ends up on both sides — the multi-membership spec 007 exists
	// to produce.
	keep, spawn := idsOf(pts, div.Keep), idsOf(pts, div.Spawn)
	assert.ElementsMatch(t, keep, spawn)
}

// distinctIDs is the single point of enforcement, so it is tested directly.
func TestDistinctIDs(t *testing.T) {
	pts := append(facetsOf("shared", 3, 0.0, 0.01), pointAt("other", 1.0))
	all := []int{0, 1, 2, 3}
	assert.Equal(t, 2, distinctIDs(pts, all))
	assert.Equal(t, 1, distinctIDs(pts, []int{0, 1, 2}))
	assert.Equal(t, 0, distinctIDs(pts, nil))
}

// less is the total order every tie-break resolves against; ID alone stopped
// being one when a signal gained multiple facets.
func TestLess_OrdersByIDThenFacet(t *testing.T) {
	id := uuid.NewSHA1(ns, []byte("same"))
	a := Point{ID: id, Facet: 0}
	b := Point{ID: id, Facet: 1}
	assert.True(t, less(a, b))
	assert.False(t, less(b, a))
	assert.False(t, less(a, a))
}
