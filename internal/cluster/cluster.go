// Package cluster holds the grouping decisions the maintenance pass makes:
// which loose points form a group, when one group is really two, and when two
// groups are really one.
//
// Everything here is a pure function of its inputs. Nothing touches a store, a
// clock, or a configuration struct, and no decision depends on map iteration
// order: point sets are consumed in the order the caller supplies, group sets are
// sorted by ID first, and ties break on the lower index or the lower ID. Two
// callers holding the same points therefore reach the same answer, which is what
// makes a re-run over unchanged data a no-op.
//
// Indices in, indices out. The functions here return positions into the caller's
// own slice rather than the points themselves, so the caller keeps whatever
// richer record it holds alongside each vector.
package cluster

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/geom"
)

// Point is one facet under consideration, with the identity used for
// deterministic tie-breaking and the timestamp used to settle which of two equal
// parts is the older.
//
// ID names the signal the facet belongs to, not the facet itself: several
// Points may share an ID when one signal contributed several facets. Facet
// distinguishes them, and (ID, Facet) is unique. Sizes are therefore counted in
// distinct IDs — see Params.MinSize.
type Point struct {
	ID    uuid.UUID
	Facet int
	At    time.Time
	Vec   []float32
}

// less orders points deterministically by (ID, Facet), which is the total order
// every tie-break in this package resolves against. ID alone stopped being one
// when a signal gained the ability to contribute more than one facet.
func less(a, b Point) bool {
	if a.ID != b.ID {
		return a.ID.String() < b.ID.String()
	}
	return a.Facet < b.Facet
}

// distinctIDs counts the distinct signals the given points belong to. It is the
// measure every size gate uses, so that a single signal splitting itself into
// many facets cannot satisfy a threshold meant to require many signals.
func distinctIDs(pts []Point, idx []int) int {
	seen := make(map[uuid.UUID]struct{}, len(idx))
	for _, i := range idx {
		seen[pts[i].ID] = struct{}{}
	}
	return len(seen)
}

// Params are the thresholds a decision is measured against. All distances are in
// centred space; see package geom.
type Params struct {
	// Assign is the radius a group must fit inside to be one group.
	Assign float64

	// Merge is the centroid distance at or below which two groups are one.
	Merge float64

	// Split is the best-partition centroid distance above which one group is
	// two. It must exceed Merge: the band between them is the hysteresis that
	// stops a merge and a split undoing each other along different seams.
	Split float64

	// MinSize is the number of *distinct signals* a group needs to exist, and
	// which each side of a split needs to be worth cutting. It counts distinct
	// Point.IDs, not points: one signal contributing MinSize facets is one
	// signal, and must not found a story by itself.
	MinSize int
}

// vecsOf is the view the geom helpers take.
func vecsOf(pts []Point) [][]float32 {
	out := make([][]float32, len(pts))
	for i := range pts {
		out[i] = pts[i].Vec
	}
	return out
}

// pickVecs is the same view restricted to a subset.
func pickVecs(pts []Point, idx []int) [][]float32 {
	out := make([][]float32, len(idx))
	for i, j := range idx {
		out[i] = pts[j].Vec
	}
	return out
}

// Centroid returns the centre of the given subset.
func Centroid(pts []Point, idx []int) []float32 { return geom.Centroid(pickVecs(pts, idx)) }

// Radius returns the radius of the whole set.
func Radius(pts []Point) float64 { return geom.Radius(vecsOf(pts)) }

// Grow partitions pts into groups that each fit inside a ball of radius
// p.Assign, returning only those holding at least p.MinSize points.
//
// Each group is seeded on the point with the most neighbours within the radius,
// then extended by repeatedly admitting whichever unused point is nearest the
// *running* centroid while it stays within the radius. The closing compaction
// then drops any member the finished centroid has left outside.
//
// That compaction is what makes non-chaining a guarantee rather than an
// observation: whatever path the centroid took while growing, every survivor
// ends within p.Assign of the final centre, so the group's diameter is bounded
// by geom.MaxAngularSeparation(p.Assign) and a ladder of near-neighbours cannot
// walk out of it.
//
// Nearest-centroid growth replaced mutual-neighbour cliques, which were
// measurably too strict for real data: a news cluster of radius 0.16 has extremes
// twice that apart, so all-pairs adjacency shattered single topics. Connected
// components remain out of the question — they are the transitive linkage that
// collapses a corpus into one group.
func Grow(pts []Point, p Params) [][]int {
	n := len(pts)
	// Facet count is a valid cheap pre-gate: distinct signals never exceed it,
	// so too few facets means too few signals. The authoritative distinct-signal
	// test runs on the finished group below.
	if n < p.MinSize {
		return nil
	}

	degree := make([]int, n)
	for i := range n {
		for j := i + 1; j < n; j++ {
			if dist.CosineDistanceUnit(pts[i].Vec, pts[j].Vec) <= p.Assign {
				degree[i]++
				degree[j]++
			}
		}
	}

	used := make([]bool, n)
	var out [][]int
	for {
		seed := -1
		for i := range n {
			if !used[i] && (seed == -1 || degree[i] > degree[seed]) {
				seed = i
			}
		}
		if seed == -1 || degree[seed] < p.MinSize-1 {
			break
		}

		group := []int{seed}
		inGroup := make([]bool, n)
		inGroup[seed] = true
		centre := pts[seed].Vec
		for {
			best, bestD := -1, p.Assign
			for j := range n {
				if used[j] || inGroup[j] {
					continue
				}
				if d := dist.CosineDistanceUnit(pts[j].Vec, centre); d <= bestD {
					best, bestD = j, d
				}
			}
			if best < 0 {
				break
			}
			group = append(group, best)
			inGroup[best] = true
			centre = Centroid(pts, group)
		}

		group = CompactToRadius(pts, group, p.Assign)
		if distinctIDs(pts, group) < p.MinSize {
			// This seed anchors nothing; retire it rather than looping on it.
			used[seed] = true
			degree[seed] = -1
			continue
		}
		for _, i := range group {
			used[i] = true
			degree[i] = -1
		}
		out = append(out, group)
	}
	return out
}

// CompactToRadius drops members lying beyond radius of the group's centroid,
// recentring after each round until every survivor is inside.
func CompactToRadius(pts []Point, group []int, radius float64) []int {
	for len(group) > 1 {
		centre := Centroid(pts, group)
		worst, worstD := -1, radius
		for gi, i := range group {
			if d := dist.CosineDistanceUnit(pts[i].Vec, centre); d > worstD {
				worst, worstD = gi, d
			}
		}
		if worst < 0 {
			return group
		}
		group = append(group[:worst:worst], group[worst+1:]...)
	}
	return group
}

// Cliques partitions [0, n) into groups whose members are all mutually adjacent
// under near, returning only those of at least minSize.
//
// Used for grouping whole stories, where the population is small and the chaining
// risk is real: a ladder of 12 stories each 0.005 from its neighbour once merged
// into one whose ends were 0.55 apart.
//
// Maximal-clique enumeration is exponential, so this is the standard greedy
// approximation: take the highest-degree unused vertex, extend with neighbours
// adjacent to everything already chosen, remove the group, repeat. Callers pass
// indices in a stable order and ties resolve to the lower index, so the result is
// deterministic.
func Cliques(n int, near func(i, j int) bool, minSize int) [][]int {
	adj := make([][]bool, n)
	degree := make([]int, n)
	for i := range adj {
		adj[i] = make([]bool, n)
	}
	for i := range n {
		for j := i + 1; j < n; j++ {
			if near(i, j) {
				adj[i][j], adj[j][i] = true, true
				degree[i]++
				degree[j]++
			}
		}
	}

	drop := func(i int, used []bool) {
		used[i] = true
		for j := range n {
			if adj[i][j] {
				degree[j]--
			}
		}
		degree[i] = -1
	}

	used := make([]bool, n)
	var out [][]int
	for {
		seed := -1
		for i := range n {
			if used[i] {
				continue
			}
			if seed == -1 || degree[i] > degree[seed] {
				seed = i
			}
		}
		if seed == -1 || degree[seed] < minSize-1 {
			break
		}

		group := []int{seed}
		for j := range n {
			if used[j] || j == seed || !adj[seed][j] {
				continue
			}
			mutual := true
			for _, g := range group {
				if g != j && !adj[g][j] {
					mutual = false
					break
				}
			}
			if mutual {
				group = append(group, j)
			}
		}

		if len(group) < minSize {
			// This seed cannot anchor a group; retire it rather than looping.
			drop(seed, used)
			continue
		}
		for _, g := range group {
			drop(g, used)
		}
		out = append(out, group)
	}
	return out
}

// Division is an accepted two-way split of a group.
type Division struct {
	Keep  []int // stays under the existing identity
	Spawn []int // moves to a new one
}

// Split tests whether pts holds two groups that p.Split says are separate, and
// returns the division if so. radius is the group's radius, which the caller
// usually has already.
//
// A necessary condition runs first, so most groups cost almost nothing:
// geom.MaxAngularSeparation(radius) must exceed p.Split. When it does not, no
// partition can clear the bar, so skipping cannot miss a split.
//
// Past the gate, the group is partitioned by a two-medoid Lloyd loop seeded on
// its two most distant members and bounded to ten iterations. The split is
// accepted only if both parts hold p.MinSize points and the two part centroids
// are more than p.Split apart — a group with no internal gap is left whole,
// since cutting it would produce halves inside the hysteresis band with nothing
// to reunite them.
//
// The larger part keeps the identity, so it survives for the majority of
// holders; ties go to the part holding the older point.
func Split(pts []Point, radius float64, p Params) (Division, bool) {
	// A cheap pre-gate in facet terms, deliberately not in distinct signals.
	// Each side needs p.MinSize distinct signals and therefore at least that
	// many facets, so fewer than 2*p.MinSize facets cannot divide — but the
	// distinct-signal count may legitimately be lower than 2*p.MinSize, since
	// one signal can put facets on both sides of the cut. Gating on distinct
	// IDs here would reject exactly those splits.
	if len(pts) < 2*p.MinSize {
		return Division{}, false
	}
	if geom.MaxAngularSeparation(radius) <= p.Split {
		return Division{}, false
	}

	a, b := TwoMedoids(pts)
	if a < 0 {
		return Division{}, false
	}

	var left, right []int
	for range 10 {
		left, right = left[:0], right[:0]
		for i := range pts {
			if dist.CosineDistanceUnit(pts[i].Vec, pts[a].Vec) <=
				dist.CosineDistanceUnit(pts[i].Vec, pts[b].Vec) {
				left = append(left, i)
			} else {
				right = append(right, i)
			}
		}
		if len(left) == 0 || len(right) == 0 {
			return Division{}, false
		}
		na, nb := medoidOf(pts, left), medoidOf(pts, right)
		if na == a && nb == b {
			break
		}
		a, b = na, nb
	}

	if distinctIDs(pts, left) < p.MinSize || distinctIDs(pts, right) < p.MinSize {
		return Division{}, false
	}
	if dist.CosineDistanceUnit(Centroid(pts, left), Centroid(pts, right)) <= p.Split {
		return Division{}, false
	}

	keep, spawn := left, right
	switch {
	case len(right) > len(left):
		keep, spawn = right, left
	case len(right) == len(left) && earliest(pts, right).Before(earliest(pts, left)):
		keep, spawn = right, left
	}
	return Division{Keep: keep, Spawn: spawn}, true
}

// TwoMedoids returns the indices of the two mutually most distant points, used
// as the partition seeds. Ties break on point ID.
func TwoMedoids(pts []Point) (int, int) {
	bestA, bestB, best := -1, -1, -1.0
	for i := range pts {
		for j := i + 1; j < len(pts); j++ {
			d := dist.CosineDistanceUnit(pts[i].Vec, pts[j].Vec)
			if d > best || (d == best && bestA >= 0 && less(pts[i], pts[bestA])) {
				bestA, bestB, best = i, j, d
			}
		}
	}
	return bestA, bestB
}

// medoidOf returns the index of the part member with the smallest total distance
// to the rest of the part.
func medoidOf(pts []Point, part []int) int {
	best, bestSum := -1, 0.0
	for _, i := range part {
		var sum float64
		for _, j := range part {
			sum += dist.CosineDistanceUnit(pts[i].Vec, pts[j].Vec)
		}
		if best == -1 || sum < bestSum ||
			(sum == bestSum && less(pts[i], pts[best])) {
			best, bestSum = i, sum
		}
	}
	return best
}

// earliest returns the oldest timestamp in the part.
func earliest(pts []Point, part []int) time.Time {
	var out time.Time
	for k, i := range part {
		if k == 0 || pts[i].At.Before(out) {
			out = pts[i].At
		}
	}
	return out
}

// MergePlan maps each retired group to the group that absorbs it.
type MergePlan map[uuid.UUID]uuid.UUID

// PlanMerges groups the given sets by centroid proximity and picks the oldest of
// each group as the survivor. Two conditions, both necessary:
//
//  1. Clique, not component. Every member must be within p.Merge of every other.
//     An earlier version used union-find, reasoning that a group-level chain
//     would stay short because there are far fewer groups than points. That was
//     wrong; see Cliques.
//
//  2. The merged group must not be born splittable. Centroid proximity alone
//     ignores spread: two groups of radius 0.25 whose centroids are 0.2 apart
//     produce a union spanning far more than either. Unchecked, this compounds
//     across runs — the merged centroid shifts toward what it just absorbed,
//     reaching the next neighbour — until everything is one group.
func PlanMerges(
	members map[uuid.UUID][]Point,
	centroids map[uuid.UUID][]float32,
	created map[uuid.UUID]time.Time,
	p Params,
) MergePlan {

	sorted := make([]uuid.UUID, 0, len(centroids))
	for id := range centroids {
		sorted = append(sorted, id)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].String() < sorted[j].String() })

	groups := Cliques(len(sorted), func(i, j int) bool {
		ca, cb := centroids[sorted[i]], centroids[sorted[j]]
		if len(ca) == 0 || len(cb) == 0 {
			return false
		}
		return dist.CosineDistanceUnit(ca, cb) <= p.Merge
	}, 2)

	plan := make(MergePlan)
	for _, g := range groups {
		ids := make([]uuid.UUID, 0, len(g))
		for _, i := range g {
			ids = append(ids, sorted[i])
		}
		ids = CompactMergeGroup(ids, members, p)
		if len(ids) < 2 {
			continue
		}

		survivor := ids[0]
		for _, id := range ids[1:] {
			switch {
			case created[id].Before(created[survivor]):
				survivor = id
			case created[id].Equal(created[survivor]) && id.String() < survivor.String():
				survivor = id
			}
		}
		for _, id := range ids {
			if id != survivor {
				plan[id] = survivor
			}
		}
	}
	return plan
}

// CompactMergeGroup shrinks a candidate merge group until the group it would
// produce is one Split would leave whole, dropping the member furthest from the
// union centroid each round. It returns fewer than two IDs when no compact subset
// survives, meaning no merge happens at all.
//
// The test is Split on the union, not Split's radius gate. The gate is only a
// necessary condition, so using it here rejects unions no split would ever
// touch: at a Split threshold of 0.55 it demands a union radius under 0.15,
// tighter than a real group ever is, and no merge fires at all. Asking the split
// decision directly makes merge and split exact inverses.
func CompactMergeGroup(ids []uuid.UUID, members map[uuid.UUID][]Point, p Params) []uuid.UUID {
	for len(ids) >= 2 {
		var union []Point
		for _, id := range ids {
			union = append(union, members[id]...)
		}
		if len(union) == 0 {
			return nil
		}
		if _, wouldSplit := Split(union, Radius(union), p); !wouldSplit {
			return ids
		}

		// Drop the group whose centroid sits furthest from the union centre.
		c := geom.Centroid(vecsOf(union))
		worst, worstD := -1, -1.0
		for i, id := range ids {
			if len(members[id]) == 0 {
				continue
			}
			mc := geom.Centroid(vecsOf(members[id]))
			if d := dist.CosineDistanceUnit(mc, c); d > worstD {
				worst, worstD = i, d
			}
		}
		if worst < 0 {
			return nil
		}
		ids = append(ids[:worst:worst], ids[worst+1:]...)
	}
	return nil
}
