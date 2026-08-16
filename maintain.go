package story

import (
	"math"
	"sort"
	"time"

	"github.com/google/uuid"

	"go.kvsh.ch/streaming-story/internal/dist"
)

// This file implements the maintenance operations that replaced batch
// re-clustering (spec 006). Each is a pure function of the collected state so
// that a run is reproducible: no map iteration order leaks into a decision,
// and ties break on signal or story ID.

// centroidOf returns the unweighted mean of the members' embeddings.
func centroidOf(members []*batchSignal) []float32 {
	if len(members) == 0 {
		return nil
	}
	c := make([]float32, len(members[0].emb))
	for _, m := range members {
		for d, v := range m.emb {
			c[d] += v
		}
	}
	n := float32(len(members))
	for d := range c {
		c[d] /= n
	}
	return c
}

// promotion is a group of outliers that has earned story status.
type promotion struct {
	members []*batchSignal
}

// greedyCliques partitions [0, n) into groups whose members are all mutually
// adjacent under near, returning only groups of at least minSize.
//
// Cliques rather than connected components, everywhere. Components are
// transitive: a ladder whose neighbours are each 0.005 apart forms one
// component even when its ends are 0.55 apart, which is the chaining that
// collapses a corpus into a single blob. Requiring mutual adjacency keeps a
// group as tight as its threshold claims.
//
// Maximal-clique enumeration is exponential, so this is the standard greedy
// approximation: take the highest-degree unused vertex, extend with neighbours
// that are adjacent to everything already chosen, remove the group, repeat.
// Callers pass indices in a stable order, and ties resolve to the lower index,
// so the result is deterministic.
func greedyCliques(n int, near func(i, j int) bool, minSize int) [][]int {
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

// promoteOutliers groups outliers into new stories by mutual-neighbour
// cliques, so a promoted story is compact from birth rather than a chain.
func (t *Tracker[T]) promoteOutliers(outliers []*batchSignal) []promotion {
	if len(outliers) < t.cfg.MinStorySize {
		return nil
	}

	cand := make([]*batchSignal, len(outliers))
	copy(cand, outliers)
	sort.Slice(cand, func(i, j int) bool {
		return cand[i].id.String() < cand[j].id.String()
	})

	groups := greedyCliques(len(cand), func(i, j int) bool {
		return dist.CosineDistance(cand[i].emb, cand[j].emb) <= t.cfg.AssignThreshold
	}, t.cfg.MinStorySize)

	out := make([]promotion, 0, len(groups))
	for _, g := range groups {
		members := make([]*batchSignal, 0, len(g))
		for _, i := range g {
			members = append(members, cand[i])
		}
		out = append(out, promotion{members: members})
	}
	return out
}

// splitResult describes an accepted two-way division of a story.
type splitResult struct {
	keep  []*batchSignal // stays under the existing story ID
	spawn []*batchSignal // moves to a newly created story
}

// splitStory tests whether a story holds two groups that SplitThreshold says
// are separate stories, and returns the division if so.
//
// The radius gate is a sound necessary condition, not a heuristic, but the
// bound is not the Euclidean 2*radius: cosine distance is not a metric, and
// 1-cos grows quadratically in the angle, so two points each at angular
// distance a from the centroid can be up to 1-cos(2a) apart rather than
// 2*(1-cos(a)).
//
// With cos(a) = 1-radius, that ceiling works out to 4*radius - 2*radius^2.
// When it does not exceed SplitThreshold no partition can clear the bar, so
// skipping the story cannot miss a split. Using 2*radius here would skip
// stories that do split -- a point pair at +/-0.5 rad sits 0.122 from the
// centroid but 0.46 from its opposite.
func (t *Tracker[T]) splitStory(members []*batchSignal, radius float64) (splitResult, bool) {
	if len(members) < 2*t.cfg.MinStorySize {
		return splitResult{}, false
	}
	if maxAngularSeparation(radius) <= t.cfg.SplitThreshold {
		return splitResult{}, false
	}

	a, b := twoMedoids(members)
	if a < 0 {
		return splitResult{}, false
	}

	var left, right []*batchSignal
	for range 10 {
		left, right = left[:0], right[:0]
		for _, m := range members {
			if dist.CosineDistance(m.emb, members[a].emb) <= dist.CosineDistance(m.emb, members[b].emb) {
				left = append(left, m)
			} else {
				right = append(right, m)
			}
		}
		if len(left) == 0 || len(right) == 0 {
			return splitResult{}, false
		}
		na, nb := medoidOf(members, left), medoidOf(members, right)
		if na == a && nb == b {
			break
		}
		a, b = na, nb
	}

	if len(left) < t.cfg.MinStorySize || len(right) < t.cfg.MinStorySize {
		return splitResult{}, false
	}
	if dist.CosineDistance(centroidOf(left), centroidOf(right)) <= t.cfg.SplitThreshold {
		// Diffuse, not bimodal. A story with no internal gap is left whole;
		// cutting it would produce halves that sit inside the hysteresis band
		// with nothing to reunite them.
		return splitResult{}, false
	}

	// The larger part keeps the story ID so identity survives for the
	// majority of holders. Ties go to the part with the older signal.
	keep, spawn := left, right
	switch {
	case len(right) > len(left):
		keep, spawn = right, left
	case len(right) == len(left) && earliest(right).Before(earliest(left)):
		keep, spawn = right, left
	}
	return splitResult{keep: keep, spawn: spawn}, true
}

// maxAngularSeparation returns the largest cosine distance possible between
// two directions that both lie within cosine distance r of a common centre.
// It is 1-cos(2*arccos(1-r)), expanded to avoid the trigonometry.
func maxAngularSeparation(r float64) float64 {
	if r <= 0 {
		return 0
	}
	if r >= 1 {
		return 2
	}
	return 4*r - 2*r*r
}

// twoMedoids returns the indices of the two mutually most distant members,
// used as the partition seeds. Ties break on signal ID.
func twoMedoids(members []*batchSignal) (int, int) {
	bestA, bestB, best := -1, -1, -1.0
	for i := range members {
		for j := i + 1; j < len(members); j++ {
			d := dist.CosineDistance(members[i].emb, members[j].emb)
			if d > best || (d == best && bestA >= 0 &&
				members[i].id.String() < members[bestA].id.String()) {
				bestA, bestB, best = i, j, d
			}
		}
	}
	return bestA, bestB
}

// medoidOf returns the index into members of the part element with the
// smallest total distance to the rest of the part.
func medoidOf(members []*batchSignal, part []*batchSignal) int {
	index := make(map[uuid.UUID]int, len(members))
	for i, m := range members {
		index[m.id] = i
	}
	best, bestSum := -1, 0.0
	for _, c := range part {
		var sum float64
		for _, o := range part {
			sum += dist.CosineDistance(c.emb, o.emb)
		}
		if best == -1 || sum < bestSum ||
			(sum == bestSum && c.id.String() < part[0].id.String()) {
			best, bestSum = index[c.id], sum
		}
	}
	return best
}

// earliest returns the oldest signal time in the group.
func earliest(members []*batchSignal) time.Time {
	var out time.Time
	for i, m := range members {
		if i == 0 || m.at.Before(out) {
			out = m.at
		}
	}
	return out
}

// mergePlan maps each retired story to the story that absorbs it.
type mergePlan map[uuid.UUID]uuid.UUID

// planMerges groups stories that are close enough to be one story and picks
// the oldest of each group as the survivor.
//
// Two conditions, both necessary:
//
//  1. **Clique, not component.** Every member is within threshold of every
//     other. An earlier version used union-find, reasoning that a story-level
//     chain would be bounded because there are far fewer stories than signals.
//     That was wrong: a ladder of 12 stories each 0.005 from its neighbour
//     merged into one whose ends were 0.55 apart.
//
//  2. **The merged story must not be born splittable.** Centroid proximity
//     alone ignores spread: two stories of radius 0.25 whose centroids are 0.2
//     apart produce a union spanning far more than either. Left unchecked this
//     compounds across runs -- the merged centroid shifts toward what it just
//     absorbed, reaching the next neighbour, and a corpus collapses into one
//     story over successive batches. Requiring the union to fall below the
//     split gate makes merge and split exact inverses: a merge may not create
//     something the next run would want to cut apart.
func (t *Tracker[T]) planMerges(
	members map[uuid.UUID][]*batchSignal,
	centroids map[uuid.UUID][]float32,
	created map[uuid.UUID]time.Time,
) mergePlan {

	sorted := make([]uuid.UUID, 0, len(centroids))
	for id := range centroids {
		sorted = append(sorted, id)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].String() < sorted[j].String() })

	groups := greedyCliques(len(sorted), func(i, j int) bool {
		ca, cb := centroids[sorted[i]], centroids[sorted[j]]
		if len(ca) == 0 || len(cb) == 0 {
			return false
		}
		return dist.CosineDistance(ca, cb) <= t.cfg.MergeThreshold
	}, 2)

	plan := make(mergePlan)
	for _, g := range groups {
		ids := make([]uuid.UUID, 0, len(g))
		for _, i := range g {
			ids = append(ids, sorted[i])
		}
		ids = t.compactMergeGroup(ids, members)
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

// compactMergeGroup shrinks a candidate merge group until the story it would
// produce falls below the split gate, dropping the member furthest from the
// union centroid each round. It returns a group of fewer than two when no
// compact subset survives, meaning no merge happens at all.
func (t *Tracker[T]) compactMergeGroup(ids []uuid.UUID, members map[uuid.UUID][]*batchSignal) []uuid.UUID {
	for len(ids) >= 2 {
		var union []*batchSignal
		for _, id := range ids {
			union = append(union, members[id]...)
		}
		if len(union) == 0 {
			return nil
		}
		if maxAngularSeparation(radiusOf(union)) <= t.cfg.SplitThreshold {
			return ids
		}

		// Drop the story whose centroid sits furthest from the union centre.
		c := centroidOf(union)
		worst, worstD := -1, -1.0
		for i, id := range ids {
			if len(members[id]) == 0 {
				continue
			}
			if d := dist.CosineDistance(centroidOf(members[id]), c); d > worstD {
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

// migrateSignals moves every signal key under from into to.
func migrateSignals(tx Tx, from, to uuid.UUID) error {
	prefix := keySignalPrefix(from)
	return tx.ScanPrefix(prefix, func(key, val []byte) error {
		sigID, ok := parseSignalKey(key, prefix)
		if !ok {
			return nil
		}
		return moveSignal(tx, key, keySignal(to, sigID))
	})
}

// applyMaintenance runs the whole maintenance pass in one write transaction:
// evict, promote, split, merge, recentre, lifecycle. It is the replacement for
// the cluster/map/apply pipeline (spec 006).
func (t *Tracker[T]) applyMaintenance(
	signals []batchSignal,
	stories map[uuid.UUID]storyRecord,
	evict []uuid.UUID,
	now time.Time,
) (*BatchSummary, []StoryEvent[T], error) {

	summary := &BatchSummary{OutliersEvicted: len(evict)}
	var events []StoryEvent[T]

	err := t.cfg.Store.Update(func(tx Tx) error {
		// Membership as it stands, keyed by story. Outliers are held apart.
		members := make(map[uuid.UUID][]*batchSignal, len(stories))
		var outliers []*batchSignal
		evicted := make(map[uuid.UUID]bool, len(evict))
		for _, id := range evict {
			evicted[id] = true
		}
		for i := range signals {
			sig := &signals[i]
			if sig.outlier {
				if !evicted[sig.id] {
					outliers = append(outliers, sig)
				}
				continue
			}
			members[sig.storyID] = append(members[sig.storyID], sig)
		}

		// 1. Evict stale outliers.
		for _, id := range evict {
			if err := tx.Delete(keyOutlier(id)); err != nil {
				return err
			}
			if err := tx.Delete(keySignalLoc(id)); err != nil {
				return err
			}
		}

		// 2. Promote outlier cliques into new stories. Runs before split and
		// merge so a promotion landing on top of an existing story is unified
		// in the same run instead of lingering as a near-duplicate.
		created := make(map[uuid.UUID]time.Time, len(stories))
		for id, rec := range stories {
			created[id] = rec.CreatedAt
		}
		for _, p := range t.promoteOutliers(outliers) {
			sid := uuid.New()
			for _, m := range p.members {
				if err := moveSignal(tx, keyOutlier(m.id), keySignal(sid, m.id)); err != nil {
					return err
				}
				m.storyID = sid
				m.outlier = false
			}
			members[sid] = p.members
			created[sid] = now
			summary.OutliersPromoted += len(p.members)
			summary.StoriesCreated++
			events = append(events, StoryEvent[T]{Kind: EventStoryCreated, StoryID: sid, At: now})
			for _, m := range p.members {
				events = append(events, StoryEvent[T]{
					Kind: EventSignalReassigned, StoryID: sid, SignalID: m.id, At: now,
				})
			}
		}

		// 3. Split stories that now hold two groups. At most one split per
		// story per run: a three-group story resolves over successive runs
		// rather than by unbounded recursion, which would be re-clustering
		// under another name.
		for _, sid := range sortedIDs(members) {
			group := members[sid]
			if len(group) == 0 {
				continue
			}
			radius := radiusOf(group)
			res, ok := t.splitStory(group, radius)
			if !ok {
				continue
			}
			child := uuid.New()
			for _, m := range res.spawn {
				if err := moveSignal(tx, keySignal(sid, m.id), keySignal(child, m.id)); err != nil {
					return err
				}
				m.storyID = child
			}
			members[sid] = res.keep
			members[child] = res.spawn
			created[child] = now
			summary.StoriesSplit++
			events = append(events, StoryEvent[T]{
				Kind: EventStorySplit, StoryID: sid, StoryID2: child, At: now,
			})
			for _, m := range res.spawn {
				events = append(events, StoryEvent[T]{
					Kind: EventSignalReassigned, StoryID: child, SignalID: m.id, At: now,
				})
			}
		}

		// 4. Merge stories that have converged, measured on post-split
		// centroids so a split and a merge in one run see consistent state.
		centroids := make(map[uuid.UUID][]float32, len(members))
		for _, sid := range sortedIDs(members) {
			if len(members[sid]) == 0 {
				continue
			}
			centroids[sid] = centroidOf(members[sid])
		}
		plan := t.planMerges(members, centroids, created)
		for _, retired := range sortedPlanKeys(plan) {
			survivor := plan[retired]
			// Migrate the full key space, including signals older than
			// BatchWindow: a merge is a key-space migration and is the
			// documented exception to the stability rule.
			if err := migrateSignals(tx, retired, survivor); err != nil {
				return err
			}
			if rec, ok := stories[retired]; ok && !rec.LastSignalAt.IsZero() {
				if err := tx.Delete(keyTimeIndex(rec.LastSignalAt.Unix(), retired)); err != nil {
					return err
				}
			}
			if err := tx.Delete(keyStoryMeta(retired)); err != nil {
				return err
			}
			for _, m := range members[retired] {
				m.storyID = survivor
			}
			members[survivor] = append(members[survivor], members[retired]...)
			delete(members, retired)
			delete(stories, retired)
			summary.StoriesMerged++
			events = append(events, StoryEvent[T]{
				Kind: EventStoryMerged, StoryID: survivor, StoryID2: retired, At: now,
			})
		}

		// 5. Recentre and advance lifecycle for every surviving story.
		var ema emaAccum
		for _, sid := range sortedIDs(members) {
			prev, existing := stories[sid]
			if !existing {
				prev = storyRecord{CreatedAt: created[sid]}
			}
			if err := t.recentreStory(tx, sid, prev, existing, members[sid], &ema, summary, &events, now); err != nil {
				return err
			}
		}

		// 6. Global calibration.
		t.calibMu.Lock()
		if ema.count > 0 {
			mean := ema.sum / float64(ema.count)
			if t.sigmaGlobal == 0 {
				t.sigmaGlobal = mean
			} else {
				t.sigmaGlobal = t.cfg.EMAAlpha*t.sigmaGlobal + (1-t.cfg.EMAAlpha)*mean
			}
		}
		t.lastBatch = now
		t.calibMu.Unlock()
		return t.saveCalibState(tx)
	})
	if err != nil {
		return nil, nil, err
	}
	return summary, events, nil
}

// recentreStory recomputes a story's geometry from its current membership and
// writes the record. Both centroids are recomputed from members rather than
// updated incrementally: an accumulated mean depends on arrival order and run
// count, so two stores holding identical signals would diverge and every
// threshold comparison downstream with them.
func (t *Tracker[T]) recentreStory(
	tx Tx, sid uuid.UUID, prev storyRecord, existing bool,
	group []*batchSignal, ema *emaAccum,
	summary *BatchSummary, events *[]StoryEvent[T], now time.Time,
) error {
	rec := prev

	if len(group) == 0 {
		// Nothing left: the story was emptied by a merge of its last signals
		// or by outlier eviction. An empty record is meaningless.
		if !existing {
			return nil
		}
		if !prev.LastSignalAt.IsZero() {
			if err := tx.Delete(keyTimeIndex(prev.LastSignalAt.Unix(), sid)); err != nil {
				return err
			}
		}
		if err := tx.Delete(keyStoryMeta(sid)); err != nil {
			return err
		}
		summary.StoriesRetired++
		*events = append(*events, StoryEvent[T]{Kind: EventStoryRetired, StoryID: sid, At: now})
		return nil
	}

	centroid := centroidOf(group)
	rec.Centroid = centroid
	rec.RecentCentroid = recentCentroidOf(group, now.Add(-t.cfg.ActiveContextWindow), centroid)
	rec.SignalCount = len(group)

	var sum, sumSq, radius float64
	dists := make([]float64, 0, len(group))
	var latestAt time.Time
	for _, m := range group {
		d := dist.CosineDistance(m.emb, centroid)
		dists = append(dists, d)
		sum += d
		sumSq += d * d
		if d > radius {
			radius = d
		}
		if m.at.After(latestAt) {
			latestAt = m.at
		}
	}
	n := float64(len(group))
	meanDist := sum / n
	variance := sumSq/n - meanDist*meanDist
	if variance < 0 {
		variance = 0
	}
	rec.Radius = radius
	rec.MeanDistance = meanDist
	rec.Sigma = math.Sqrt(variance)
	rec.LastSignalAt = latestAt
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}

	var newState StoryState
	switch {
	case latestAt.Before(now.Add(-t.cfg.ArchiveWindow)):
		newState = StoryStateArchived
	case latestAt.Before(now.Add(-t.cfg.SilenceWindow)):
		newState = StoryStateDormant
	default:
		newState = StoryStateActive
	}
	rec.State = newState

	switch {
	case newState == StoryStateDormant && prev.State != StoryStateDormant:
		rec.FrozenMeanDistance = prev.MeanDistance
		rec.FrozenSigma = prev.Sigma
	case newState == StoryStateActive:
		rec.FrozenMeanDistance = 0
		rec.FrozenSigma = 0
	}

	if newState == StoryStateActive {
		for _, d := range dists {
			ema.add(d)
		}
	}
	if newState == StoryStateDormant && prev.State != StoryStateDormant {
		*events = append(*events, StoryEvent[T]{Kind: EventStoryDormant, StoryID: sid, At: now})
	}
	if newState == StoryStateArchived && prev.State != StoryStateArchived {
		*events = append(*events, StoryEvent[T]{Kind: EventStoryArchived, StoryID: sid, At: now})
	}

	oldLastAt := prev.LastSignalAt
	if !existing {
		oldLastAt = time.Time{}
	}
	return t.writeStoryMeta(tx, sid, oldLastAt, rec)
}

// recentCentroidOf returns the mean of members at or after cutoff, falling
// back to the supplied lifetime centroid when none qualify.
func recentCentroidOf(group []*batchSignal, cutoff time.Time, fallback []float32) []float32 {
	recent := make([]*batchSignal, 0, len(group))
	for _, m := range group {
		if !m.at.Before(cutoff) {
			recent = append(recent, m)
		}
	}
	if len(recent) == 0 {
		return fallback
	}
	return centroidOf(recent)
}

// radiusOf returns the maximum distance from the group's centroid, which is
// the quantity the split gate tests.
func radiusOf(group []*batchSignal) float64 {
	c := centroidOf(group)
	var r float64
	for _, m := range group {
		if d := dist.CosineDistance(m.emb, c); d > r {
			r = d
		}
	}
	return r
}

// sortedIDs returns the map's keys in a stable order so that iteration never
// leaks map ordering into a decision.
func sortedIDs(m map[uuid.UUID][]*batchSignal) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// sortedPlanKeys returns the merge plan's retired story IDs in a stable order.
func sortedPlanKeys(p mergePlan) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(p))
	for id := range p {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
