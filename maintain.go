package story

import (
	"bytes"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/cluster"
	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/keys"
)

// The maintenance pass: evict, promote, admit, split, merge, recentre,
// lifecycle. Grouping decisions come from internal/cluster; this file applies
// them to the store and emits the events.

// promotion is a group of outliers that has earned story status.
type promotion struct {
	members []*batchSignal
}

// deriveStoryID derives a new story's ID from the signals that founded it, as
// UUID v5 under the tracker namespace. Story IDs are therefore a function of
// the input stream alone: replaying the same signals against a fresh store
// reproduces the same IDs, so a recorded run can be diffed against a replay.
//
// seed separates the two ways a story is born, promotion out of the outlier
// bucket and a split off a parent, so one member set reaching story status by
// different routes does not claim a single ID.
//
// An ID already in use is rejected and rederived with the next salt. A split
// can spawn exactly the member set some live story was founded on, and reusing
// that ID would silently fold the two together. Salting stays deterministic:
// a replay meets the same occupied IDs in the same order and takes the same
// number of steps. taken carries the stories of this run that hold an ID
// without having written metadata yet.
func deriveStoryID(
	tx Tx,
	ns uuid.UUID,
	seed string,
	members []*batchSignal,
	taken map[uuid.UUID]time.Time,
) (uuid.UUID, error) {

	ids := make([]uuid.UUID, len(members))
	for i, m := range members {
		ids[i] = m.id
	}
	sort.Slice(ids, func(i, j int) bool {
		return bytes.Compare(ids[i][:], ids[j][:]) < 0
	})

	for salt := 0; ; salt++ {
		name := make([]byte, 0, len(seed)+1+16*len(ids)+8)
		name = append(name, seed...)
		name = append(name, 0)
		for _, id := range ids {
			name = append(name, id[:]...)
		}
		if salt > 0 {
			name = append(name, 0)
			name = strconv.AppendInt(name, int64(salt), 10)
		}

		id := uuid.NewSHA1(ns, name)
		if _, ok := taken[id]; ok {
			continue
		}
		// Archived stories are left out of the batch's story map, so the
		// store is the authority on whether an ID is free.
		val, err := tx.Get(keys.StoryMeta(id))
		if err != nil {
			return uuid.Nil, err
		}
		if val == nil {
			return id, nil
		}
	}
}

// promoteOutliers groups outliers into new stories by nearest-centroid growth,
// so a promoted story is compact from birth rather than a chain. Candidates are
// sorted by ID first, which is what makes the grouping independent of the order
// collection happened to read them in.
func (t *Tracker[T]) promoteOutliers(outliers []*batchSignal) []promotion {
	if len(outliers) < t.cfg.MinStorySize {
		return nil
	}

	cand := make([]*batchSignal, len(outliers))
	copy(cand, outliers)
	sort.Slice(cand, func(i, j int) bool {
		return cand[i].id.String() < cand[j].id.String()
	})

	groups := cluster.Grow(clusterPoints(cand), t.clusterParams())
	out := make([]promotion, 0, len(groups))
	for _, g := range groups {
		out = append(out, promotion{members: pickSignals(cand, g)})
	}
	return out
}

// admission records an outlier joining an existing story.
type admission struct {
	sig     *batchSignal
	storyID uuid.UUID
}

// admitOutliers assigns leftover outliers to the nearest existing story whose
// adaptive threshold accepts them.
//
// Without this step an outlier that no group claimed sits in the bucket until
// its TTL expires, even when an established story already covers it. The Draft
// phase cannot recover it either: Draft runs once, at ingest, which for the
// first signals of a corpus is before the batch that creates the story they
// belong to. Measured on the 596-signal corpus, 342 signals were stranded this
// way.
//
// Admission is not the individual signal reassignment the stability rule
// forbids. That rule protects story *membership* from being reshuffled run to
// run; an outlier has no membership to disturb, and admission is the same
// nearest-centroid test the Draft phase would have applied had the story existed
// at ingest.
//
// Every threshold and centroid is computed once, from pre-admission membership,
// so the outcome does not depend on the order outliers are visited and no
// admission can widen a story enough to admit the next -- the chaining a moving
// per-signal centroid would reintroduce.
func (t *Tracker[T]) admitOutliers(
	members map[uuid.UUID][]*batchSignal,
	outliers []*batchSignal,
	now time.Time,
) []admission {
	if len(outliers) == 0 || len(members) == 0 {
		return nil
	}

	type anchor struct {
		id     uuid.UUID
		centre []float32
		thresh float64
	}
	cutoff := now.Add(-t.cfg.ActiveContextWindow)
	anchors := make([]anchor, 0, len(members))
	for _, sid := range sortedIDs(members) {
		group := members[sid]
		if len(group) == 0 {
			continue
		}
		st := measure(group)
		anchors = append(anchors, anchor{
			id:     sid,
			centre: recentCentroidOf(group, cutoff, st.Centroid),
			thresh: t.admissionThreshold(st, len(group)),
		})
	}
	if len(anchors) == 0 {
		return nil
	}

	cand := make([]*batchSignal, len(outliers))
	copy(cand, outliers)
	sort.Slice(cand, func(i, j int) bool { return cand[i].id.String() < cand[j].id.String() })

	var out []admission
	for _, sig := range cand {
		best, bestD := -1, math.MaxFloat64
		for i, a := range anchors {
			d := dist.CosineDistance(sig.emb, a.centre)
			if d < bestD && d <= a.thresh {
				best, bestD = i, d
			}
		}
		if best < 0 {
			continue
		}
		out = append(out, admission{sig: sig, storyID: anchors[best].id})
	}
	return out
}

// splitResult describes an accepted two-way division of a story.
type splitResult struct {
	keep  []*batchSignal // stays under the existing story ID
	spawn []*batchSignal // moves to a newly created story
}

// splitStory tests whether a story holds two groups the split threshold says are
// separate stories, and returns the division if so. The decision, including the
// radius pre-filter and the two-medoid search, is cluster.Split.
func (t *Tracker[T]) splitStory(members []*batchSignal, radius float64) (splitResult, bool) {
	div, ok := cluster.Split(clusterPoints(members), radius, t.clusterParams())
	if !ok {
		return splitResult{}, false
	}
	return splitResult{
		keep:  pickSignals(members, div.Keep),
		spawn: pickSignals(members, div.Spawn),
	}, true
}

// planMerges maps each story that is about to be retired to the story absorbing
// it. The grouping and the survivor rule are cluster.PlanMerges.
func (t *Tracker[T]) planMerges(
	members map[uuid.UUID][]*batchSignal,
	centroids map[uuid.UUID][]float32,
	created map[uuid.UUID]time.Time,
) cluster.MergePlan {
	return cluster.PlanMerges(clusterMembers(members), centroids, created, t.clusterParams())
}

// applyMaintenance runs the whole maintenance pass in one write transaction:
// evict, promote, split, merge, recentre, lifecycle. It is the replacement for
// the cluster/map/apply pipeline (spec 006).
func (t *Tracker[T]) applyMaintenance(
	signals []batchSignal,
	stories map[uuid.UUID]storyRecord,
	evict []uuid.UUID,
	mean []float32,
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
			if err := tx.Delete(keys.Outlier(id)); err != nil {
				return err
			}
			if err := tx.Delete(keys.SignalLoc(id)); err != nil {
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
			sid, err := deriveStoryID(tx, t.cfg.Namespace, "promote", p.members, created)
			if err != nil {
				return err
			}
			for _, m := range p.members {
				if err := moveSignal(tx, keys.Outlier(m.id), keys.Signal(sid, m.id)); err != nil {
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

		// 3. Admit the outliers that no clique claimed into stories that
		// already cover them. Runs after promotion so a fresh clique competes
		// for them, and before split so a story widened by admission is cut in
		// the same run rather than staying diffuse until the next one.
		var leftover []*batchSignal
		for _, sig := range outliers {
			if sig.outlier {
				leftover = append(leftover, sig)
			}
		}
		for _, ad := range t.admitOutliers(members, leftover, now) {
			if err := moveSignal(tx, keys.Outlier(ad.sig.id), keys.Signal(ad.storyID, ad.sig.id)); err != nil {
				return err
			}
			ad.sig.storyID = ad.storyID
			ad.sig.outlier = false
			members[ad.storyID] = append(members[ad.storyID], ad.sig)
			summary.OutliersAdmitted++
			events = append(events, StoryEvent[T]{
				Kind: EventSignalReassigned, StoryID: ad.storyID, SignalID: ad.sig.id, At: now,
			})
		}

		// 4. Split stories that now hold two groups. At most one split per
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
			child, err := deriveStoryID(tx, t.cfg.Namespace, "split:"+sid.String(), res.spawn, created)
			if err != nil {
				return err
			}
			for _, m := range res.spawn {
				if err := moveSignal(tx, keys.Signal(sid, m.id), keys.Signal(child, m.id)); err != nil {
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

		// 5. Merge stories that have converged, measured on post-split
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
				if err := tx.Delete(keys.TimeIndex(rec.LastSignalAt.Unix(), retired)); err != nil {
					return err
				}
			}
			if err := tx.Delete(keys.StoryMeta(retired)); err != nil {
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

		// 6. Recentre and advance lifecycle for every surviving story.
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

		// 7. Global calibration. The mean is published together with the
		// centroids it was used to compute, so a Draft lookup between runs
		// never centres a signal against one geometry and compares it with
		// centroids built in another.
		t.calibMu.Lock()
		if len(mean) > 0 {
			t.mean = mean
		}
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
			if err := tx.Delete(keys.TimeIndex(prev.LastSignalAt.Unix(), sid)); err != nil {
				return err
			}
		}
		if err := tx.Delete(keys.StoryMeta(sid)); err != nil {
			return err
		}
		summary.StoriesRetired++
		*events = append(*events, StoryEvent[T]{Kind: EventStoryRetired, StoryID: sid, At: now})
		return nil
	}

	st := measure(group)
	rec.Centroid = st.Centroid
	rec.RecentCentroid = recentCentroidOf(group, now.Add(-t.cfg.ActiveContextWindow), st.Centroid)
	rec.SignalCount = len(group)
	rec.Radius = st.Radius
	rec.MeanDistance = st.Mean
	rec.Sigma = st.Sigma
	rec.LastSignalAt = st.LatestAt
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	latestAt := st.LatestAt

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
		for _, d := range st.Dists {
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
func sortedPlanKeys(p cluster.MergePlan) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(p))
	for id := range p {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
