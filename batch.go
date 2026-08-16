package story

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/hdbscan"
	"go.kvsh.ch/streaming-story/internal/hungarian"
)

const (
	// mergeCoverageThreshold is the minimum combined coverage that qualifies
	// a Phase 2 split or merge (see DESIGN.md).
	mergeCoverageThreshold = 0.7

	// mappingInf is the sentinel cost used for cluster/story pairs whose
	// Jaccard similarity is below MappingMinJaccard. The Hungarian solver
	// assigns such pairs only when no real match exists; the assignment is
	// discarded afterwards. Real costs live in [0, 1], so any value well
	// above 1 is safe.
	mappingInf = 1e6
)

// errStopIteration terminates a range scan early; the caller checks for it
// with errors.Is to distinguish a requested stop from a real failure.
var errStopIteration = errors.New("stop iteration")

// batchSignal is one signal collected for a batch run.
type batchSignal struct {
	id      uuid.UUID
	at      time.Time
	emb     []float32
	storyID uuid.UUID // current story assignment; uuid.Nil for outlier signals
	outlier bool      // stored under the o: prefix
	kept    bool      // selected by sampling and fed to HDBSCAN
	label   int       // HDBSCAN cluster label; -1 is noise
}

// runBatch executes one full batch re-clustering cycle.
func (t *Tracker[T]) runBatch() {
	// Safety net: a panic inside the batch core must not leave Ingest
	// permanently redirected to the staging buffer.
	defer t.endApplyWindow()

	summary := t.runBatchCore()

	t.emit(StoryEvent[T]{
		Kind:         EventBatchComplete,
		At:           time.Now(),
		BatchSummary: summary,
	})
}

// beginApplyWindow redirects Ingest into the staging buffer and publishes the
// story snapshot that Draft lookups answer from while the write transaction
// holds the store.
func (t *Tracker[T]) beginApplyWindow(stories map[uuid.UUID]storyRecord) {
	t.publishDraftSnapshot(stories)
	t.applyInProgress.Store(true)
}

// endApplyWindow reopens the direct ingest path and flushes whatever was
// staged. The snapshot outlives the flag until the drain completes, so an
// Ingest that observed the flag just before it cleared still gets an answer.
// It is idempotent.
func (t *Tracker[T]) endApplyWindow() {
	t.applyInProgress.Store(false)
	t.drainBuffer()
	t.clearDraftSnapshot()
}

// runBatchCore performs collection, sampling, HDBSCAN clustering, cluster
// mapping, and the apply transaction. It returns a summary of the run; on
// internal failure it returns an all-zero summary so the lifecycle continues.
func (t *Tracker[T]) runBatchCore() *BatchSummary {
	now := time.Now()

	var (
		signals []batchSignal
		stories map[uuid.UUID]storyRecord
		evict   []uuid.UUID
	)
	err := t.cfg.Store.View(func(tx Tx) error {
		var err error
		signals, stories, evict, err = t.collectBatch(tx, now)
		return err
	})
	if err != nil {
		t.reportBatchError(fmt.Errorf("story: batch collect: %w", err))
		return &BatchSummary{}
	}

	// Reduce to the sampling cap when needed.
	if len(signals) > t.cfg.BatchSampleCap {
		keep := t.sampleSignals(signals, stories)
		for i := range signals {
			signals[i].kept = keep[i]
		}
	} else {
		for i := range signals {
			signals[i].kept = true
		}
	}

	t.clusterSignals(signals)
	mapping := t.mapClusters(signals, stories)

	// Only the write transaction needs the ingest path redirected: collection
	// is read-only and clustering touches no store at all, so writers are not
	// stalled for those phases.
	t.beginApplyWindow(stories)
	summary, events, err := t.applyBatch(signals, stories, mapping, evict, now)
	t.endApplyWindow()
	if err != nil {
		t.reportBatchError(fmt.Errorf("story: batch apply: %w", err))
		return &BatchSummary{}
	}

	for _, ev := range events {
		t.emit(ev)
	}
	return summary
}

// reportBatchError hands err to Config.OnBatchError when one is configured.
// A batch failure is otherwise invisible: the run returns an empty summary and
// the store is left untouched until the next tick.
func (t *Tracker[T]) reportBatchError(err error) {
	if t.cfg.OnBatchError != nil {
		t.cfg.OnBatchError(err)
	}
}

// drainBuffer ingests any signals that arrived while applyInProgress was set.
// It must be called after applyInProgress is cleared so the signals reach the
// store instead of being re-buffered.
func (t *Tracker[T]) drainBuffer() {
	for {
		select {
		case sig := <-t.ingestBuffer:
			if _, err := t.Ingest(context.Background(), sig); err != nil {
				t.reportBatchError(fmt.Errorf("story: drain ingest buffer: %w", err))
			}
		default:
			return
		}
	}
}

// collectBatch gathers the signals in the batch window from every Active and
// Dormant story, plus retained outliers, and returns a snapshot of the story
// records and the outlier IDs to be evicted. Eviction is not performed here:
// the IDs are returned so the apply transaction can delete them.
func (t *Tracker[T]) collectBatch(tx Tx, now time.Time) ([]batchSignal, map[uuid.UUID]storyRecord, []uuid.UUID, error) {
	var (
		signals     []batchSignal
		stories     = make(map[uuid.UUID]storyRecord)
		evict       []uuid.UUID
		windowStart = now.Add(-t.cfg.BatchWindow)
	)

	err := tx.ScanPrefix([]byte("s:"), func(key, val []byte) error {
		id, ok := parseStoryMetaKey(key)
		if !ok {
			return nil
		}
		var rec storyRecord
		if err := json.Unmarshal(val, &rec); err != nil {
			return nil
		}
		if rec.State == StoryStateArchived {
			return nil
		}
		stories[id] = rec

		// Collect the story's window signals.
		return tx.ScanPrefix(keySignalPrefix(id), func(sigKey, sigVal []byte) error {
			sig, err := t.cfg.Codec.Decode(sigVal)
			if err != nil {
				return nil
			}
			if sig.At.Before(windowStart) {
				return nil
			}
			signals = append(signals, batchSignal{
				id:      sig.ID,
				at:      sig.At,
				emb:     sig.Embedding,
				storyID: id,
				label:   -1,
			})
			return nil
		})
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// Outliers: collect those within lastBatch − OutlierTTL and flag the rest
	// for eviction. The reference is lastBatch, not wall-clock now, so
	// maintenance pauses do not cause mass eviction.
	err = tx.ScanPrefix([]byte("o:"), func(key, val []byte) error {
		sig, err := t.cfg.Codec.Decode(val)
		if err != nil {
			return nil
		}
		if sig.At.Before(t.lastBatch.Add(-t.cfg.OutlierTTL)) {
			evict = append(evict, sig.ID)
			return nil
		}
		signals = append(signals, batchSignal{
			id:      sig.ID,
			at:      sig.At,
			emb:     sig.Embedding,
			storyID: uuid.Nil,
			outlier: true,
			label:   -1,
		})
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return signals, stories, evict, nil
}

// sampleGroup is a group of signal indices for stratified sampling.
type sampleGroup struct {
	indices []int // signal indices, reverse-chronological
	active  bool  // an Active story (eligible for the guaranteed pass)
}

// sampleSignals applies the two-pass stratified sampling from DESIGN.md and
// returns a keep mask over the input signals. Pass 1 reserves a per-Active
// story minimum; pass 2 distributes the remaining capacity proportionally by
// signal count. Outliers are treated as a single pseudo-group.
func (t *Tracker[T]) sampleSignals(signals []batchSignal, stories map[uuid.UUID]storyRecord) []bool {
	var groups []sampleGroup
	groupOf := make(map[uuid.UUID]int)
	outlierGroup := -1

	for i := range signals {
		sig := &signals[i]
		var g int
		if sig.outlier {
			if outlierGroup < 0 {
				groups = append(groups, sampleGroup{})
				outlierGroup = len(groups) - 1
			}
			g = outlierGroup
		} else {
			var ok bool
			if g, ok = groupOf[sig.storyID]; !ok {
				groups = append(groups, sampleGroup{active: stories[sig.storyID].State == StoryStateActive})
				g = len(groups) - 1
				groupOf[sig.storyID] = g
			}
		}
		groups[g].indices = append(groups[g].indices, i)
	}

	// Reverse-chronological order within each group: the newest signals are
	// reserved first.
	for gi := range groups {
		idx := groups[gi].indices
		sort.Slice(idx, func(a, b int) bool {
			return signals[idx[a]].at.After(signals[idx[b]].at)
		})
	}

	return sampleGroups(groups, t.cfg.BatchSampleCap, t.cfg.MinClusterSize, t.cfg.SampleGuaranteeMaxFraction)
}

// sampleGroups implements the two-pass allocation. groups[i].indices must be
// sorted newest-first. The returned mask is indexed by the absolute signal
// index, which is the flattened concatenation of the groups.
func sampleGroups(groups []sampleGroup, cap, minClusterSize int, maxFraction float64) []bool {
	total := 0
	for _, g := range groups {
		total += len(g.indices)
	}
	keep := make([]bool, total)
	if total <= cap {
		for i := range keep {
			keep[i] = true
		}
		return keep
	}

	offsets := make([]int, len(groups))
	off := 0
	for i, g := range groups {
		offsets[i] = off
		off += len(g.indices)
	}

	// Pass 1: guaranteed minimums for Active stories.
	activeCount := 0
	for _, g := range groups {
		if g.active {
			activeCount++
		}
	}
	per := minClusterSize
	budget := int(float64(cap) * maxFraction)
	if activeCount > 0 && activeCount*per > budget {
		per = budget / activeCount
		if per < 1 {
			per = 1
		}
	}
	keptPer := make([]int, len(groups))
	for i, g := range groups {
		if !g.active || per <= 0 {
			continue
		}
		n := per
		if n > len(g.indices) {
			n = len(g.indices)
		}
		for j := 0; j < n; j++ {
			keep[offsets[i]+j] = true
		}
		keptPer[i] = n
	}

	used := 0
	for _, n := range keptPer {
		used += n
	}
	remaining := cap - used
	if remaining <= 0 {
		return keep
	}

	// Pass 2: proportional allocation of the remaining capacity.
	alloc := make([]int, len(groups))
	frac := make([]float64, len(groups))
	allocated := 0
	for i, g := range groups {
		capacity := len(g.indices) - keptPer[i]
		if capacity <= 0 {
			continue
		}
		raw := float64(remaining) * float64(len(g.indices)) / float64(total)
		base := int(math.Floor(raw))
		if base > capacity {
			base = capacity
		}
		alloc[i] = base
		frac[i] = raw - float64(base)
		allocated += base
	}

	// Largest-remainder distribution of the leftover slots.
	leftover := remaining - allocated
	for leftover > 0 {
		best := -1
		for i, g := range groups {
			if alloc[i] >= len(g.indices)-keptPer[i] {
				continue
			}
			if best == -1 || frac[i] > frac[best] {
				best = i
			}
		}
		if best == -1 {
			break
		}
		alloc[best]++
		frac[best] -= 1.0
		leftover--
	}
	for i := range groups {
		for j := 0; j < alloc[i]; j++ {
			keep[offsets[i]+keptPer[i]+j] = true
		}
	}
	return keep
}

// clusterSignals runs HDBSCAN over the kept signals and records the cluster
// label on each input signal. Noise is −1. Failures leave every signal as
// noise, which means no re-assignment happens.
func (t *Tracker[T]) clusterSignals(signals []batchSignal) {
	var kept []int
	for i := range signals {
		if signals[i].kept {
			kept = append(kept, i)
		}
	}
	if len(kept) == 0 {
		return
	}
	pts := make([][]float32, len(kept))
	for k, i := range kept {
		pts[k] = signals[i].emb
	}
	selection := hdbscan.SelectionEOM
	if t.cfg.ClusterSelection == ClusterSelectionLeaf {
		selection = hdbscan.SelectionLeaf
	}
	labels, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{
		MinClusterSize: t.cfg.MinClusterSize,
		MinSamples:     t.cfg.MinSamples,
		Selection:      selection,
		MaxClusterSize: t.cfg.MaxClusterSize,
	})
	if err != nil {
		t.reportBatchError(fmt.Errorf("story: batch cluster: %w", err))
		return
	}
	for k, i := range kept {
		signals[i].label = labels[k]
	}
}

// clusterMapping is the result of the two-phase cluster mapping.
type clusterMapping struct {
	// labelStory maps a cluster label to the story that receives its signals:
	// a Phase 1 match (possibly a merge survivor), a split child, or a newly
	// created story. Absent labels denote small unmatched clusters whose
	// signals are demoted to outliers.
	labelStory map[int]uuid.UUID
	// splitParents maps a split child story ID back to its parent, so the
	// apply phase can emit EventStorySplit.
	splitParents map[uuid.UUID]uuid.UUID
	// retired maps a merged-away story ID to its surviving story.
	retired map[uuid.UUID]uuid.UUID
}

// mapClusters performs the two-phase cluster mapping described in DESIGN.md:
// Phase 1 is an optimal 1-to-1 Hungarian assignment; Phase 2 detects N-way
// merges and splits over the unmatched remainder.
func (t *Tracker[T]) mapClusters(signals []batchSignal, stories map[uuid.UUID]storyRecord) clusterMapping {
	res := clusterMapping{
		labelStory:   make(map[int]uuid.UUID),
		splitParents: make(map[uuid.UUID]uuid.UUID),
		retired:      make(map[uuid.UUID]uuid.UUID),
	}

	// Group kept signals by cluster label and by current story.
	clusterMembers := make(map[int][]int)
	storyMembers := make(map[uuid.UUID][]int)
	for i := range signals {
		sig := &signals[i]
		if !sig.kept || sig.outlier {
			continue
		}
		storyMembers[sig.storyID] = append(storyMembers[sig.storyID], i)
	}
	var labels []int
	for i := range signals {
		sig := &signals[i]
		if !sig.kept || sig.label < 0 {
			continue
		}
		if _, ok := clusterMembers[sig.label]; !ok {
			labels = append(labels, sig.label)
		}
		clusterMembers[sig.label] = append(clusterMembers[sig.label], i)
	}
	sort.Ints(labels)
	storiesSeen := make([]uuid.UUID, 0, len(storyMembers))
	for sid := range storyMembers {
		storiesSeen = append(storiesSeen, sid)
	}
	sort.Slice(storiesSeen, func(i, j int) bool {
		return storiesSeen[i].String() < storiesSeen[j].String()
	})

	// Phase 1: Hungarian 1-to-1 continuity matching.
	cost := make([][]float64, len(labels))
	for c := range cost {
		cost[c] = make([]float64, len(storiesSeen))
		for s := range storiesSeen {
			j := jaccardIndex(clusterMembers[labels[c]], storyMembers[storiesSeen[s]])
			if j >= t.cfg.MappingMinJaccard {
				cost[c][s] = 1 - j
			} else {
				cost[c][s] = mappingInf
			}
		}
	}
	var assignment []int
	if len(labels) > 0 && len(storiesSeen) > 0 {
		if a, err := hungarian.Solve(cost); err == nil {
			assignment = a
		}
	}
	for c, s := range assignment {
		if s < 0 || cost[c][s] >= mappingInf {
			continue
		}
		res.labelStory[labels[c]] = storiesSeen[s]
	}

	// A story already claimed by a Phase 1 match, or absorbed by a merge
	// below, is not eligible to be absorbed again.
	claimed := make(map[uuid.UUID]bool, len(res.labelStory))
	for _, sid := range res.labelStory {
		claimed[sid] = true
	}

	// Phase 2a: N-way merges. An unmatched story that overlaps a matched
	// cluster sufficiently is absorbed; the oldest creation time survives.
	for _, label := range labels {
		sid, matched := res.labelStory[label]
		if !matched {
			continue
		}
		mergeGroup := []uuid.UUID{sid}
		for _, s2 := range storiesSeen {
			if s2 == sid || claimed[s2] {
				continue
			}
			if jaccardIndex(clusterMembers[label], storyMembers[s2]) < t.cfg.SplitMinJaccard {
				continue
			}
			if coverageIndex(storyMembers[sid], storyMembers[s2], clusterMembers[label]) <= mergeCoverageThreshold {
				continue
			}
			mergeGroup = append(mergeGroup, s2)
		}
		if len(mergeGroup) > 1 {
			survivor := oldestStory(mergeGroup, stories)
			for _, s2 := range mergeGroup {
				if s2 != survivor {
					res.retired[s2] = survivor
					claimed[s2] = true
				}
			}
			res.labelStory[label] = survivor
		}
	}

	// A story that survives a merge is the target of the matched cluster;
	// retired stories are no longer split candidates.
	aliveMatched := make(map[uuid.UUID]int)
	for label, sid := range res.labelStory {
		aliveMatched[sid] = label
	}

	// Phase 2b: N-way splits. Unmatched clusters overlapping a matched story
	// are promoted as new stories. Splits on Dormant stories are suppressed
	// into plain story creation.
	for _, label := range labels {
		if _, matched := res.labelStory[label]; matched {
			continue
		}
		for parent, parentLabel := range aliveMatched {
			if jaccardIndex(clusterMembers[label], storyMembers[parent]) < t.cfg.SplitMinJaccard {
				continue
			}
			if coverageIndex(clusterMembers[parentLabel], clusterMembers[label], storyMembers[parent]) <= mergeCoverageThreshold {
				continue
			}
			child := uuid.New()
			res.labelStory[label] = child
			if stories[parent].State == StoryStateActive {
				res.splitParents[child] = parent
			}
			break
		}
	}

	// Phase 2c: unmatched clusters of sufficient size become new stories;
	// smaller ones are retained as outliers.
	for _, label := range labels {
		if _, matched := res.labelStory[label]; matched {
			continue
		}
		if len(clusterMembers[label]) < t.cfg.MinClusterSize {
			continue
		}
		res.labelStory[label] = uuid.New()
	}

	return res
}

// applyBatch persists the mapping result in a single transaction: outlier
// eviction, merge key-space migrations, signal re-assignment, story metadata
// recomputation, lifecycle transitions, and the calibration state update.
func (t *Tracker[T]) applyBatch(signals []batchSignal, stories map[uuid.UUID]storyRecord, m clusterMapping, evict []uuid.UUID, now time.Time) (*BatchSummary, []StoryEvent[T], error) {
	summary := &BatchSummary{OutliersEvicted: len(evict)}
	var events []StoryEvent[T]

	err := t.cfg.Store.Update(func(tx Tx) error {
		// Evict stale outliers.
		for _, id := range evict {
			if err := tx.Delete(keyOutlier(id)); err != nil {
				return err
			}
			if err := tx.Delete(keySignalLoc(id)); err != nil {
				return err
			}
		}

		// Merge: migrate the full key space of every retired story to its
		// survivor, including signals older than the batch window. This runs
		// before per-signal moves so retired signals all land under the
		// survivor prefix.
		for retired, survivor := range m.retired {
			prefix := keySignalPrefix(retired)
			if err := tx.ScanPrefix(prefix, func(key, val []byte) error {
				sigID, ok := parseSignalKey(key, prefix)
				if !ok {
					return nil
				}
				return moveSignal(tx, key, keySignal(survivor, sigID))
			}); err != nil {
				return err
			}
			rec := stories[retired]
			if !rec.LastSignalAt.IsZero() {
				if err := tx.Delete(keyTimeIndex(rec.LastSignalAt.Unix(), retired)); err != nil {
					return err
				}
			}
			if err := tx.Delete(keyStoryMeta(retired)); err != nil {
				return err
			}
			events = append(events, StoryEvent[T]{Kind: EventStoryMerged, StoryID: survivor, StoryID2: retired, At: now})
			summary.StoriesMerged++
		}

		// Re-assign sampled window signals to their post-batch locations.
		for i := range signals {
			sig := &signals[i]
			if !sig.kept || sig.label < 0 {
				continue
			}
			// A retired story's signals were migrated under the survivor
			// prefix; use that as their current location.
			cur := sig.storyID
			if survivor, ok := m.retired[cur]; ok {
				cur = survivor
			}
			target, ok := m.labelStory[sig.label]
			if !ok {
				// Small unmatched cluster: demote to outlier.
				if !sig.outlier {
					if err := moveSignal(tx, keySignal(cur, sig.id), keyOutlier(sig.id)); err != nil {
						return err
					}
					summary.SignalsReassigned++
					events = append(events, StoryEvent[T]{Kind: EventSignalReassigned, StoryID: uuid.Nil, SignalID: sig.id, At: now})
				}
				continue
			}
			switch {
			case sig.outlier:
				if err := moveSignal(tx, keyOutlier(sig.id), keySignal(target, sig.id)); err != nil {
					return err
				}
				summary.OutliersPromoted++
				events = append(events, StoryEvent[T]{Kind: EventSignalReassigned, StoryID: target, SignalID: sig.id, At: now})
			case cur != target:
				if err := moveSignal(tx, keySignal(cur, sig.id), keySignal(target, sig.id)); err != nil {
					return err
				}
				summary.SignalsReassigned++
				events = append(events, StoryEvent[T]{Kind: EventSignalReassigned, StoryID: target, SignalID: sig.id, At: now})
			}
		}

		// Final window membership per story.
		finalMembers := make(map[uuid.UUID][]*batchSignal)
		for i := range signals {
			sig := &signals[i]
			var loc uuid.UUID
			if sig.outlier {
				if sig.kept && sig.label >= 0 {
					loc = m.labelStory[sig.label]
				}
			} else {
				loc = sig.storyID
				if sig.kept && sig.label >= 0 {
					if id, ok := m.labelStory[sig.label]; ok {
						loc = id
					} else {
						loc = uuid.Nil
					}
				}
				if survivor, ok := m.retired[loc]; ok {
					loc = survivor
				}
			}
			if loc != uuid.Nil {
				finalMembers[loc] = append(finalMembers[loc], sig)
			}
		}

		// Retire stories this batch emptied. A persistent story whose signals
		// were all reassigned elsewhere (or demoted) is left with no data — an
		// empty record is meaningless. Only stories with no remaining signal
		// keys are retired; a story that keeps historical signals outside the
		// batch window is untouched, per the re-assignment stability rule.
		emptied := make(map[uuid.UUID]bool)
		for sid, rec := range stories {
			if _, ok := m.retired[sid]; ok {
				continue
			}
			if len(finalMembers[sid]) > 0 {
				continue
			}
			empty := true
			err := tx.ScanPrefix(keySignalPrefix(sid), func(key, val []byte) error {
				empty = false
				return errStopIteration
			})
			if err != nil && !errors.Is(err, errStopIteration) {
				return err
			}
			if !empty {
				continue
			}
			if !rec.LastSignalAt.IsZero() {
				if err := tx.Delete(keyTimeIndex(rec.LastSignalAt.Unix(), sid)); err != nil {
					return err
				}
			}
			if err := tx.Delete(keyStoryMeta(sid)); err != nil {
				return err
			}
			emptied[sid] = true
			summary.StoriesRetired++
			events = append(events, StoryEvent[T]{Kind: EventStoryRetired, StoryID: sid, At: now})
		}

		// Persist surviving persistent stories and create new ones.
		var ema emaAccum
		handled := make(map[uuid.UUID]bool, len(stories)+len(finalMembers))
		for sid := range stories {
			if _, ok := m.retired[sid]; ok {
				continue
			}
			if emptied[sid] {
				handled[sid] = true
				continue
			}
			if err := t.persistStory(tx, sid, stories[sid], finalMembers[sid], &ema, summary, &events, now); err != nil {
				return err
			}
			handled[sid] = true
		}
		for sid := range finalMembers {
			if handled[sid] {
				continue
			}
			if err := t.createStory(tx, sid, finalMembers[sid], m, summary, &events, now); err != nil {
				return err
			}
		}

		// Update the global calibration state.
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

// persistStory writes the post-batch metadata for a surviving persistent
// story, applying the lifecycle transition and freezing statistics on the
// Dormant transition.
func (t *Tracker[T]) persistStory(tx Tx, sid uuid.UUID, prev storyRecord, members []*batchSignal, ema *emaAccum, summary *BatchSummary, events *[]StoryEvent[T], now time.Time) error {
	rec := prev
	lastAt := rec.LastSignalAt
	var dists []float64
	if len(members) > 0 {
		centroid, radius, meanDist, sigma, latestAt, ds := storyStats(members)
		rec.Centroid = centroid
		rec.Radius = radius
		rec.MeanDistance = meanDist
		rec.Sigma = sigma
		rec.SignalCount = len(members)
		rec.LastSignalAt = latestAt
		lastAt = latestAt
		dists = ds
	} else {
		rec.MeanDistance = 0
		rec.Sigma = 0
		rec.SignalCount = 0
	}

	var newState StoryState
	switch {
	case lastAt.Before(now.Add(-t.cfg.ArchiveWindow)):
		newState = StoryStateArchived
	case lastAt.Before(now.Add(-t.cfg.SilenceWindow)):
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

	return t.writeStoryMeta(tx, sid, prev.LastSignalAt, rec)
}

// createStory persists the metadata for a brand-new story (from an unmatched
// cluster or a split child) and emits the appropriate event.
func (t *Tracker[T]) createStory(tx Tx, sid uuid.UUID, members []*batchSignal, m clusterMapping, summary *BatchSummary, events *[]StoryEvent[T], now time.Time) error {
	if len(members) == 0 {
		return nil
	}
	centroid, radius, meanDist, sigma, latestAt, _ := storyStats(members)
	rec := storyRecord{
		State:        StoryStateActive,
		Centroid:     centroid,
		Radius:       radius,
		CreatedAt:    now,
		LastSignalAt: latestAt,
		MeanDistance: meanDist,
		Sigma:        sigma,
		SignalCount:  len(members),
	}
	if err := t.writeStoryMeta(tx, sid, time.Time{}, rec); err != nil {
		return err
	}
	if parent, ok := m.splitParents[sid]; ok {
		*events = append(*events, StoryEvent[T]{Kind: EventStorySplit, StoryID: parent, StoryID2: sid, At: now})
		summary.StoriesSplit++
	} else {
		*events = append(*events, StoryEvent[T]{Kind: EventStoryCreated, StoryID: sid, At: now})
		summary.StoriesCreated++
	}
	return nil
}

// emaAccum collects per-signal centroid distances of Active stories for the
// σ_global EMA update.
type emaAccum struct {
	sum   float64
	count int
}

func (e *emaAccum) add(d float64) {
	e.sum += d
	e.count++
}

// storyStats computes the centroid, radius, mean distance, and population
// standard deviation of the distances for a set of window signals, along with
// the individual distances and the latest signal time.
func storyStats(members []*batchSignal) (centroid []float32, radius, meanDist, sigma float64, latestAt time.Time, dists []float64) {
	dim := len(members[0].emb)
	centroid = make([]float32, dim)
	for _, m := range members {
		for d, v := range m.emb {
			centroid[d] += v
		}
		if m.at.After(latestAt) {
			latestAt = m.at
		}
	}
	n := float32(len(members))
	for d := range centroid {
		centroid[d] /= n
	}
	dists = make([]float64, len(members))
	var sum, sumSq float64
	for i, m := range members {
		d := dist.CosineDistance(m.emb, centroid)
		dists[i] = d
		sum += d
		sumSq += d * d
		if d > radius {
			radius = d
		}
	}
	meanDist = sum / float64(len(members))
	variance := sumSq/float64(len(members)) - meanDist*meanDist
	if variance < 0 {
		variance = 0
	}
	sigma = math.Sqrt(variance)
	return centroid, radius, meanDist, sigma, latestAt, dists
}

// oldestStory returns the story with the earliest CreatedAt among ids.
func oldestStory(ids []uuid.UUID, stories map[uuid.UUID]storyRecord) uuid.UUID {
	best := ids[0]
	for _, id := range ids[1:] {
		if stories[id].CreatedAt.Before(stories[best].CreatedAt) {
			best = id
		}
	}
	return best
}

// moveSignal moves a stored value from one key to another and keeps the
// signal's location-index entry pointing at the destination, so re-ingestion
// of a batch-moved signal finds its new home and never duplicates it.
func moveSignal(tx Tx, from, to []byte) error {
	val, err := tx.Get(from)
	if err != nil {
		return err
	}
	if val == nil {
		return nil
	}
	if err := tx.Put(to, val); err != nil {
		return err
	}
	if err := tx.Delete(from); err != nil {
		return err
	}
	sigID, ok := parseSignalIDFromLocKey(to)
	if !ok {
		return nil
	}
	if isOutlierKey(to) {
		return writeSignalLoc(tx, sigID, uuid.Nil, true)
	}
	storyID, ok := parseStoryIDFromSignalKey(to)
	if !ok {
		return fmt.Errorf("move signal: cannot parse destination story from %q", to)
	}
	return writeSignalLoc(tx, sigID, storyID, false)
}

// parseSignalKey extracts the signal ID from a "s:{storyID}:s:{signalID}" key
// given the key's prefix.
func parseSignalKey(key, prefix []byte) (uuid.UUID, bool) {
	if len(key) <= len(prefix) {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(string(key[len(prefix):]))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// jaccardIndex returns the Jaccard similarity between two index sets, 0 if
// either is empty.
func jaccardIndex(a, b []int) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	set := make(map[int]struct{}, len(b))
	for _, x := range b {
		set[x] = struct{}{}
	}
	intersection := 0
	for _, x := range a {
		if _, ok := set[x]; ok {
			intersection++
		}
	}
	return float64(intersection) / float64(len(a)+len(b)-intersection)
}

// coverageIndex returns |(a ∪ b) ∩ c| / |c|, 0 if c is empty.
func coverageIndex(a, b, c []int) float64 {
	if len(c) == 0 {
		return 0
	}
	set := make(map[int]struct{}, len(a)+len(b))
	for _, x := range a {
		set[x] = struct{}{}
	}
	for _, x := range b {
		set[x] = struct{}{}
	}
	count := 0
	for _, x := range c {
		if _, ok := set[x]; ok {
			count++
		}
	}
	return float64(count) / float64(len(c))
}
