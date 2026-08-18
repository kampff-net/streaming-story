package story

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/keys"
)

// The Draft phase: the real-time path a single arriving signal takes.

// Ingest processes a signal and returns the provisional story IDs its facets
// were assigned to: sorted, de-duplicated, and empty when no story claimed any
// facet. The set may change after the next batch run resolves the final story
// structure.
//
// Returns ErrDimensionMismatch if the signal's embedding length differs from
// the dimensionality established by the first ingested signal.
//
// While a batch Apply is in progress the signal is buffered in memory and the
// returned ID is computed from the story snapshot the batch published, so a
// caller never has to distinguish "no match" from "ask again later". The
// batch goroutine drains the buffer into the store once the Apply transaction
// commits, and that placement — not this one — is authoritative.
func (t *Tracker[T]) Ingest(ctx context.Context, sig Signal[T]) ([]uuid.UUID, error) {
	// Held for the whole call: Close must not shut the store down underneath
	// a transaction this call has already started.
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()
	if t.closed.Load() {
		return nil, fmt.Errorf("story: tracker is closed")
	}

	// Establish or validate dimensionality. Every facet must agree with the
	// corpus and, therefore, with every other facet of its own signal.
	if len(sig.Embeddings) == 0 {
		return nil, fmt.Errorf("story: signal must carry at least one facet")
	}
	if t.cfg.MaxFacetsPerSignal > 0 && len(sig.Embeddings) > t.cfg.MaxFacetsPerSignal {
		return nil, fmt.Errorf("story: %d facets exceeds MaxFacetsPerSignal %d: %w",
			len(sig.Embeddings), t.cfg.MaxFacetsPerSignal, ErrTooManyFacets)
	}
	embLen := int32(len(sig.Embeddings[0]))
	if embLen == 0 {
		return nil, fmt.Errorf("story: embedding must not be empty")
	}
	for i, facet := range sig.Embeddings {
		if int32(len(facet)) != embLen {
			return nil, fmt.Errorf("story: facet %d: %w", i, ErrDimensionMismatch)
		}
	}
	if !t.dim.CompareAndSwap(0, embLen) {
		if t.dim.Load() != embLen {
			return nil, ErrDimensionMismatch
		}
	}

	// If a batch Apply is in progress, buffer the signal instead of writing
	// directly to the store, and answer the caller from the snapshot the
	// batch published. The store is not touched: the Store contract does not
	// promise that View may run concurrently with Update.
	// Every distance is measured in centred space (geometry.go). The stored
	// copy stays raw; the projection is re-derived on read.
	proj := t.projector()
	embs := make([]Embedding, len(sig.Embeddings))
	for i, facet := range sig.Embeddings {
		embs[i] = proj.Project(facet)
	}

	if t.applyInProgress.Load() {
		now := time.Now()
		provisional := make([]uuid.UUID, 0, len(embs))
		for _, emb := range embs {
			provisional = append(provisional, t.provisionalStory(emb, now))
		}
		select {
		case t.ingestBuffer <- sig:
			return storyIDSet(provisional...), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	var assigned []uuid.UUID
	// Stories this call newly placed a facet in. One event per (signal, story)
	// however many facets landed there: it is still one signal joining one
	// story, and a subscriber must not see it once per facet.
	var emitTo []uuid.UUID

	err := t.cfg.Store.Update(func(tx Tx) error {
		assigned, emitTo = nil, nil

		// The canonical record is written once, before any placement decision,
		// so a signal exists in the store whether or not a story claims it.
		if err := t.writeCanonicalSignal(tx, sig); err != nil {
			return err
		}

		// Locate the signal's existing placement first. Re-ingestion of a
		// signal any story already holds is a strict no-op at the signal
		// level: batch placements are authoritative, and a duplicate delivery
		// must not partially overwrite one. Only a signal whose every facet is
		// unplaced is assigned again.
		curLocs, hasIndex, err := readSignalLocSet(tx, sig.ID)
		if err != nil {
			return err
		}
		if placed := placedStories(curLocs); hasIndex && len(placed) > 0 {
			assigned = placed
			return nil
		}

		matches, err := t.findNearestStories(tx, embs)
		if err != nil {
			return err
		}

		locs := make([]keys.FacetLoc, len(embs))
		// Story records touched by this signal, so each is written once with
		// every facet's effect folded in rather than once per facet. The
		// metadata is carried over from the scan that matched it: re-reading
		// and re-decoding it here would be the scan's work done twice.
		touched := make(map[uuid.UUID]StoryMeta, len(embs))
		newlyPlaced := make(map[uuid.UUID]bool, len(embs))

		for facet, m := range matches {
			if !m.accepted {
				if err := holdFacetOutlier(tx, sig.ID, facet); err != nil {
					return err
				}
				locs[facet] = keys.FacetLoc{IsOutlier: true}
				continue
			}

			existing, err := tx.Get(keys.FacetMember(m.story.ID, sig.ID, facet))
			if err != nil {
				return err
			}
			if existing == nil {
				if err := placeFacet(tx, m.story.ID, sig.ID, facet); err != nil {
					return err
				}
				newlyPlaced[m.story.ID] = true
			}
			locs[facet] = keys.FacetLoc{StoryID: m.story.ID}
			touched[m.story.ID] = m.story
		}

		if err := writeSignalLocSet(tx, sig.ID, locs); err != nil {
			return err
		}

		assigned = placedStories(locs)
		for _, id := range assigned {
			if newlyPlaced[id] {
				emitTo = append(emitTo, id)
			}
		}

		// One metadata write per touched story, in sorted order so a replay
		// makes the same writes in the same sequence.
		for _, id := range storyIDSet(keysOf(touched)...) {
			meta := touched[id]
			rec := storyRecord{
				State:              meta.State,
				Centroid:           meta.Centroid,
				RecentCentroid:     meta.RecentCentroid,
				Radius:             meta.Radius,
				CreatedAt:          meta.CreatedAt,
				LastSignalAt:       meta.LastSignalAt,
				MeanDistance:       meta.MeanDistance,
				Sigma:              meta.Sigma,
				SignalCount:        meta.SignalCount,
				FrozenMeanDistance: meta.FrozenMeanDistance,
				FrozenSigma:        meta.FrozenSigma,
			}

			// LastSignalAt advances monotonically; out-of-order signals do not
			// regress it.
			if sig.At.After(rec.LastSignalAt) {
				rec.LastSignalAt = sig.At
			}

			// Reactivation clears the frozen and live statistics: the story
			// re-enters cold-start until the next batch run recomputes live
			// statistics.
			if rec.State == StoryStateDormant {
				rec.State = StoryStateActive
				rec.MeanDistance = 0
				rec.Sigma = 0
				rec.SignalCount = 0
				rec.FrozenMeanDistance = 0
				rec.FrozenSigma = 0
			}

			if err := t.writeStoryMeta(tx, id, meta.LastSignalAt, rec); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	now := time.Now()
	for _, id := range emitTo {
		t.emit(StoryEvent[T]{
			Kind:     EventDraftAssigned,
			StoryID:  id,
			SignalID: sig.ID,
			At:       now,
		})
	}

	return assigned, nil
}

// keysOf returns a map's keys, unordered. Callers that need determinism sort
// the result; storyIDSet does.
func keysOf[V any](m map[uuid.UUID]V) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	return out
}

// storyIDSet renders a single placement as the story set Ingest returns. A
// signal no story claimed yields an empty set rather than a slice holding
// uuid.Nil, so a caller can test it with len alone.
func storyIDSet(ids ...uuid.UUID) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(ids))
	seen := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		if id == uuid.Nil {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

// facetMatch is the best story found for one facet, and whether that story's
// threshold accepts it.
type facetMatch struct {
	story    StoryMeta
	dist     float64
	found    bool
	accepted bool
}

// findNearestStories finds the nearest active or dormant story centroid for
// every facet, in a single walk of the time index.
//
// Candidates come from the Tier 3 Active Context: stories whose last signal is
// at least ActiveContextWindow old are not anchors. The t: time index is
// scanned rather than the full s: prefix, so the cost is proportional to the
// number of candidate stories rather than the number of stored signals — and
// the scan is walked once for all facets rather than once per facet, which is
// the difference between O(S) and O(F·S) store reads on the hot path.
func (t *Tracker[T]) findNearestStories(tx Tx, embs []Embedding) ([]facetMatch, error) {
	out := make([]facetMatch, len(embs))
	for i := range out {
		out[i].dist = math.MaxFloat64
	}

	cutoff := keys.TimeIndexFrom(time.Now().Add(-t.cfg.ActiveContextWindow).Unix())

	err := tx.ScanRange(cutoff, []byte("u:"), func(key, val []byte) error {
		id, ok := keys.ParseTimeIndex(key)
		if !ok {
			return nil
		}
		var rec storyRecord
		b, err := tx.Get(keys.StoryMeta(id))
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil
		}
		if rec.State == StoryStateArchived || len(rec.Centroid) == 0 {
			return nil
		}

		// Admission uses the recency centroid so a developing story keeps
		// admitting its own current coverage (spec 006 §2.7).
		centre := recentOrCentroid(rec)
		var meta StoryMeta
		decoded := false
		for i, emb := range embs {
			d := dist.CosineDistance(emb, centre)
			if d >= out[i].dist {
				continue
			}
			if !decoded {
				meta = storyMetaFromRecord(id, rec)
				decoded = true
			}
			out[i] = facetMatch{story: meta, dist: d, found: true}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Each facet is admitted on its own story's terms, the same test the
	// single-vector path applied (threshold.go).
	for i := range out {
		if out[i].found && out[i].dist <= t.calcThreshold(out[i].story) {
			out[i].accepted = true
		}
	}
	return out, nil
}
