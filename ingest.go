package story

import (
	"bytes"
	"context"
	"fmt"
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

	// Normalize every facet to a unit vector immediately. The store holds unit
	// vectors and all downstream math relies on unit length.
	normEmbs := make([]Embedding, len(sig.Embeddings))
	for i, facet := range sig.Embeddings {
		n := dist.Norm(facet)
		if n == 0 {
			return nil, ErrZeroEmbedding
		}
		u := make([]float32, len(facet))
		inv := 1.0 / n
		for j, v := range facet {
			u[j] = v * inv
		}
		normEmbs[i] = u
	}
	sig.Embeddings = normEmbs

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

	now := time.Now()
	matches := make([]nearestStory, len(embs))
	hasMatch := make([]bool, len(embs))
	for i, emb := range embs {
		m := t.findNearestStories(emb, now)
		if len(m) > 0 {
			matches[i] = m[0]
			hasMatch[i] = true
		}
	}

	if t.applyInProgress.Load() {
		provisional := make([]uuid.UUID, 0, len(embs))
		for i := range embs {
			if hasMatch[i] {
				provisional = append(provisional, matches[i].id)
			}
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

	type patchInfo struct {
		lastAt      time.Time
		reactivated bool
		reactAt     time.Time
		state       StoryState // state as of this Ingest call, before any reactivation
	}
	patched := make(map[uuid.UUID]patchInfo)

	err := t.cfg.Store.Update(func(tx Tx) error {
		assigned, emitTo = nil, nil

		// The canonical record is written once, before any placement decision,
		// so a signal exists in the store whether or not a story claims it.
		if err := t.writeCanonicalSignal(tx, sig); err != nil {
			return err
		}

		// A re-delivery carrying fewer facets than the stored record drops the
		// ones past its end. This runs ahead of the no-op check below, because
		// a signal whose facets are already placed is exactly the case where
		// the dropped markers would otherwise be left with nothing pointing at
		// them.
		if err := t.reconcileFacetCount(tx, sig.ID, len(embs)); err != nil {
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

		locs := make([]keys.FacetLoc, len(embs))
		touched := make(map[uuid.UUID]StoryMeta, len(embs))
		newlyPlaced := make(map[uuid.UUID]bool, len(embs))

		for facet := range embs {
			if !hasMatch[facet] {
				if err := holdFacetOutlier(tx, sig.ID, facet); err != nil {
					return err
				}
				locs[facet] = keys.FacetLoc{IsOutlier: true}
				continue
			}

			m := matches[facet]
			existing, err := tx.Get(keys.FacetMember(m.id, sig.ID, facet))
			if err != nil {
				return err
			}
			if existing == nil {
				if err := placeFacet(tx, m.id, sig.ID, facet); err != nil {
					return err
				}
				newlyPlaced[m.id] = true
			}
			locs[facet] = keys.FacetLoc{StoryID: m.id}
			touched[m.id] = m.story
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

		// One hot metadata write per touched story, in sorted order.
		for _, id := range storyIDSet(keysOf(touched)...) {
			meta := touched[id]
			hot := storyHot{
				State:         meta.State,
				LastSignalAt:  meta.LastSignalAt,
				ReactivatedAt: meta.ReactivatedAt,
			}

			if sig.At.After(hot.LastSignalAt) {
				hot.LastSignalAt = sig.At
			}

			reactivated := false
			if hot.State == StoryStateDormant {
				hot.State = StoryStateActive
				hot.ReactivatedAt = sig.At
				reactivated = true
			}

			if err := t.writeStoryHot(tx, id, meta.LastSignalAt, hot); err != nil {
				return err
			}
			patched[id] = patchInfo{
				lastAt:      hot.LastSignalAt,
				reactivated: reactivated,
				reactAt:     hot.ReactivatedAt,
				state:       meta.State,
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	for id, p := range patched {
		t.patchStoryIndex(id, p.lastAt, p.reactivated, p.reactAt)
	}

	eventNow := time.Now()
	for _, id := range emitTo {
		t.emit(StoryEvent[T]{
			Kind:     EventDraftAssigned,
			StoryID:  id,
			SignalID: sig.ID,
			At:       eventNow,
		})
		// A suppressed story stays suppressed on a signal join (see Ingest's
		// hot-write loop above); this is the only signal to a subscriber that
		// it is still accumulating members and may be worth re-evaluating.
		if patched[id].state == StoryStateSuppressed {
			t.emit(StoryEvent[T]{
				Kind:     EventSuppressedStorySignal,
				StoryID:  id,
				SignalID: sig.ID,
				At:       eventNow,
			})
		}
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
