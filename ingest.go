package story

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/keys"
)

// The Draft phase: the real-time path a single arriving signal takes.

// Ingest processes a signal and returns its provisional StoryID.
// The returned ID may change after the next batch run resolves the final
// story structure.
//
// Returns ErrDimensionMismatch if the signal's embedding length differs from
// the dimensionality established by the first ingested signal.
//
// While a batch Apply is in progress the signal is buffered in memory and the
// returned ID is computed from the story snapshot the batch published, so a
// caller never has to distinguish "no match" from "ask again later". The
// batch goroutine drains the buffer into the store once the Apply transaction
// commits, and that placement — not this one — is authoritative.
func (t *Tracker[T]) Ingest(ctx context.Context, sig Signal[T]) (uuid.UUID, error) {
	// Held for the whole call: Close must not shut the store down underneath
	// a transaction this call has already started.
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()
	if t.closed.Load() {
		return uuid.Nil, fmt.Errorf("story: tracker is closed")
	}

	// Establish or validate dimensionality.
	embLen := int32(len(sig.Embedding))
	if embLen == 0 {
		return uuid.Nil, fmt.Errorf("story: embedding must not be empty")
	}
	if !t.dim.CompareAndSwap(0, embLen) {
		if t.dim.Load() != embLen {
			return uuid.Nil, ErrDimensionMismatch
		}
	}

	// If a batch Apply is in progress, buffer the signal instead of writing
	// directly to the store, and answer the caller from the snapshot the
	// batch published. The store is not touched: the Store contract does not
	// promise that View may run concurrently with Update.
	// Every distance is measured in centred space (geometry.go). The stored
	// copy stays raw; the projection is re-derived on read.
	emb := t.projector().Project(sig.Embedding)

	if t.applyInProgress.Load() {
		provisional := t.provisionalStory(emb, time.Now())
		select {
		case t.ingestBuffer <- sig:
			return provisional, nil
		case <-ctx.Done():
			return uuid.Nil, ctx.Err()
		}
	}

	var assignedID uuid.UUID
	emitted := false
	err := t.cfg.Store.Update(func(tx Tx) error {
		// Locate any existing copy first. Re-ingestion of a signal that
		// already lives in a story is a strict no-op: batch placements are
		// authoritative, and a duplicate delivery must neither duplicate nor
		// move the stored copy.
		curStory, isOutlier, hasIndex, err := readSignalLoc(tx, sig.ID)
		if err != nil {
			return err
		}
		if hasIndex && !isOutlier {
			assignedID = curStory
			return nil
		}

		bestStory, d, found, err := t.findNearestStory(tx, emb)
		if err != nil {
			return err
		}

		if found {
			thresh := t.calcThreshold(bestStory)
			if d <= thresh {
				assignedID = bestStory.ID
				rec := storyRecord{
					State:              bestStory.State,
					Centroid:           bestStory.Centroid,
					RecentCentroid:     bestStory.RecentCentroid,
					Radius:             bestStory.Radius,
					CreatedAt:          bestStory.CreatedAt,
					LastSignalAt:       bestStory.LastSignalAt,
					MeanDistance:       bestStory.MeanDistance,
					Sigma:              bestStory.Sigma,
					SignalCount:        bestStory.SignalCount,
					FrozenMeanDistance: bestStory.FrozenMeanDistance,
					FrozenSigma:        bestStory.FrozenSigma,
				}

				sigKey := keys.Signal(bestStory.ID, sig.ID)
				existing, err := tx.Get(sigKey)
				if err != nil {
					return err
				}
				if existing == nil {
					encoded, err := t.cfg.Codec.Encode(sig)
					if err != nil {
						return fmt.Errorf("encode signal: %w", err)
					}
					if err := tx.Put(sigKey, encoded); err != nil {
						return err
					}
					// A promoted signal leaves the outlier bucket; drop any
					// stale outlier copy. This is a no-op for signals never
					// stored there.
					if err := tx.Delete(keys.Outlier(sig.ID)); err != nil {
						return err
					}
					if err := writeSignalLoc(tx, sig.ID, bestStory.ID, false); err != nil {
						return err
					}
					emitted = true
				} else if !hasIndex {
					// A copy exists under this story without an index entry
					// (for example a pre-seeded store). Backfill the index.
					if err := writeSignalLoc(tx, sig.ID, bestStory.ID, false); err != nil {
						return err
					}
				}

				// LastSignalAt advances monotonically; out-of-order signals
				// do not regress it.
				if sig.At.After(rec.LastSignalAt) {
					rec.LastSignalAt = sig.At
				}

				// Reactivation clears the frozen and live statistics: the
				// story re-enters cold-start until the next batch run
				// recomputes live statistics.
				if rec.State == StoryStateDormant {
					rec.State = StoryStateActive
					rec.MeanDistance = 0
					rec.Sigma = 0
					rec.SignalCount = 0
					rec.FrozenMeanDistance = 0
					rec.FrozenSigma = 0
				}

				return t.writeStoryMeta(tx, bestStory.ID, bestStory.LastSignalAt, rec)
			}
		}

		// No story matched or within threshold -> write to outlier bucket.
		assignedID = uuid.Nil
		encoded, err := t.cfg.Codec.Encode(sig)
		if err != nil {
			return fmt.Errorf("encode signal: %w", err)
		}
		if err := tx.Put(keys.Outlier(sig.ID), encoded); err != nil {
			return err
		}
		return writeSignalLoc(tx, sig.ID, uuid.Nil, true)
	})

	if err != nil {
		return uuid.Nil, err
	}

	if emitted && assignedID != uuid.Nil {
		t.emit(StoryEvent[T]{
			Kind:     EventDraftAssigned,
			StoryID:  assignedID,
			SignalID: sig.ID,
			At:       time.Now(),
		})
	}

	return assignedID, nil
}

// findNearestStory finds the nearest active or dormant story centroid for emb.
// Candidates come from the Tier 3 Active Context: stories whose last signal is
// at least ActiveContextWindow old are not anchors. The t: time index is
// scanned rather than the full s: prefix, so the cost is proportional to the
// number of candidate stories rather than the number of stored signals.
func (t *Tracker[T]) findNearestStory(tx Tx, emb []float32) (StoryMeta, float64, bool, error) {
	var bestStory StoryMeta
	bestDist := math.MaxFloat64
	found := false

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
		d := dist.CosineDistance(emb, recentOrCentroid(rec))
		if d < bestDist {
			bestDist = d
			bestStory = storyMetaFromRecord(id, rec)
			found = true
		}
		return nil
	})

	return bestStory, bestDist, found, err
}
