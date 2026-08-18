package story

// The persisted shapes and the store access that reads and writes them:
// story records, the time index, the signal location index, and the global
// calibration state. Nothing outside this file marshals a record.

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/keys"
)

// The persisted record shapes and the store access that reads and writes them.
// Nothing outside this file marshals a record or touches the time index.

type storyRecord struct {
	State              StoryState `json:"state"`
	Centroid           []float32  `json:"centroid"`
	RecentCentroid     []float32  `json:"recent_centroid,omitempty"`
	Radius             float64    `json:"radius"`
	CreatedAt          time.Time  `json:"created_at"`
	LastSignalAt       time.Time  `json:"last_signal_at"`
	MeanDistance       float64    `json:"mean_distance,omitempty"`
	Sigma              float64    `json:"sigma,omitempty"`
	SignalCount        int        `json:"signal_count,omitempty"`
	FrozenMeanDistance float64    `json:"frozen_mean_distance,omitempty"`
	FrozenSigma        float64    `json:"frozen_sigma,omitempty"`
}

// calibState is the JSON-serialised form of the global calibration state
// stored at keys.CalibState().
type calibState struct {
	SigmaGlobal float64   `json:"sigma_global"`
	Dim         int       `json:"dim"`
	LastBatchAt time.Time `json:"last_batch_at"`

	// Mean is the corpus mean direction subtracted from every embedding
	// before any distance is measured. Empty until the first batch run
	// measures one; see geometry.go.
	Mean []float32 `json:"mean,omitempty"`
}

// storyRecord is the JSON-serialised form of story metadata stored at
// keys.StoryMeta(). It mirrors StoryMeta but keeps JSON tags out of the
// public type.
// recentOrCentroid returns the recency centroid, falling back to the lifetime
// centroid for stories with no members inside ActiveContextWindow — every
// Dormant story, and any Active one whose recent traffic has lapsed.
func recentOrCentroid(rec storyRecord) []float32 {
	if len(rec.RecentCentroid) > 0 {
		return rec.RecentCentroid
	}
	return rec.Centroid
}

// storyMetaFromRecord converts a persisted storyRecord into the public
// StoryMeta value.
func storyMetaFromRecord(id uuid.UUID, rec storyRecord) StoryMeta {
	return StoryMeta{
		ID:                 id,
		State:              rec.State,
		Centroid:           rec.Centroid,
		RecentCentroid:     recentOrCentroid(rec),
		Radius:             rec.Radius,
		CreatedAt:          rec.CreatedAt,
		LastSignalAt:       rec.LastSignalAt,
		MeanDistance:       rec.MeanDistance,
		Sigma:              rec.Sigma,
		SignalCount:        rec.SignalCount,
		FrozenMeanDistance: rec.FrozenMeanDistance,
		FrozenSigma:        rec.FrozenSigma,
	}
}

// readStoryMeta reads and decodes story metadata for id from tx.
func (t *Tracker[T]) readStoryMeta(tx Tx, id uuid.UUID) (StoryMeta, error) {
	b, err := tx.Get(keys.StoryMeta(id))
	if err != nil {
		return StoryMeta{}, err
	}
	if b == nil {
		return StoryMeta{}, fmt.Errorf("story %s: %w", id, ErrNotFound)
	}
	var rec storyRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return StoryMeta{}, fmt.Errorf("decode story %s: %w", id, err)
	}
	return storyMetaFromRecord(id, rec), nil
}

// writeStoryMeta persists rec for storyID and keeps the story's time-index
// entry current. When LastSignalAt changes, the previous index entry (if
// any) is removed and a fresh one is written so the index carries at most
// one entry per story.
func (t *Tracker[T]) writeStoryMeta(tx Tx, storyID uuid.UUID, oldLastSignalAt time.Time, rec storyRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := tx.Put(keys.StoryMeta(storyID), b); err != nil {
		return err
	}
	if rec.LastSignalAt.Equal(oldLastSignalAt) {
		return nil
	}
	if !oldLastSignalAt.IsZero() {
		if err := tx.Delete(keys.TimeIndex(oldLastSignalAt.Unix(), storyID)); err != nil {
			return err
		}
	}
	if !rec.LastSignalAt.IsZero() {
		// The time index carries no payload; a non-empty sentinel satisfies
		// the Store contract that values must not be empty.
		if err := tx.Put(keys.TimeIndex(rec.LastSignalAt.Unix(), storyID), []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

// The canonical signal record: the one authoritative copy of a signal, held
// independently of where its facets are placed.

// writeCanonicalSignal stores the signal at its canonical key if no copy is
// there yet. The record is written once and never rewritten: a re-delivery of
// the same signal ID must not overwrite what a batch run has already placed
// facets against.
func (t *Tracker[T]) writeCanonicalSignal(tx Tx, sig Signal[T]) error {
	key := keys.CanonicalSignal(sig.ID)
	existing, err := tx.Get(key)
	if err != nil {
		return err
	}
	if existing != nil {
		return nil
	}
	encoded, err := t.cfg.Codec.Encode(sig)
	if err != nil {
		return fmt.Errorf("encode signal: %w", err)
	}
	return tx.Put(key, encoded)
}

// readCanonicalSignal reads the canonical record for a signal. It reports
// found=false when no record exists, which is how a caller distinguishes an
// unknown signal from one whose facets are merely all unplaced.
func (t *Tracker[T]) readCanonicalSignal(tx Tx, id uuid.UUID) (Signal[T], bool, error) {
	b, err := tx.Get(keys.CanonicalSignal(id))
	if err != nil {
		return Signal[T]{}, false, err
	}
	if b == nil {
		return Signal[T]{}, false, nil
	}
	sig, err := t.cfg.Codec.Decode(b)
	if err != nil {
		return Signal[T]{}, false, fmt.Errorf("decode signal %s: %w", id, err)
	}
	return sig, true, nil
}

// Facet placement. A facet lives in exactly one of three states: under a story,
// in the outlier bucket, or nowhere. The marker key spaces are what a scan
// walks; the location index is the same information keyed by signal, so Ingest
// can ask "where does this signal live" without scanning anything. Both are
// written in the same transaction, so they cannot disagree.

// markerValue is the payload of a facet membership or outlier key. The keys
// carry the information; the Store contract only forbids an empty value.
var markerValue = []byte{1}

// placeFacet records that one facet of a signal belongs to a story, and clears
// any outlier marker the facet held.
func placeFacet(tx Tx, storyID, signalID uuid.UUID, facet int) error {
	if err := tx.Put(keys.FacetMember(storyID, signalID, facet), markerValue); err != nil {
		return err
	}
	return tx.Delete(keys.OutlierFacet(signalID, facet))
}

// unplaceFacet removes a facet's membership of a story. It does not touch the
// canonical record: the caller decides whether the signal still has a reason to
// exist (see gcCanonicalSignal).
func unplaceFacet(tx Tx, storyID, signalID uuid.UUID, facet int) error {
	return tx.Delete(keys.FacetMember(storyID, signalID, facet))
}

// holdFacetOutlier records that a facet is unplaced.
func holdFacetOutlier(tx Tx, signalID uuid.UUID, facet int) error {
	return tx.Put(keys.OutlierFacet(signalID, facet), markerValue)
}

// dropFacetOutlier removes a facet from the outlier bucket.
func dropFacetOutlier(tx Tx, signalID uuid.UUID, facet int) error {
	return tx.Delete(keys.OutlierFacet(signalID, facet))
}

// readSignalLocSet reads the per-facet location index for a signal. hasIndex
// reports whether an entry exists at all, which distinguishes a signal that has
// never been placed from one whose facets are all unplaced.
func readSignalLocSet(tx Tx, signalID uuid.UUID) (locs []keys.FacetLoc, hasIndex bool, err error) {
	b, err := tx.Get(keys.SignalLoc(signalID))
	if err != nil {
		return nil, false, err
	}
	if b == nil {
		return nil, false, nil
	}
	locs, ok := keys.ParseSignalLocSet(b)
	if !ok {
		// A malformed index is treated as absent. It is derived state, so the
		// next write rebuilds it rather than the read failing.
		return nil, false, nil
	}
	return locs, true, nil
}

// writeSignalLocSet upserts the per-facet location index for a signal.
func writeSignalLocSet(tx Tx, signalID uuid.UUID, locs []keys.FacetLoc) error {
	return tx.Put(keys.SignalLoc(signalID), keys.EncodeSignalLocSet(locs))
}

// placedStories returns the distinct stories the given facet locations name,
// sorted. It is how a signal's membership is derived from its facets'.
func placedStories(locs []keys.FacetLoc) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(locs))
	for _, l := range locs {
		if !l.IsOutlier && l.StoryID != uuid.Nil {
			ids = append(ids, l.StoryID)
		}
	}
	return storyIDSet(ids...)
}

// evictOutlierFacets removes every unplaced facet of a signal from the outlier
// bucket, clears their entries in the location index, and drops the canonical
// record if nothing is left holding it.
//
// The TTL is a property of the signal's timestamp rather than of any one facet,
// so all of a signal's unplaced facets age out together. Facets the signal has
// placed in stories are untouched: a partially placed signal keeps its record
// and its memberships.
func evictOutlierFacets(tx Tx, signalID uuid.UUID) error {
	locs, hasIndex, err := readSignalLocSet(tx, signalID)
	if err != nil {
		return err
	}
	for facet, loc := range locs {
		if !loc.IsOutlier {
			continue
		}
		if err := dropFacetOutlier(tx, signalID, facet); err != nil {
			return err
		}
		locs[facet] = keys.FacetLoc{}
	}
	if hasIndex {
		if err := writeSignalLocSet(tx, signalID, locs); err != nil {
			return err
		}
	}
	return gcCanonicalSignal(tx, signalID)
}

// gcCanonicalSignal deletes a signal's canonical record and location index once
// no facet of it remains anywhere — no membership under any story, and nothing
// in the outlier bucket. It is the lifetime rule: the payload outlives any one
// placement, but not all of them.
//
// It must run in the same transaction as the delete that removed the last
// facet, or a crash between the two leaves a record nothing references.
func gcCanonicalSignal(tx Tx, signalID uuid.UUID) error {
	locs, hasIndex, err := readSignalLocSet(tx, signalID)
	if err != nil {
		return err
	}
	if hasIndex {
		for _, l := range locs {
			if l.IsOutlier || l.StoryID != uuid.Nil {
				return nil
			}
		}
	}
	if err := tx.Delete(keys.CanonicalSignal(signalID)); err != nil {
		return err
	}
	return tx.Delete(keys.SignalLoc(signalID))
}

// Global calibration state: sigma_global and the corpus mean, loaded at open and
// rewritten at the end of every batch run.

// loadCalibState reads persisted calibration state from the store, if any.
func (t *Tracker[T]) loadCalibState() error {
	return t.cfg.Store.View(func(tx Tx) error {
		b, err := tx.Get(keys.CalibState())
		if err != nil || b == nil {
			return err
		}
		var s calibState
		if err := json.Unmarshal(b, &s); err != nil {
			return fmt.Errorf("decode calib state: %w", err)
		}
		if s.Dim > 0 {
			t.dim.Store(int32(s.Dim))
		}
		t.calibMu.Lock()
		t.sigmaGlobal = s.SigmaGlobal
		t.lastBatch = s.LastBatchAt
		t.mean = s.Mean
		t.calibMu.Unlock()
		return nil
	})
}

// saveCalibState writes the current calibration state to the store inside tx.
func (t *Tracker[T]) saveCalibState(tx Tx) error {
	t.calibMu.RLock()
	s := calibState{
		SigmaGlobal: t.sigmaGlobal,
		Dim:         int(t.dim.Load()),
		LastBatchAt: t.lastBatch,
		Mean:        t.mean,
	}
	t.calibMu.RUnlock()

	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return tx.Put(keys.CalibState(), b)
}
