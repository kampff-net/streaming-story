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

// readSignalLoc reads the location-index entry for a signal. It reports
// whether an entry exists (hasIndex), whether the signal lives in the outlier
// bucket (isOutlier), and, for story membership, the owning story ID.
func readSignalLoc(tx Tx, signalID uuid.UUID) (storyID uuid.UUID, isOutlier, hasIndex bool, err error) {
	b, err := tx.Get(keys.SignalLoc(signalID))
	if err != nil {
		return uuid.Nil, false, false, err
	}
	if b == nil {
		return uuid.Nil, false, false, nil
	}
	storyID, isOutlier, ok := keys.ParseSignalLoc(b)
	if !ok {
		return uuid.Nil, false, false, nil
	}
	return storyID, isOutlier, true, nil
}

// writeSignalLoc upserts the location-index entry for a signal. Pass a nil
// storyID with isOutlier=true to record the outlier bucket.
func writeSignalLoc(tx Tx, signalID uuid.UUID, storyID uuid.UUID, isOutlier bool) error {
	return tx.Put(keys.SignalLoc(signalID), keys.EncodeSignalLoc(storyID, isOutlier))
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
