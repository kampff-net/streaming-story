package story

// The persisted shapes and the store access that reads and writes them:
// story records, the time index, the signal location index, and the global
// calibration state. Nothing outside this file marshals a record.

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/keys"
)

// The persisted record shapes and the store access that reads and writes them.
// Nothing outside this file marshals a record or touches the time index.

type storyRecord struct {
	State              StoryState `cbor:"0,keyasint"`
	Centroid           []float32  `cbor:"1,keyasint"`
	RecentCentroid     []float32  `cbor:"2,keyasint,omitempty"`
	Radius             float64    `cbor:"3,keyasint"`
	CreatedAt          time.Time  `cbor:"4,keyasint"`
	LastSignalAt       time.Time  `cbor:"5,keyasint"`
	MeanDistance       float64    `cbor:"6,keyasint,omitempty"`
	Sigma              float64    `cbor:"7,keyasint,omitempty"`
	SignalCount        int        `cbor:"8,keyasint,omitempty"`
	FrozenMeanDistance float64    `cbor:"9,keyasint,omitempty"`
	FrozenSigma        float64    `cbor:"10,keyasint,omitempty"`
	ReactivatedAt      time.Time  `cbor:"11,keyasint,omitempty"`
	StatsAt            time.Time  `cbor:"12,keyasint,omitempty"`
	WasSuppressed      bool       `cbor:"13,keyasint,omitempty"`
	SuppressionReason  string     `cbor:"14,keyasint,omitempty"`
}

// storyHot is the CBOR-serialised form of the hot metadata written by Ingest
// at keys.StoryHot(storyID).
type storyHot struct {
	State         StoryState `cbor:"0,keyasint"`
	LastSignalAt  time.Time  `cbor:"1,keyasint"`
	ReactivatedAt time.Time  `cbor:"2,keyasint,omitempty"`
}

// calibState is the CBOR-serialised form of the global calibration state
// stored at keys.CalibState().
type calibState struct {
	SigmaGlobal float64   `cbor:"0,keyasint"`
	Dim         int       `cbor:"1,keyasint"`
	LastBatchAt time.Time `cbor:"2,keyasint"`

	// Mean is the corpus mean direction subtracted from every embedding
	// before any distance is measured. Empty until the first batch run
	// measures one; see geometry.go.
	Mean []float32 `cbor:"3,keyasint,omitempty"`
}

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
		ReactivatedAt:      rec.ReactivatedAt,
		StatsAt:            rec.StatsAt,
		WasSuppressed:      rec.WasSuppressed,
		SuppressionReason:  rec.SuppressionReason,
	}
}

// readStoryRecord reads and decodes the persisted record for id from tx,
// overlaying the hot state (keys.StoryHot) on top of the batch-owned record
// (keys.StoryMeta). It is the record-shaped counterpart of readStoryMeta,
// used by callers that need to mutate and write the record back rather than
// just read it.
func (t *Tracker[T]) readStoryRecord(r Reader, id uuid.UUID) (storyRecord, error) {
	b, err := r.Get(keys.StoryMeta(id))
	if err != nil {
		return storyRecord{}, err
	}
	if b == nil {
		return storyRecord{}, fmt.Errorf("story %s: %w", id, ErrNotFound)
	}
	var rec storyRecord
	if err := cborStrictDecMode.Unmarshal(b, &rec); err != nil {
		return storyRecord{}, fmt.Errorf("decode story %s: %w", id, err)
	}
	bh, err := r.Get(keys.StoryHot(id))
	if err != nil {
		return storyRecord{}, err
	}
	if bh != nil {
		var hot storyHot
		if err := cborStrictDecMode.Unmarshal(bh, &hot); err != nil {
			return storyRecord{}, fmt.Errorf("decode story hot %s: %w", id, err)
		}
		rec.State = hot.State
		if hot.LastSignalAt.After(rec.LastSignalAt) {
			rec.LastSignalAt = hot.LastSignalAt
		}
		if !hot.ReactivatedAt.IsZero() {
			rec.ReactivatedAt = hot.ReactivatedAt
		}
	}
	return rec, nil
}

// readStoryMeta reads and decodes story metadata for id from tx, overlaying
// the hot state (keys.StoryHot) on top of the batch-owned record (keys.StoryMeta).
func (t *Tracker[T]) readStoryMeta(r Reader, id uuid.UUID) (StoryMeta, error) {
	rec, err := t.readStoryRecord(r, id)
	if err != nil {
		return StoryMeta{}, err
	}
	return storyMetaFromRecord(id, rec), nil
}

// writeStoryHot persists hot metadata for storyID and updates the time-index.
func (t *Tracker[T]) writeStoryHot(tx Tx, storyID uuid.UUID, oldLastSignalAt time.Time, hot storyHot) error {
	b, err := cborEncMode.Marshal(hot)
	if err != nil {
		return err
	}
	if err := tx.Put(keys.StoryHot(storyID), b); err != nil {
		return err
	}
	if hot.LastSignalAt.Equal(oldLastSignalAt) {
		return nil
	}
	if !oldLastSignalAt.IsZero() {
		if err := tx.Delete(keys.TimeIndex(oldLastSignalAt.Unix(), storyID)); err != nil {
			return err
		}
	}
	if !hot.LastSignalAt.IsZero() {
		// The time index carries no payload; a non-empty sentinel satisfies
		// the Store contract that values must not be empty.
		if err := tx.Put(keys.TimeIndex(hot.LastSignalAt.Unix(), storyID), []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

// writeStoryMeta persists rec for storyID, synchronizing both keys.StoryMeta
// and keys.StoryHot.
func (t *Tracker[T]) writeStoryMeta(tx Tx, storyID uuid.UUID, oldLastSignalAt time.Time, rec storyRecord) error {
	b, err := cborEncMode.Marshal(rec)
	if err != nil {
		return err
	}
	if err := tx.Put(keys.StoryMeta(storyID), b); err != nil {
		return err
	}
	hot := storyHot{
		State:         rec.State,
		LastSignalAt:  rec.LastSignalAt,
		ReactivatedAt: rec.ReactivatedAt,
	}
	return t.writeStoryHot(tx, storyID, oldLastSignalAt, hot)
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
	encoded, err := cborEncMode.Marshal(sig)
	if err != nil {
		return fmt.Errorf("encode signal: %w", err)
	}
	return tx.Put(key, encoded)
}

// reconcileFacetCount brings the store in line with a re-delivery that carries
// fewer facets than the stored record does.
//
// A signal's facet set is normally fixed at first ingest: the record is written
// once, and the facet index in a marker key is only meaningful against it. A
// shorter re-delivery is the one case that genuinely removes facets, and every
// trace of them has to go in this transaction — the markers, their entries in
// the location index, and their vectors in the record. Truncating the index
// alone (which is all the write at the end of Ingest does) leaves markers no
// later read can resolve and no eviction can reach.
//
// Only the embeddings are truncated. At and Data keep the stored values: the
// record stays write-once for the payload, so a re-delivery still cannot
// overwrite what a batch run has already clustered.
func (t *Tracker[T]) reconcileFacetCount(tx Tx, id uuid.UUID, want int) error {
	stored, found, err := t.readCanonicalSignal(tx, id)
	if err != nil || !found {
		return err
	}
	if want >= len(stored.Embeddings) {
		return nil
	}

	locs, _, err := readSignalLocSet(tx, id)
	if err != nil {
		return err
	}
	for facet := want; facet < len(stored.Embeddings); facet++ {
		if err := dropFacetOutlier(tx, id, facet); err != nil {
			return err
		}
		if facet >= len(locs) {
			continue
		}
		if sid := locs[facet].StoryID; sid != uuid.Nil {
			if err := unplaceFacet(tx, sid, id, facet); err != nil {
				return err
			}
		}
		locs[facet] = keys.FacetLoc{}
	}
	if len(locs) > want {
		locs = locs[:want]
	}
	if err := writeSignalLocSet(tx, id, locs); err != nil {
		return err
	}

	stored.Embeddings = stored.Embeddings[:want]
	encoded, err := cborEncMode.Marshal(stored)
	if err != nil {
		return fmt.Errorf("encode signal: %w", err)
	}
	return tx.Put(keys.CanonicalSignal(id), encoded)
}

// readCanonicalSignal reads the canonical record for a signal. It reports
// found=false when no record exists, which is how a caller distinguishes an
// unknown signal from one whose facets are merely all unplaced.
func (t *Tracker[T]) readCanonicalSignal(r Reader, id uuid.UUID) (Signal[T], bool, error) {
	b, err := r.Get(keys.CanonicalSignal(id))
	if err != nil {
		return Signal[T]{}, false, err
	}
	if b == nil {
		return Signal[T]{}, false, nil
	}
	var sig Signal[T]
	if err := cborDecMode.Unmarshal(b, &sig); err != nil {
		return Signal[T]{}, false, fmt.Errorf("decode signal %s: %w", id, err)
	}
	return sig, true, nil
}

// cborSignalHeader is the part of a Signal[T] a batch run needs. Key 0 (ID) and
// key 3 (Data) are deliberately absent: a key the struct does not declare is
// skipped by the decoder without being allocated, copied, or retained. The
// signal's ID is already in hand from the membership key that named it.
type cborSignalHeader struct {
	At         time.Time   `cbor:"1,keyasint"`
	Embeddings []Embedding `cbor:"2,keyasint"`
}

// readSignalHeader reads timestamp and embeddings from the canonical record,
// skipping the caller payload T.
func (t *Tracker[T]) readSignalHeader(r Reader, id uuid.UUID) (time.Time, []Embedding, bool, error) {
	b, err := r.Get(keys.CanonicalSignal(id))
	if err != nil {
		return time.Time{}, nil, false, err
	}
	if b == nil {
		return time.Time{}, nil, false, nil
	}
	var hdr cborSignalHeader
	if err := cborDecMode.Unmarshal(b, &hdr); err != nil {
		return time.Time{}, nil, false, fmt.Errorf("decode signal header %s: %w", id, err)
	}
	return hdr.At, hdr.Embeddings, true, nil
}

// Facet placement. A facet lives in exactly one of three states: under a story,
// in the outlier bucket, or nowhere. The marker key spaces are what a scan
// walks; the location index is the same information keyed by signal, so Ingest
// can ask "where does this signal live" without scanning anything. Both are
// written in the same transaction, so they cannot disagree — with one case that
// has to be enforced rather than assumed: a re-delivery carrying fewer facets
// shortens the index, and reconcileFacetCount removes the markers it drops.

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
func readSignalLocSet(r Reader, signalID uuid.UUID) (locs []keys.FacetLoc, hasIndex bool, err error) {
	b, err := r.Get(keys.SignalLoc(signalID))
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
	// The markers are the truth, not the location index. The index is derived
	// state and can be shorter than the marker set — a re-delivery carrying
	// fewer facets truncates it — so driving eviction from it silently leaves
	// markers behind that nothing can ever resolve or remove.
	facets, err := outlierFacetsOf(tx, signalID)
	if err != nil {
		return err
	}
	locs, hasIndex, err := readSignalLocSet(tx, signalID)
	if err != nil {
		return err
	}
	for _, facet := range facets {
		if err := dropFacetOutlier(tx, signalID, facet); err != nil {
			return err
		}
		if facet < len(locs) {
			locs[facet] = keys.FacetLoc{}
		}
	}
	if hasIndex {
		if err := writeSignalLocSet(tx, signalID, locs); err != nil {
			return err
		}
	}
	return gcCanonicalSignal(tx, signalID)
}

// outlierFacetsOf lists the facet indices of a signal currently held in the
// outlier bucket. The markers are collected before any delete: mutating the
// store from inside its own scan is not something the Store contract promises.
func outlierFacetsOf(r Reader, signalID uuid.UUID) ([]int, error) {
	var facets []int
	err := r.ScanPrefix(keys.OutlierSignalPrefix(signalID), func(key, _ []byte) error {
		_, facet, ok := keys.ParseOutlierFacet(key)
		if !ok {
			return nil
		}
		facets = append(facets, facet)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return facets, nil
}

// gcCanonicalSignal deletes a signal's canonical record and location index once
// no facet of it remains anywhere — no membership under any story, and nothing
// in the outlier bucket. It is the lifetime rule: the payload outlives any one
// placement, but not all of them.
//
// It must run in the same transaction as the delete that removed the last
// facet, or a crash between the two leaves a record nothing references.
func gcCanonicalSignal(tx Tx, signalID uuid.UUID) error {
	// An outlier marker holds the record alive whether or not the location
	// index still mentions it: the markers are authoritative and the index is
	// derived, so a truncated index must never license deleting the payload
	// out from under a marker that still exists.
	outliers, err := outlierFacetsOf(tx, signalID)
	if err != nil {
		return err
	}
	if len(outliers) > 0 {
		return nil
	}
	locs, hasIndex, err := readSignalLocSet(tx, signalID)
	if err != nil {
		return err
	}
	if hasIndex {
		for _, l := range locs {
			if l.StoryID != uuid.Nil {
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
	b, err := t.cfg.Store.Get(keys.CalibState())
	if err != nil || b == nil {
		return err
	}
	var s calibState
	if err := cborStrictDecMode.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("decode calib state: %w", err)
	}
	if s.Dim > 0 {
		t.dim.Store(int32(s.Dim))
	}
	t.calibMu.Lock()
	defer t.calibMu.Unlock()
	t.sigmaGlobal = s.SigmaGlobal
	t.lastBatch = s.LastBatchAt
	t.mean = s.Mean
	return nil
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

	b, err := cborEncMode.Marshal(s)
	if err != nil {
		return err
	}
	return tx.Put(keys.CalibState(), b)
}
