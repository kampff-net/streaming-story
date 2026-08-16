package story

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/dist"
)

// Tracker ingests a stream of signals and groups them into evolving stories
// using a hybrid real-time / periodic-batch clustering strategy.
//
// Tracker is safe for concurrent use. Each Subscribe call returns an
// independent event channel.
type Tracker[T any] struct {
	cfg Config[T]

	// dim is the embedding dimensionality, set atomically on the first
	// successful Ingest call. 0 means unset.
	dim atomic.Int32

	// calibration state — sigmaGlobal and lastBatch are written only by the
	// batch goroutine and read by Ingest; protected by calibMu.
	calibMu     sync.RWMutex
	sigmaGlobal float64
	lastBatch   time.Time

	// event subscribers
	subMu     sync.RWMutex
	subs      []chan StoryEvent[T]
	closed    atomic.Bool // set before subscriber channels are closed
	closeOnce sync.Once   // makes Close idempotent

	// closeMu excludes in-flight Ingest calls from Close, so no store write
	// can start after the store has been closed.
	closeMu sync.RWMutex

	// batch-apply concurrency: while applyInProgress is set, Ingest writes
	// to ingestBuffer instead of directly to the store, and answers from
	// draftSnapshot instead of reading the store.
	applyInProgress atomic.Bool
	ingestBuffer    chan Signal[T]
	draftSnapshot   atomic.Pointer[draftSnapshot]

	// lifecycle
	stopCh  chan struct{}
	stopped chan struct{}
}

// NewTracker creates a Tracker using the provided configuration.
// The background batch goroutine is started immediately.
// Call Close to stop it and release resources.
func NewTracker[T any](cfg Config[T]) (*Tracker[T], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	t := &Tracker[T]{
		cfg:          cfg,
		ingestBuffer: make(chan Signal[T], cfg.IngestBufferCap),
		stopCh:       make(chan struct{}),
		stopped:      make(chan struct{}),
	}

	if err := t.loadCalibState(); err != nil {
		return nil, fmt.Errorf("story: load calibration state: %w", err)
	}

	go t.batchLoop()
	return t, nil
}

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
	if t.applyInProgress.Load() {
		provisional := t.provisionalStory(sig.Embedding, time.Now())
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

		bestStory, d, found, err := t.findNearestStory(tx, sig.Embedding)
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
					Radius:             bestStory.Radius,
					CreatedAt:          bestStory.CreatedAt,
					LastSignalAt:       bestStory.LastSignalAt,
					MeanDistance:       bestStory.MeanDistance,
					Sigma:              bestStory.Sigma,
					SignalCount:        bestStory.SignalCount,
					FrozenMeanDistance: bestStory.FrozenMeanDistance,
					FrozenSigma:        bestStory.FrozenSigma,
				}

				sigKey := keySignal(bestStory.ID, sig.ID)
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
					if err := tx.Delete(keyOutlier(sig.ID)); err != nil {
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
		if err := tx.Put(keyOutlier(sig.ID), encoded); err != nil {
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

// Subscribe returns a channel of real-time and batch-refined events.
// Events are dropped (and EventBufferOverflow emitted to the channel) if
// the buffer fills. Each call returns an independent channel. The channel
// is closed when the Tracker is closed. Calling Subscribe after Close
// returns an already-closed channel.
func (t *Tracker[T]) Subscribe() <-chan StoryEvent[T] {
	ch := make(chan StoryEvent[T], t.cfg.EventBufferSize)
	t.subMu.Lock()
	if t.closed.Load() {
		close(ch)
	} else {
		t.subs = append(t.subs, ch)
	}
	t.subMu.Unlock()
	return ch
}

// Close stops the background batch goroutine, waits for the current batch
// run (if any) to complete, closes all subscriber channels, and closes the
// store. It is safe to call more than once; subsequent calls return nil.
func (t *Tracker[T]) Close() error {
	var closeErr error
	t.closeOnce.Do(func() {
		close(t.stopCh)
		<-t.stopped

		// Wait for in-flight Ingest calls to finish, then bar new ones before
		// the store goes away.
		t.closeMu.Lock()
		defer t.closeMu.Unlock()

		t.subMu.Lock()
		t.closed.Store(true)
		subs := t.subs
		t.subs = nil
		for _, ch := range subs {
			close(ch)
		}
		t.subMu.Unlock()

		closeErr = t.cfg.Store.Close()
	})
	return closeErr
}

// SignalID derives the UUID v5 signal ID for a domain key under this
// Tracker's namespace (Config.Namespace, or TrackerNamespace when unset).
//
// Deriving IDs this way makes re-ingesting the same source item a no-op
// rather than a duplicate signal. Prefer it over calling uuid.NewSHA1 with
// TrackerNamespace directly, which ignores a configured namespace.
func (t *Tracker[T]) SignalID(domainKey string) uuid.UUID {
	return uuid.NewSHA1(t.cfg.Namespace, []byte(domainKey))
}

// Story returns current metadata for a single story.
func (t *Tracker[T]) Story(id uuid.UUID) (StoryMeta, error) {
	var meta StoryMeta
	err := t.cfg.Store.View(func(tx Tx) error {
		var err error
		meta, err = t.readStoryMeta(tx, id)
		return err
	})
	return meta, err
}

// Stories returns an iterator over stories in the given state.
// Pass StoryStateAny to iterate all stories.
func (t *Tracker[T]) Stories(state StoryState) iter.Seq[StoryMeta] {
	return func(yield func(StoryMeta) bool) {
		_ = t.cfg.Store.View(func(tx Tx) error {
			return tx.ScanPrefix([]byte("s:"), func(key, val []byte) error {
				id, ok := parseStoryMetaKey(key)
				if !ok {
					return nil
				}
				var rec storyRecord
				if err := json.Unmarshal(val, &rec); err != nil {
					return nil
				}
				if state != StoryStateAny && rec.State != state {
					return nil
				}
				if !yield(storyMetaFromRecord(id, rec)) {
					return errors.New("stop iteration")
				}
				return nil
			})
		})
	}
}

// SignalsOf returns an iterator over all signals belonging to storyID.
// Signal data is retained through archival, so Archived stories are
// fully iterable.
func (t *Tracker[T]) SignalsOf(storyID uuid.UUID) iter.Seq2[Signal[T], error] {
	return func(yield func(Signal[T], error) bool) {
		prefix := keySignalPrefix(storyID)
		_ = t.cfg.Store.View(func(tx Tx) error {
			return tx.ScanPrefix(prefix, func(key, val []byte) error {
				sig, err := t.cfg.Codec.Decode(val)
				if err != nil {
					if !yield(Signal[T]{}, err) {
						return errors.New("stop iteration")
					}
					return nil
				}
				if !yield(sig, nil) {
					return errors.New("stop iteration")
				}
				return nil
			})
		})
	}
}

// Signal returns the signal with the given ID, wherever it currently lives:
// attached to a story or held in the outlier bucket. Callers that need to know
// which of the two, or which story, should use SignalsOf or Outliers instead;
// this method deliberately reports only the signal.
//
// It returns an error wrapping ErrNotFound when the ID has no location-index
// entry, when the index points at a record that no longer exists, or when the
// index value is malformed. A signal evicted from the outlier bucket or
// belonging to a retired story is therefore not found, which is the intended
// behavior.
func (t *Tracker[T]) Signal(id uuid.UUID) (Signal[T], error) {
	var sig Signal[T]
	err := t.cfg.Store.View(func(tx Tx) error {
		storyID, isOutlier, hasIndex, err := readSignalLoc(tx, id)
		if err != nil {
			return err
		}
		if !hasIndex {
			return fmt.Errorf("signal %s: %w", id, ErrNotFound)
		}

		var key []byte
		if isOutlier {
			key = keyOutlier(id)
		} else {
			key = keySignal(storyID, id)
		}

		b, err := tx.Get(key)
		if err != nil {
			return err
		}
		if b == nil {
			return fmt.Errorf("signal %s: %w", id, ErrNotFound)
		}

		s, err := t.cfg.Codec.Decode(b)
		if err != nil {
			return fmt.Errorf("decode signal %s: %w", id, err)
		}
		sig = s
		return nil
	})
	return sig, err
}

// calcThreshold calculates the dynamic distance threshold T_assign(story).
//
// T_assign(story) = mean_distance(story) + AssignmentK × σ(story).
//
// Dormant stories use the statistics frozen at the Dormant transition.
// Active stories use live per-story statistics once the story has reached
// ColdStartMinSignals window signals; below that the story is in cold-start
// and falls back to AssignmentK × σ_global. σ is floored at
// SigmaFloor × σ_global to prevent the threshold collapsing on near-identical
// first signals.
func (t *Tracker[T]) calcThreshold(story StoryMeta) float64 {
	t.calibMu.RLock()
	sigmaGlobal := t.sigmaGlobal
	t.calibMu.RUnlock()

	if sigmaGlobal == 0 {
		// No batch has completed yet, so σ_global has never been measured.
		// InitialSigmaGlobal stands in until the first run seeds it.
		sigmaGlobal = t.cfg.InitialSigmaGlobal
	}
	floor := t.cfg.SigmaFloor * sigmaGlobal

	if story.State == StoryStateDormant {
		sigma := story.FrozenSigma
		if sigma == 0 {
			sigma = sigmaGlobal
		}
		if sigma < floor {
			sigma = floor
		}
		return story.FrozenMeanDistance + t.cfg.AssignmentK*sigma
	}

	if story.SignalCount >= t.cfg.ColdStartMinSignals {
		sigma := story.Sigma
		if sigma < floor {
			sigma = floor
		}
		return story.MeanDistance + t.cfg.AssignmentK*sigma
	}

	return t.cfg.AssignmentK * sigmaGlobal
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

	cutoff := keyTimeIndexFrom(time.Now().Add(-t.cfg.ActiveContextWindow).Unix())

	err := tx.ScanRange(cutoff, []byte("u:"), func(key, val []byte) error {
		id, ok := parseTimeIndexKey(key)
		if !ok {
			return nil
		}
		var rec storyRecord
		b, err := tx.Get(keyStoryMeta(id))
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
		d := dist.CosineDistance(emb, rec.Centroid)
		if d < bestDist {
			bestDist = d
			bestStory = storyMetaFromRecord(id, rec)
			found = true
		}
		return nil
	})

	return bestStory, bestDist, found, err
}

// emit delivers ev to all current subscribers. If a subscriber's buffer is
// full, an EventBufferOverflow event is sent instead; if that also fails the
// event is silently dropped.
func (t *Tracker[T]) emit(ev StoryEvent[T]) {
	t.subMu.RLock()
	defer t.subMu.RUnlock()

	if t.closed.Load() {
		return
	}

	for _, ch := range t.subs {
		select {
		case ch <- ev:
		default:
			select {
			case <-ch:
			default:
			}
			overflow := StoryEvent[T]{Kind: EventBufferOverflow, At: time.Now()}
			select {
			case ch <- overflow:
			default:
			}
		}
	}
}

// loadCalibState reads persisted calibration state from the store, if any.
func (t *Tracker[T]) loadCalibState() error {
	return t.cfg.Store.View(func(tx Tx) error {
		b, err := tx.Get(keyCalibState())
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
	}
	t.calibMu.RUnlock()

	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return tx.Put(keyCalibState(), b)
}

// readStoryMeta reads and decodes story metadata for id from tx.
func (t *Tracker[T]) readStoryMeta(tx Tx, id uuid.UUID) (StoryMeta, error) {
	b, err := tx.Get(keyStoryMeta(id))
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

// storyMetaFromRecord converts a persisted storyRecord into the public
// StoryMeta value.
func storyMetaFromRecord(id uuid.UUID, rec storyRecord) StoryMeta {
	return StoryMeta{
		ID:                 id,
		State:              rec.State,
		Centroid:           rec.Centroid,
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

// parseStoryMetaKey extracts the story ID from a "s:{storyID}:m" metadata key.
// It returns ok=false for keys that do not match the metadata key shape
// (for example signal keys, which share the "s:" prefix).
func parseStoryMetaKey(key []byte) (uuid.UUID, bool) {
	if len(key) < len("s::m") || string(key[len(key)-2:]) != ":m" {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(string(key[2 : len(key)-2]))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
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
	if err := tx.Put(keyStoryMeta(storyID), b); err != nil {
		return err
	}
	if rec.LastSignalAt.Equal(oldLastSignalAt) {
		return nil
	}
	if !oldLastSignalAt.IsZero() {
		if err := tx.Delete(keyTimeIndex(oldLastSignalAt.Unix(), storyID)); err != nil {
			return err
		}
	}
	if !rec.LastSignalAt.IsZero() {
		// The time index carries no payload; a non-empty sentinel satisfies
		// the Store contract that values must not be empty.
		if err := tx.Put(keyTimeIndex(rec.LastSignalAt.Unix(), storyID), []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

// readSignalLoc reads the location-index entry for a signal. It reports
// whether an entry exists (hasIndex), whether the signal lives in the outlier
// bucket (isOutlier), and, for story membership, the owning story ID.
func readSignalLoc(tx Tx, signalID uuid.UUID) (storyID uuid.UUID, isOutlier, hasIndex bool, err error) {
	b, err := tx.Get(keySignalLoc(signalID))
	if err != nil {
		return uuid.Nil, false, false, err
	}
	if b == nil {
		return uuid.Nil, false, false, nil
	}
	storyID, isOutlier, ok := parseSignalLoc(b)
	if !ok {
		return uuid.Nil, false, false, nil
	}
	return storyID, isOutlier, true, nil
}

// writeSignalLoc upserts the location-index entry for a signal. Pass a nil
// storyID with isOutlier=true to record the outlier bucket.
func writeSignalLoc(tx Tx, signalID uuid.UUID, storyID uuid.UUID, isOutlier bool) error {
	var val []byte
	if isOutlier {
		val = []byte("o")
	} else {
		val = fmt.Appendf(nil, "s:%s", storyID)
	}
	return tx.Put(keySignalLoc(signalID), val)
}

// parseSignalLoc decodes a location-index value: "s:{storyID}" for story
// membership or "o" for the outlier bucket.
func parseSignalLoc(val []byte) (storyID uuid.UUID, isOutlier, ok bool) {
	if len(val) == 1 && val[0] == 'o' {
		return uuid.Nil, true, true
	}
	if len(val) < len("s:") || val[0] != 's' || val[1] != ':' {
		return uuid.Nil, false, false
	}
	id, err := uuid.Parse(string(val[2:]))
	if err != nil {
		return uuid.Nil, false, false
	}
	return id, false, true
}

// batchLoop runs the periodic batch re-clustering cycle until stopCh is closed.
func (t *Tracker[T]) batchLoop() {
	defer close(t.stopped)

	ticker := time.NewTicker(t.cfg.BatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.runBatch()
		}
	}
}
