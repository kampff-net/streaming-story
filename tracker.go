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
	subMu  sync.RWMutex
	subs   []chan StoryEvent[T]
	closed atomic.Bool // set before subscriber channels are closed

	// batch-apply concurrency: while applyInProgress is set, Ingest writes
	// to ingestBuffer instead of directly to the store.
	applyInProgress atomic.Bool
	ingestBuffer    chan Signal[T]

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
func (t *Tracker[T]) Ingest(ctx context.Context, sig Signal[T]) (uuid.UUID, error) {
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
	// directly to the store.
	if t.applyInProgress.Load() {
		select {
		case t.ingestBuffer <- sig:
			return uuid.Nil, nil
		case <-ctx.Done():
			return uuid.Nil, ctx.Err()
		}
	}

	var assignedID uuid.UUID
	err := t.cfg.Store.Update(func(tx Tx) error {
		bestStory, d, found, err := t.findNearestStory(tx, sig.Embedding)
		if err != nil {
			return err
		}

		encoded, err := t.cfg.Codec.Encode(sig)
		if err != nil {
			return fmt.Errorf("encode signal: %w", err)
		}

		if found {
			thresh := t.calcThreshold(bestStory)
			if d <= thresh {
				assignedID = bestStory.ID
				if err := tx.Put(keySignal(bestStory.ID, sig.ID), encoded); err != nil {
					return err
				}
				bestStory.LastSignalAt = sig.At
				if bestStory.State == StoryStateDormant {
					bestStory.State = StoryStateActive
					bestStory.FrozenMeanDistance = 0
					bestStory.FrozenSigma = 0
				}
				rec := storyRecord{
					State:              bestStory.State,
					Centroid:           bestStory.Centroid,
					Radius:             bestStory.Radius,
					CreatedAt:          bestStory.CreatedAt,
					LastSignalAt:       bestStory.LastSignalAt,
					FrozenMeanDistance: bestStory.FrozenMeanDistance,
					FrozenSigma:        bestStory.FrozenSigma,
				}
				recBytes, err := json.Marshal(rec)
				if err != nil {
					return err
				}
				if err := tx.Put(keyStoryMeta(bestStory.ID), recBytes); err != nil {
					return err
				}
				if err := tx.Put(keyTimeIndex(sig.At.Unix(), bestStory.ID), nil); err != nil {
					return err
				}
				return nil
			}
		}

		// No story matched or within threshold -> write to outlier bucket
		assignedID = uuid.Nil
		return tx.Put(keyOutlier(sig.ID), encoded)
	})

	if err != nil {
		return uuid.Nil, err
	}

	if assignedID != uuid.Nil {
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
// is closed when the Tracker is closed.
func (t *Tracker[T]) Subscribe() <-chan StoryEvent[T] {
	ch := make(chan StoryEvent[T], t.cfg.EventBufferSize)
	t.subMu.Lock()
	t.subs = append(t.subs, ch)
	t.subMu.Unlock()
	return ch
}

// Close stops the background batch goroutine, waits for the current batch
// run (if any) to complete, closes all subscriber channels, and closes the
// store.
func (t *Tracker[T]) Close() error {
	close(t.stopCh)
	<-t.stopped

	t.subMu.Lock()
	t.closed.Store(true)
	subs := t.subs
	t.subs = nil
	for _, ch := range subs {
		close(ch)
	}
	t.subMu.Unlock()

	return t.cfg.Store.Close()
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
				if len(key) <= 2 || string(key[len(key)-2:]) != ":m" {
					return nil
				}
				var rec storyRecord
				if err := json.Unmarshal(val, &rec); err != nil {
					return nil
				}
				if state != StoryStateAny && rec.State != state {
					return nil
				}
				idStr := string(key[2 : len(key)-2])
				id, err := uuid.Parse(idStr)
				if err != nil {
					return nil
				}
				meta := StoryMeta{
					ID:                 id,
					State:              rec.State,
					Centroid:           rec.Centroid,
					Radius:             rec.Radius,
					CreatedAt:          rec.CreatedAt,
					LastSignalAt:       rec.LastSignalAt,
					FrozenMeanDistance: rec.FrozenMeanDistance,
					FrozenSigma:        rec.FrozenSigma,
				}
				if !yield(meta) {
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

// calcThreshold calculates the dynamic distance threshold T_assign(story).
func (t *Tracker[T]) calcThreshold(story StoryMeta) float64 {
	t.calibMu.RLock()
	sigmaGlobal := t.sigmaGlobal
	t.calibMu.RUnlock()

	if sigmaGlobal == 0 {
		sigmaGlobal = 0.25
	}

	sigma := story.FrozenSigma
	if sigma == 0 {
		sigma = sigmaGlobal
	}

	floor := t.cfg.SigmaFloor * sigmaGlobal
	if sigma < floor {
		sigma = floor
	}

	meanDist := story.FrozenMeanDistance
	return meanDist + t.cfg.AssignmentK*sigma
}

// findNearestStory finds the nearest active or dormant story centroid for emb.
func (t *Tracker[T]) findNearestStory(tx Tx, emb []float32) (StoryMeta, float64, bool, error) {
	var bestStory StoryMeta
	bestDist := math.MaxFloat64
	found := false

	err := tx.ScanPrefix([]byte("s:"), func(key, val []byte) error {
		if len(key) <= 2 || string(key[len(key)-2:]) != ":m" {
			return nil
		}
		var rec storyRecord
		if err := json.Unmarshal(val, &rec); err != nil {
			return nil
		}
		if rec.State == StoryStateArchived || len(rec.Centroid) == 0 {
			return nil
		}
		d := dist.CosineDistance(emb, rec.Centroid)
		if d < bestDist {
			bestDist = d
			idStr := string(key[2 : len(key)-2])
			id, err := uuid.Parse(idStr)
			if err == nil {
				bestStory = StoryMeta{
					ID:                 id,
					State:              rec.State,
					Centroid:           rec.Centroid,
					Radius:             rec.Radius,
					CreatedAt:          rec.CreatedAt,
					LastSignalAt:       rec.LastSignalAt,
					FrozenMeanDistance: rec.FrozenMeanDistance,
					FrozenSigma:        rec.FrozenSigma,
				}
				found = true
			}
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
	return StoryMeta{
		ID:                 id,
		State:              rec.State,
		Centroid:           rec.Centroid,
		Radius:             rec.Radius,
		CreatedAt:          rec.CreatedAt,
		LastSignalAt:       rec.LastSignalAt,
		FrozenMeanDistance: rec.FrozenMeanDistance,
		FrozenSigma:        rec.FrozenSigma,
	}, nil
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

// runBatch executes one full batch re-clustering cycle.
func (t *Tracker[T]) runBatch() {
	t.applyInProgress.Store(true)
	defer func() {
		// Drain ingest buffer
		for {
			select {
			case sig := <-t.ingestBuffer:
				_, _ = t.Ingest(context.Background(), sig)
			default:
				t.applyInProgress.Store(false)
				t.emit(StoryEvent[T]{
					Kind: EventBatchComplete,
					At:   time.Now(),
					BatchSummary: &BatchSummary{
						StoriesCreated: 0,
					},
				})
				return
			}
		}
	}()
}
