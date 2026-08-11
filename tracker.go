package story

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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
			// buffered; will be drained by the batch goroutine after Apply commits
			return uuid.Nil, nil // TODO: return draft assignment from in-memory centroid lookup
		case <-ctx.Done():
			return uuid.Nil, ctx.Err()
		}
	}

	panic("not implemented: draft-phase centroid lookup and store write")
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
	panic("not implemented")
}

// SignalsOf returns an iterator over all signals belonging to storyID.
// Signal data is retained through archival, so Archived stories are
// fully iterable.
func (t *Tracker[T]) SignalsOf(storyID uuid.UUID) iter.Seq2[Signal[T], error] {
	panic("not implemented")
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
	panic("not implemented")
}
