package story

// Tracker lifecycle: construction, shutdown, the background batch loop, and
// the subscriber fan-out those events go out through.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Tracker lifecycle: construction, shutdown, and the background batch loop.

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
	// mean is the corpus mean direction every embedding is centred against
	// before a distance is measured; nil until the first batch measures one.
	mean []float32

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
	// storyIndex instead of reading the store.
	applyInProgress atomic.Bool
	ingestBuffer    chan Signal[T]
	storyIndex      atomic.Pointer[activeStoryIndex]

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

	idx, err := t.buildActiveStoryIndex(cfg.Store)
	if err != nil {
		return nil, fmt.Errorf("story: build story index: %w", err)
	}
	t.storyIndex.Store(idx)

	go t.batchLoop()
	return t, nil
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

// Subscriber fan-out.

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
