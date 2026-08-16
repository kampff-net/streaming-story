package story

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// errStopIteration terminates a range scan early; the caller checks for it
// with errors.Is to distinguish a requested stop from a real failure.
var errStopIteration = errors.New("stop iteration")

// batchSignal is one signal collected for a batch run.
type batchSignal struct {
	id      uuid.UUID
	at      time.Time
	emb     []float32
	storyID uuid.UUID // current story assignment; uuid.Nil for outlier signals
	outlier bool      // stored under the o: prefix
}

// runBatch executes one full batch re-clustering cycle.
func (t *Tracker[T]) runBatch() {
	// Safety net: a panic inside the batch core must not leave Ingest
	// permanently redirected to the staging buffer.
	defer t.endApplyWindow()

	summary := t.runBatchCore()

	t.emit(StoryEvent[T]{
		Kind:         EventBatchComplete,
		At:           time.Now(),
		BatchSummary: summary,
	})
}

// beginApplyWindow redirects Ingest into the staging buffer and publishes the
// story snapshot that Draft lookups answer from while the write transaction
// holds the store.
func (t *Tracker[T]) beginApplyWindow(stories map[uuid.UUID]storyRecord) {
	t.publishDraftSnapshot(stories)
	t.applyInProgress.Store(true)
}

// endApplyWindow reopens the direct ingest path and flushes whatever was
// staged. The snapshot outlives the flag until the drain completes, so an
// Ingest that observed the flag just before it cleared still gets an answer.
// It is idempotent.
func (t *Tracker[T]) endApplyWindow() {
	t.applyInProgress.Store(false)
	t.drainBuffer()
	t.clearDraftSnapshot()
}

// runBatchCore performs collection, sampling, HDBSCAN clustering, cluster
// mapping, and the apply transaction. It returns a summary of the run; on
// internal failure it returns an all-zero summary so the lifecycle continues.
func (t *Tracker[T]) runBatchCore() *BatchSummary {
	now := time.Now()

	var (
		signals []batchSignal
		stories map[uuid.UUID]storyRecord
		evict   []uuid.UUID
	)
	err := t.cfg.Store.View(func(tx Tx) error {
		var err error
		signals, stories, evict, err = t.collectBatch(tx, now)
		return err
	})
	if err != nil {
		t.reportBatchError(fmt.Errorf("story: batch collect: %w", err))
		return &BatchSummary{}
	}

	// Only the write transaction needs the ingest path redirected: collection
	// is read-only, and every maintenance decision is computed inside the
	// write transaction from state already in hand.
	t.beginApplyWindow(stories)
	summary, events, err := t.applyMaintenance(signals, stories, evict, now)
	t.endApplyWindow()
	if err != nil {
		t.reportBatchError(fmt.Errorf("story: batch apply: %w", err))
		return &BatchSummary{}
	}

	for _, ev := range events {
		t.emit(ev)
	}
	return summary
}

// reportBatchError hands err to Config.OnBatchError when one is configured.
// A batch failure is otherwise invisible: the run returns an empty summary and
// the store is left untouched until the next tick.
func (t *Tracker[T]) reportBatchError(err error) {
	if t.cfg.OnBatchError != nil {
		t.cfg.OnBatchError(err)
	}
}

// drainBuffer ingests any signals that arrived while applyInProgress was set.
// It must be called after applyInProgress is cleared so the signals reach the
// store instead of being re-buffered.
func (t *Tracker[T]) drainBuffer() {
	for {
		select {
		case sig := <-t.ingestBuffer:
			if _, err := t.Ingest(context.Background(), sig); err != nil {
				t.reportBatchError(fmt.Errorf("story: drain ingest buffer: %w", err))
			}
		default:
			return
		}
	}
}

// collectBatch gathers every signal from each Active and Dormant story, plus
// retained outliers, and returns a snapshot of the story records and the
// outlier IDs to be evicted. Eviction is not performed here: the IDs are
// returned so the apply transaction can delete them.
//
// Membership is read in full rather than windowed because the lifetime
// centroid is the mean of every member; reading a window would compute a
// centre that slides as old signals age out.
func (t *Tracker[T]) collectBatch(tx Tx, now time.Time) ([]batchSignal, map[uuid.UUID]storyRecord, []uuid.UUID, error) {
	var (
		signals []batchSignal
		stories = make(map[uuid.UUID]storyRecord)
		evict   []uuid.UUID
	)

	err := tx.ScanPrefix([]byte("s:"), func(key, val []byte) error {
		id, ok := parseStoryMetaKey(key)
		if !ok {
			return nil
		}
		var rec storyRecord
		if err := json.Unmarshal(val, &rec); err != nil {
			return nil
		}
		if rec.State == StoryStateArchived {
			return nil
		}
		stories[id] = rec

		// Collect every member. The lifetime centroid is the mean of all of
		// them (spec 006 §2.7), so a window-scoped read would compute a
		// different, drifting centre.
		return tx.ScanPrefix(keySignalPrefix(id), func(sigKey, sigVal []byte) error {
			sig, err := t.cfg.Codec.Decode(sigVal)
			if err != nil {
				return nil
			}
			signals = append(signals, batchSignal{
				id:      sig.ID,
				at:      sig.At,
				emb:     sig.Embedding,
				storyID: id,
			})
			return nil
		})
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// Outliers: collect those within lastBatch − OutlierTTL and flag the rest
	// for eviction. The reference is lastBatch, not wall-clock now, so
	// maintenance pauses do not cause mass eviction.
	err = tx.ScanPrefix([]byte("o:"), func(key, val []byte) error {
		sig, err := t.cfg.Codec.Decode(val)
		if err != nil {
			return nil
		}
		if sig.At.Before(t.lastBatch.Add(-t.cfg.OutlierTTL)) {
			evict = append(evict, sig.ID)
			return nil
		}
		signals = append(signals, batchSignal{
			id:      sig.ID,
			at:      sig.At,
			emb:     sig.Embedding,
			storyID: uuid.Nil,
			outlier: true,
		})
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return signals, stories, evict, nil
}

// sampleGroup is a group of signal indices for stratified sampling.

// emaAccum collects per-signal centroid distances of Active stories for the
// σ_global EMA update.
type emaAccum struct {
	sum   float64
	count int
}

func (e *emaAccum) add(d float64) {
	e.sum += d
	e.count++
}

// storyStats computes the centroid, radius, mean distance, and population
// standard deviation of the distances for a set of window signals, along with

// moveSignal moves a stored value from one key to another and keeps the
// signal's location-index entry pointing at the destination, so re-ingestion
// of a batch-moved signal finds its new home and never duplicates it.
func moveSignal(tx Tx, from, to []byte) error {
	val, err := tx.Get(from)
	if err != nil {
		return err
	}
	if val == nil {
		return nil
	}
	if err := tx.Put(to, val); err != nil {
		return err
	}
	if err := tx.Delete(from); err != nil {
		return err
	}
	sigID, ok := parseSignalIDFromLocKey(to)
	if !ok {
		return nil
	}
	if isOutlierKey(to) {
		return writeSignalLoc(tx, sigID, uuid.Nil, true)
	}
	storyID, ok := parseStoryIDFromSignalKey(to)
	if !ok {
		return fmt.Errorf("move signal: cannot parse destination story from %q", to)
	}
	return writeSignalLoc(tx, sigID, storyID, false)
}

// parseSignalKey extracts the signal ID from a "s:{storyID}:s:{signalID}" key
// given the key's prefix.
func parseSignalKey(key, prefix []byte) (uuid.UUID, bool) {
	if len(key) <= len(prefix) {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(string(key[len(prefix):]))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// jaccardIndex returns the Jaccard similarity between two index sets, 0 if
// either is empty.
