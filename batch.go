package story

// Batch plumbing: collection from the store, the apply window that redirects
// Ingest, the snapshot that answers Draft lookups while it is open, and the
// buffer drain. The decisions themselves are in maintain.go.

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/geom"
	"go.kvsh.ch/streaming-story/internal/keys"
)

// Batch plumbing: collection from the store, the apply window that redirects
// Ingest, and the buffer drain. The decisions themselves are in maintain.go.

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

// beginApplyWindow redirects Ingest into the staging buffer while the write
// transaction holds the store.
func (t *Tracker[T]) beginApplyWindow() {
	t.applyInProgress.Store(true)
}

// endApplyWindow reopens the direct ingest path and flushes whatever was staged.
func (t *Tracker[T]) endApplyWindow() {
	t.applyInProgress.Store(false)
	t.drainBuffer()
}

// runBatchCore performs collection, calibration, and the apply transaction.
func (t *Tracker[T]) runBatchCore() *BatchSummary {
	now := time.Now()

	var (
		signals           []batchFacet
		stories           map[uuid.UUID]storyRecord
		evict             []uuid.UUID
		mean              []float32
		droppedMismatches int
	)
	err := t.cfg.Store.View(func(tx Tx) error {
		var err error
		signals, stories, evict, mean, droppedMismatches, err = t.collectBatch(tx, now)
		return err
	})
	if err != nil {
		t.reportBatchError(fmt.Errorf("story: batch collect: %w", err))
		return &BatchSummary{}
	}

	// Establish this run's geometry before any distance is measured.
	// In-place projection pass over the collected facets:
	if len(mean) > 0 && t.cfg.MeanRemoval > 0 {
		strength := float32(t.cfg.MeanRemoval)
		for i := range signals {
			geom.ProjectInPlace(signals[i].emb, mean, strength)
		}
	}

	// Only the write transaction needs the ingest path redirected: collection
	// is read-only, and every maintenance decision is computed inside the
	// write transaction from state already in hand.
	t.beginApplyWindow()
	summary, events, err := t.applyMaintenance(signals, stories, evict, mean, now)
	t.endApplyWindow()
	if err != nil {
		t.reportBatchError(fmt.Errorf("story: batch apply: %w", err))
		return &BatchSummary{}
	}
	summary.DimensionMismatchesDropped = droppedMismatches

	// Rebuild and atomically swap activeStoryIndex with the updated store state
	var newIdx *activeStoryIndex
	_ = t.cfg.Store.View(func(tx Tx) error {
		var err error
		newIdx, err = t.buildActiveStoryIndex(tx)
		return err
	})
	if newIdx != nil {
		t.storyIndex.Store(newIdx)
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
func (t *Tracker[T]) collectBatch(tx Tx, now time.Time) ([]batchFacet, map[uuid.UUID]storyRecord, []uuid.UUID, []float32, int, error) {
	var (
		signals           []batchFacet
		stories           = make(map[uuid.UUID]storyRecord)
		evict             []uuid.UUID
		meanSum           []float32
		meanCount         int
		droppedMismatches int
	)
	dim := int(t.dim.Load())

	// A signal with several facets is named by several markers, so a batch
	// that decoded per marker decoded it once per facet. Cache what a
	// batchFacet actually needs and nothing else: holding the whole record
	// would keep every payload in the batch alive for the length of the
	// collect, and the payload is the bulk of it.
	type cachedSignal struct {
		at   time.Time
		embs []Embedding
	}
	sigCache := make(map[uuid.UUID]*cachedSignal)
	getSignal := func(sigID uuid.UUID) (*cachedSignal, bool, error) {
		if s, ok := sigCache[sigID]; ok {
			return s, s != nil, nil
		}
		at, embs, found, err := t.readSignalHeader(tx, sigID)
		if err != nil {
			return nil, false, err
		}
		if !found {
			sigCache[sigID] = nil
			return nil, false, nil
		}
		c := &cachedSignal{at: at, embs: embs}
		sigCache[sigID] = c
		return c, true, nil
	}

	err := tx.ScanPrefix([]byte("s:"), func(key, val []byte) error {
		id, ok := keys.ParseStoryMeta(key)
		if !ok {
			return nil
		}
		var rec storyRecord
		if err := cborStrictDecMode.Unmarshal(val, &rec); err != nil {
			return nil
		}
		if rec.State == StoryStateArchived {
			return nil
		}
		stories[id] = rec

		// Collect every member facet. The lifetime centroid is the mean of all
		// of them (spec 006 §2.7), so a window-scoped read would compute a
		// different, drifting centre. The markers carry no payload, so each
		// referenced signal is read from its canonical record.
		prefix := keys.FacetPrefix(id)
		return tx.ScanPrefix(prefix, func(key, _ []byte) error {
			sigID, facet, ok := keys.ParseFacetMember(key, prefix)
			if !ok {
				return nil
			}
			sig, found, err := getSignal(sigID)
			if err != nil {
				return err
			}
			if !found || facet >= len(sig.embs) {
				// A marker with no record behind it, or naming a facet the
				// record does not have. Skip rather than fail: the store
				// invariant test is what catches this, and a batch run must
				// not be stopped by one bad key.
				return nil
			}
			emb := sig.embs[facet]
			if dim > 0 && len(emb) != dim {
				droppedMismatches++
				return nil
			}
			if len(meanSum) == 0 {
				meanSum = make([]float32, len(emb))
			}
			for i, v := range emb {
				meanSum[i] += v
			}
			meanCount++
			signals = append(signals, batchFacet{
				id:      sigID,
				facet:   facet,
				at:      sig.at,
				emb:     emb,
				storyID: id,
			})
			return nil
		})
	})
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	// Outliers: collect those within lastBatch − OutlierTTL and flag the rest
	// for eviction. The reference is lastBatch, not wall-clock now, so
	// maintenance pauses do not cause mass eviction.
	err = tx.ScanPrefix(keys.OutlierPrefix(), func(key, _ []byte) error {
		sigID, facet, ok := keys.ParseOutlierFacet(key)
		if !ok {
			return nil
		}
		sig, found, err := getSignal(sigID)
		if err != nil {
			return err
		}
		if !found || facet >= len(sig.embs) {
			return nil
		}
		// The TTL is a property of the signal's timestamp, so every unplaced
		// facet of an aged signal is evicted in the same pass.
		if sig.at.Before(t.lastBatch.Add(-t.cfg.OutlierTTL)) {
			evict = append(evict, sigID)
			return nil
		}
		emb := sig.embs[facet]
		if dim > 0 && len(emb) != dim {
			droppedMismatches++
			return nil
		}
		if len(meanSum) == 0 {
			meanSum = make([]float32, len(emb))
		}
		for i, v := range emb {
			meanSum[i] += v
		}
		meanCount++
		signals = append(signals, batchFacet{
			id:      sigID,
			facet:   facet,
			at:      sig.at,
			emb:     emb,
			storyID: uuid.Nil,
			outlier: true,
		})
		return nil
	})
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	var mean []float32
	if meanCount > 0 {
		mean = make([]float32, len(meanSum))
		inv := 1.0 / float32(meanCount)
		for i, v := range meanSum {
			mean[i] = v * inv
		}
		mean = geom.Unit(mean)
	}

	return signals, stories, evict, mean, droppedMismatches, nil
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

// moveFacetToStory moves one facet into a story, from wherever it currently
// sits — another story, or the outlier bucket — and repoints the signal's
// location index, so re-ingestion of a batch-moved signal finds its new home
// and never duplicates it.
//
// from is uuid.Nil when the facet is coming out of the outlier bucket.
func moveFacetToStory(tx Tx, from, to, signalID uuid.UUID, facet int) error {
	if from == uuid.Nil {
		if err := dropFacetOutlier(tx, signalID, facet); err != nil {
			return err
		}
	} else if from != to {
		if err := unplaceFacet(tx, from, signalID, facet); err != nil {
			return err
		}
	}
	if err := placeFacet(tx, to, signalID, facet); err != nil {
		return err
	}
	return setFacetLoc(tx, signalID, facet, keys.FacetLoc{StoryID: to})
}

// setFacetLoc updates one facet's entry in a signal's location index, growing
// the index if the facet sits past its current end.
func setFacetLoc(tx Tx, signalID uuid.UUID, facet int, loc keys.FacetLoc) error {
	locs, _, err := readSignalLocSet(tx, signalID)
	if err != nil {
		return err
	}
	for len(locs) <= facet {
		locs = append(locs, keys.FacetLoc{})
	}
	locs[facet] = loc
	return writeSignalLocSet(tx, signalID, locs)
}

// migrateFacets moves every facet marker under from into to. Two facets of one
// signal converging on the same survivor cannot collide: the key carries the
// facet index.
func migrateFacets(tx Tx, from, to uuid.UUID) error {
	prefix := keys.FacetPrefix(from)
	type ref struct {
		signalID uuid.UUID
		facet    int
	}
	var refs []ref
	err := tx.ScanPrefix(prefix, func(key, _ []byte) error {
		sigID, facet, ok := keys.ParseFacetMember(key, prefix)
		if !ok {
			return nil
		}
		refs = append(refs, ref{sigID, facet})
		return nil
	})
	if err != nil {
		return err
	}
	for _, r := range refs {
		if err := moveFacetToStory(tx, from, to, r.signalID, r.facet); err != nil {
			return err
		}
	}
	return nil
}


