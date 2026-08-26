package story

import (
	"fmt"
	"iter"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/keys"
)

// The read API: stories, signals, and derived signal IDs.

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
	return t.readStoryMeta(t.cfg.Store, id)
}

// Stories returns an iterator over stories in the given state.
// Pass StoryStateAny to iterate all stories.
//
// A store failure is yielded once, with a zero StoryMeta, and ends the
// iteration. A story record that fails to decode is yielded as its own error
// and iteration continues, so one corrupt record does not hide the rest.
func (t *Tracker[T]) Stories(state StoryState) iter.Seq2[StoryMeta, error] {
	return func(yield func(StoryMeta, error) bool) {
		// StoryMeta is small and fixed-size, so the whole selection is
		// materialised before anything is yielded. See scanDistinctIDs for why
		// nothing is yielded from inside a scan. A per-record decode failure
		// rides along in order rather than being collected separately, so the
		// caller sees errors where the bad records actually sat.
		type entry struct {
			meta StoryMeta
			err  error
		}
		var entries []entry

		err := t.cfg.Store.ScanPrefix([]byte("s:"), func(key, val []byte) error {
			id, ok := keys.ParseStoryMeta(key)
			if !ok {
				return nil
			}
			var rec storyRecord
			if err := cborStrictDecMode.Unmarshal(val, &rec); err != nil {
				entries = append(entries, entry{err: fmt.Errorf("decode story record %s: %w", id, err)})
				return nil
			}
			if state != StoryStateAny && rec.State != state {
				return nil
			}
			entries = append(entries, entry{meta: storyMetaFromRecord(id, rec)})
			return nil
		})
		if err != nil {
			yield(StoryMeta{}, err)
			return
		}

		for _, e := range entries {
			if !yield(e.meta, e.err) {
				return
			}
		}
	}
}

// SignalsOf returns an iterator over all signals belonging to storyID.
// Signal data is retained through archival, so Archived stories are
// fully iterable.
func (t *Tracker[T]) SignalsOf(storyID uuid.UUID) iter.Seq2[Signal[T], error] {
	return func(yield func(Signal[T], error) bool) {
		prefix := keys.FacetPrefix(storyID)
		// A signal contributing several facets to one story is still one
		// member, so it is yielded once.
		ids, err := scanDistinctIDs(t.cfg.Store, prefix, func(key []byte) (uuid.UUID, bool) {
			id, _, ok := keys.ParseFacetMember(key, prefix)
			return id, ok
		})
		if err != nil {
			yield(Signal[T]{}, err)
			return
		}
		t.yieldSignals(ids, yield)
	}
}

// Placement is one facet's membership: the atom of the many-to-many relation
// between signals and stories. StoryID is uuid.Nil for a facet still held in
// the outlier bucket.
type Placement struct {
	SignalID uuid.UUID
	Facet    int
	StoryID  uuid.UUID
}

// StoriesOf returns the stories signalID currently has at least one facet in,
// sorted and de-duplicated. An empty slice means every facet is an outlier or
// the signal is unknown; use Signal to distinguish the two.
func (t *Tracker[T]) StoriesOf(signalID uuid.UUID) ([]uuid.UUID, error) {
	locs, _, err := readSignalLocSet(t.cfg.Store, signalID)
	if err != nil {
		return nil, err
	}
	return placedStories(locs), nil
}

// FacetsOfSignal returns one Placement per facet of signalID, in facet order,
// including facets still held as outliers. It is the detailed form of
// StoriesOf: it reports not just which stories claimed the signal but which of
// its facets each story claimed, and which facets nothing claimed at all.
func (t *Tracker[T]) FacetsOfSignal(signalID uuid.UUID) ([]Placement, error) {
	locs, _, err := readSignalLocSet(t.cfg.Store, signalID)
	if err != nil {
		return nil, err
	}
	out := make([]Placement, len(locs))
	for facet, loc := range locs {
		out[facet] = Placement{SignalID: signalID, Facet: facet, StoryID: loc.StoryID}
	}
	return out, nil
}

// FacetsOfStory returns an iterator over every facet the story holds, ordered
// by (signal, facet). A signal contributing two facets appears twice — which is
// the point: this is the view that shows a story's true geometric membership,
// the same multiset the centroid and radius are computed over.
func (t *Tracker[T]) FacetsOfStory(storyID uuid.UUID) iter.Seq2[Placement, error] {
	return func(yield func(Placement, error) bool) {
		prefix := keys.FacetPrefix(storyID)
		// A Placement is three small fields, so the story's whole membership is
		// materialised rather than read back one key at a time.
		var out []Placement
		err := t.cfg.Store.ScanPrefix(prefix, func(key, _ []byte) error {
			sigID, facet, ok := keys.ParseFacetMember(key, prefix)
			if !ok {
				return nil
			}
			out = append(out, Placement{SignalID: sigID, Facet: facet, StoryID: storyID})
			return nil
		})
		if err != nil {
			yield(Placement{}, err)
			return
		}

		for _, p := range out {
			if !yield(p, nil) {
				return
			}
		}
	}
}

// Signal returns the signal with the given ID, read from its canonical record.
// It does not consult the location index and does not need to: the record
// exists independently of where — or whether — the signal's facets are placed.
//
// It returns an error wrapping ErrNotFound when no canonical record exists. A
// signal whose every facet was evicted from the outlier bucket is therefore not
// found, which is the intended behavior; a signal merely held as an outlier
// still is.
//
// Callers that need to know which stories hold the signal should use StoriesOf,
// or FacetsOfSignal for the per-facet detail.
func (t *Tracker[T]) Signal(id uuid.UUID) (Signal[T], error) {
	sig, found, err := t.readCanonicalSignal(t.cfg.Store, id)
	if err != nil {
		return Signal[T]{}, err
	}
	if !found {
		return Signal[T]{}, fmt.Errorf("signal %s: %w", id, ErrNotFound)
	}
	return sig, nil
}

// Signals returns an iterator over every signal in the store, in signal-ID
// order, independently of where — or whether — its facets are placed. Members,
// partially placed signals, and signals whose every facet is still an outlier
// all appear, and a signal appears once regardless of facet count.
//
// The yielded value is complete: ID, At, and Data are preserved, and
// Embeddings are returned as unit-normalized vectors. Signals is lossless for
// replay: replaying it through Ingest against a fresh store reproduces the
// exact clustering without requiring original embedding sources.
func (t *Tracker[T]) Signals() iter.Seq2[Signal[T], error] {
	return func(yield func(Signal[T], error) bool) {
		ids, err := scanDistinctIDs(t.cfg.Store, keys.CanonicalPrefix(), keys.ParseCanonicalSignal)
		if err != nil {
			yield(Signal[T]{}, err)
			return
		}
		t.yieldSignals(ids, yield)
	}
}

// Outliers returns an iterator over signals that have at least one unplaced facet.
// A signal with several unplaced facets is yielded once. Signals whose canonical record
// is missing or undecodable are skipped so one bad entry does not stop iteration.
func (t *Tracker[T]) Outliers() iter.Seq2[Signal[T], error] {
	return func(yield func(Signal[T], error) bool) {
		ids, err := scanDistinctIDs(t.cfg.Store, keys.OutlierPrefix(), func(key []byte) (uuid.UUID, bool) {
			id, _, ok := keys.ParseOutlierFacet(key)
			return id, ok
		})
		if err != nil {
			yield(Signal[T]{}, err)
			return
		}
		t.yieldSignals(ids, yield)
	}
}

// scanDistinctIDs collects the distinct IDs under prefix into a slice.
//
// It exists so that no public iterator yields from inside a scan. A store may
// hold a read transaction open for the duration of a scan, and caller code
// reached through yield is free to write back to the Tracker — which blocks on
// that very read on bbolt, and on MemStore's non-reentrant RWMutex. Collecting
// the IDs first costs 16 bytes each and lets every yield run with the store
// released.
//
// Keys of one ID are contiguous and sorted, so remembering the last one is
// enough to de-duplicate a signal that contributes several facets.
func scanDistinctIDs(r Reader, prefix []byte, parse func(key []byte) (uuid.UUID, bool)) ([]uuid.UUID, error) {
	var out []uuid.UUID
	var last uuid.UUID
	seen := false

	err := r.ScanPrefix(prefix, func(key, _ []byte) error {
		id, ok := parse(key)
		if !ok {
			return nil
		}
		if seen && id == last {
			return nil
		}
		last, seen = id, true
		out = append(out, id)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// yieldSignals reads each signal by ID and yields it, one standalone read at a
// time, so the store is never held while yield runs. This is why the ID list is
// materialised first rather than the signals themselves: a Signal carries its
// embeddings, and a whole corpus of them will not fit in memory.
//
// A signal whose canonical record is gone by the time it is read is skipped.
// The record may have been evicted since the scan, or — the case that always
// had to be handled — a membership marker may point at nothing. Either way a
// zero signal the caller would read as real is worse than an omission.
func (t *Tracker[T]) yieldSignals(ids []uuid.UUID, yield func(Signal[T], error) bool) {
	for _, id := range ids {
		sig, found, err := t.readCanonicalSignal(t.cfg.Store, id)
		if err != nil {
			if !yield(Signal[T]{}, err) {
				return
			}
			continue
		}
		if !found {
			continue
		}
		if !yield(sig, nil) {
			return
		}
	}
}
