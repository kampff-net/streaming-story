package story

import (
	"encoding/json"
	"errors"
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
				id, ok := keys.ParseStoryMeta(key)
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
		prefix := keys.FacetPrefix(storyID)
		_ = t.cfg.Store.View(func(tx Tx) error {
			// A signal contributing several facets to one story is still one
			// member, so it is yielded once. Facet keys of a signal are
			// contiguous and sorted, so remembering the last ID suffices.
			var last uuid.UUID
			seen := false
			return tx.ScanPrefix(prefix, func(key, _ []byte) error {
				sigID, _, ok := keys.ParseFacetMember(key, prefix)
				if !ok {
					return nil
				}
				if seen && sigID == last {
					return nil
				}
				last, seen = sigID, true

				sig, found, err := t.readCanonicalSignal(tx, sigID)
				if err != nil {
					if !yield(Signal[T]{}, err) {
						return errStopIteration
					}
					return nil
				}
				if !found {
					// A membership marker with no record behind it. The store
					// invariant forbids this; skip rather than yield a zero
					// signal that a caller would read as real.
					return nil
				}
				if !yield(sig, nil) {
					return errStopIteration
				}
				return nil
			})
		})
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
	var out []uuid.UUID
	err := t.cfg.Store.View(func(tx Tx) error {
		locs, _, err := readSignalLocSet(tx, signalID)
		if err != nil {
			return err
		}
		out = placedStories(locs)
		return nil
	})
	return out, err
}

// FacetsOfSignal returns one Placement per facet of signalID, in facet order,
// including facets still held as outliers. It is the detailed form of
// StoriesOf: it reports not just which stories claimed the signal but which of
// its facets each story claimed, and which facets nothing claimed at all.
func (t *Tracker[T]) FacetsOfSignal(signalID uuid.UUID) ([]Placement, error) {
	var out []Placement
	err := t.cfg.Store.View(func(tx Tx) error {
		locs, _, err := readSignalLocSet(tx, signalID)
		if err != nil {
			return err
		}
		out = make([]Placement, len(locs))
		for facet, loc := range locs {
			out[facet] = Placement{SignalID: signalID, Facet: facet, StoryID: loc.StoryID}
		}
		return nil
	})
	return out, err
}

// FacetsOfStory returns an iterator over every facet the story holds, ordered
// by (signal, facet). A signal contributing two facets appears twice — which is
// the point: this is the view that shows a story's true geometric membership,
// the same multiset the centroid and radius are computed over.
func (t *Tracker[T]) FacetsOfStory(storyID uuid.UUID) iter.Seq2[Placement, error] {
	return func(yield func(Placement, error) bool) {
		prefix := keys.FacetPrefix(storyID)
		_ = t.cfg.Store.View(func(tx Tx) error {
			return tx.ScanPrefix(prefix, func(key, _ []byte) error {
				sigID, facet, ok := keys.ParseFacetMember(key, prefix)
				if !ok {
					return nil
				}
				if !yield(Placement{SignalID: sigID, Facet: facet, StoryID: storyID}, nil) {
					return errStopIteration
				}
				return nil
			})
		})
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
	var sig Signal[T]
	err := t.cfg.Store.View(func(tx Tx) error {
		s, found, err := t.readCanonicalSignal(tx, id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("signal %s: %w", id, ErrNotFound)
		}
		sig = s
		return nil
	})
	return sig, err
}

// Signals returns an iterator over every signal in the store, in signal-ID
// order, independently of where — or whether — its facets are placed. Members,
// partially placed signals, and signals whose every facet is still an outlier
// all appear, and a signal appears once regardless of facet count.
//
// The yielded value is complete: ID, At, Embeddings, and Data are the values
// that were ingested. Signals is therefore a lossless dump, and replaying it
// through Ingest against a fresh store is a full rebuild that needs no access
// to the original source and no re-embedding.
func (t *Tracker[T]) Signals() iter.Seq2[Signal[T], error] {
	return func(yield func(Signal[T], error) bool) {
		_ = t.cfg.Store.View(func(tx Tx) error {
			return tx.ScanPrefix(keys.CanonicalPrefix(), func(key, val []byte) error {
				if _, ok := keys.ParseCanonicalSignal(key); !ok {
					return nil
				}
				sig, err := t.cfg.Codec.Decode(val)
				if err != nil {
					if !yield(Signal[T]{}, err) {
						return errStopIteration
					}
					return nil
				}
				if !yield(sig, nil) {
					return errStopIteration
				}
				return nil
			})
		})
	}
}

// errStopIteration terminates a range scan early; the caller checks for it
// with errors.Is to distinguish a requested stop from a real failure.
var errStopIteration = errors.New("stop iteration")
