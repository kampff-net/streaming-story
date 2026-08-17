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
		prefix := keys.SignalPrefix(storyID)
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
			key = keys.Outlier(id)
		} else {
			key = keys.Signal(storyID, id)
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

// errStopIteration terminates a range scan early; the caller checks for it
// with errors.Is to distinguish a requested stop from a real failure.
var errStopIteration = errors.New("stop iteration")
