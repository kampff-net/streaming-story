package story

// Explicit suppression: Tracker.Suppress and Tracker.Unsuppress. Unlike the
// batch and Draft paths, these write the persisted record directly and are
// not gated by applyInProgress, so both must patch storyIndex themselves
// (see patchStoryIndexState) to keep the Draft phase's in-memory snapshot
// from lagging until the next batch rebuild.

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Suppress marks story id as suppressed: excluded from
// Stories(StoryStateActive), but still absorbing matching signals in the
// Draft phase (see EventSuppressedStorySignal) rather than being pulled out
// of clustering entirely. reason is stored as SuppressionReason and retained
// even after Unsuppress.
//
// Emits EventStorySuppressed on a genuine Active/Dormant -> Suppressed
// transition. Calling Suppress on a story that is already suppressed updates
// SuppressionReason but does not re-emit the event. Returns an error
// wrapping ErrNotFound if id does not exist, and rejects Archived stories.
func (t *Tracker[T]) Suppress(id uuid.UUID, reason string) error {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()
	if t.closed.Load() {
		return fmt.Errorf("story: tracker is closed")
	}

	var transitioned bool
	err := t.cfg.Store.Update(func(tx Tx) error {
		rec, err := t.readStoryRecord(tx, id)
		if err != nil {
			return err
		}
		if rec.State == StoryStateArchived {
			return fmt.Errorf("story %s: cannot suppress an archived story", id)
		}
		transitioned = rec.State != StoryStateSuppressed

		oldLastAt := rec.LastSignalAt
		rec.State = StoryStateSuppressed
		rec.WasSuppressed = true
		rec.SuppressionReason = reason
		return t.writeStoryMeta(tx, id, oldLastAt, rec)
	})
	if err != nil {
		return err
	}

	t.patchStoryIndexState(id, StoryStateSuppressed)
	if transitioned {
		t.emit(StoryEvent[T]{Kind: EventStorySuppressed, StoryID: id, At: time.Now()})
	}
	return nil
}

// Unsuppress clears story id's suppression and marks it active.
// SuppressionReason and WasSuppressed are left untouched: they are the
// historical record that this story was, at some point, judged unwanted.
//
// Emits EventStoryUnsuppressed on a genuine Suppressed -> Active transition.
// Calling Unsuppress on a story that is not suppressed is a no-op. Returns an
// error wrapping ErrNotFound if id does not exist.
func (t *Tracker[T]) Unsuppress(id uuid.UUID) error {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()
	if t.closed.Load() {
		return fmt.Errorf("story: tracker is closed")
	}

	var transitioned bool
	err := t.cfg.Store.Update(func(tx Tx) error {
		rec, err := t.readStoryRecord(tx, id)
		if err != nil {
			return err
		}
		if rec.State != StoryStateSuppressed {
			transitioned = false
			return nil
		}
		transitioned = true

		oldLastAt := rec.LastSignalAt
		rec.State = StoryStateActive
		return t.writeStoryMeta(tx, id, oldLastAt, rec)
	})
	if err != nil {
		return err
	}

	if transitioned {
		t.patchStoryIndexState(id, StoryStateActive)
		t.emit(StoryEvent[T]{Kind: EventStoryUnsuppressed, StoryID: id, At: time.Now()})
	}
	return nil
}
