package story

import (
	"math"
	"time"

	"github.com/google/uuid"

	"go.kvsh.ch/streaming-story/internal/dist"
)

// draftSnapshot is an immutable copy of the story metadata a batch run
// collected, published for the duration of the Apply transaction.
//
// It exists because the Store contract does not promise that View may run
// concurrently with Update — single-lock backends (MemStore among them) would
// block a Draft lookup for the whole Apply, which is exactly the stall the
// ingest buffer exists to avoid. Answering from memory keeps Ingest wait-free
// while still using last-batch centroids, which is what the Draft phase reads
// at any other time.
type draftSnapshot struct {
	stories []snapshotStory
}

// snapshotStory is one story's Draft-relevant metadata.
type snapshotStory struct {
	meta StoryMeta
}

// newDraftSnapshot builds a snapshot from the story records a batch run has
// already read. Archived stories and stories with no centroid are omitted:
// neither can be a Draft anchor.
func newDraftSnapshot(stories map[uuid.UUID]storyRecord) *draftSnapshot {
	snap := &draftSnapshot{stories: make([]snapshotStory, 0, len(stories))}
	for id, rec := range stories {
		if rec.State == StoryStateArchived || len(rec.Centroid) == 0 {
			continue
		}
		snap.stories = append(snap.stories, snapshotStory{meta: storyMetaFromRecord(id, rec)})
	}
	return snap
}

// publishDraftSnapshot makes snap the source of Draft answers until it is
// cleared.
func (t *Tracker[T]) publishDraftSnapshot(stories map[uuid.UUID]storyRecord) {
	t.draftSnapshot.Store(newDraftSnapshot(stories))
}

// clearDraftSnapshot drops the snapshot so Draft lookups go back to the store.
func (t *Tracker[T]) clearDraftSnapshot() {
	t.draftSnapshot.Store(nil)
}

// provisionalStory returns the story a signal would be assigned to, computed
// against the published snapshot rather than the store. It returns uuid.Nil
// when no story is within its adaptive threshold, or when no snapshot is
// published.
//
// The answer is provisional in the same sense as any Draft assignment: the
// Apply transaction running concurrently may merge, retire, or re-shape the
// named story. The buffered signal is re-ingested for real once Apply
// commits, and that placement is the authoritative one.
func (t *Tracker[T]) provisionalStory(emb []float32, now time.Time) uuid.UUID {
	snap := t.draftSnapshot.Load()
	if snap == nil {
		return uuid.Nil
	}

	cutoff := now.Add(-t.cfg.ActiveContextWindow)
	best := uuid.Nil
	bestDist := math.MaxFloat64
	var bestMeta StoryMeta

	for _, s := range snap.stories {
		if s.meta.LastSignalAt.Before(cutoff) {
			continue
		}
		d := dist.CosineDistance(emb, s.meta.Centroid)
		if d < bestDist {
			bestDist, best, bestMeta = d, s.meta.ID, s.meta
		}
	}
	if best == uuid.Nil || bestDist > t.calcThreshold(bestMeta) {
		return uuid.Nil
	}
	return best
}
