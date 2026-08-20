package story

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/keys"
)

type activeStoryIndex struct {
	dim     int
	ids     []uuid.UUID
	recents []float32 // story i occupies [i*dim:(i+1)*dim], unit normalized
	mu      sync.RWMutex
	metas   []activeStoryMeta
}

type activeStoryMeta struct {
	state              StoryState
	createdAt          time.Time
	lastSignalAt       time.Time
	reactivatedAt      time.Time
	statsAt            time.Time
	radius             float64
	meanDistance       float64
	sigma              float64
	signalCount        int
	frozenMeanDistance float64
	frozenSigma        float64
}

// buildActiveStoryIndex constructs an activeStoryIndex by scanning s: keys in the store.
func (t *Tracker[T]) buildActiveStoryIndex(tx Tx) (*activeStoryIndex, error) {
	records := make(map[uuid.UUID]storyRecord)
	hots := make(map[uuid.UUID]storyHot)

	err := tx.ScanPrefix([]byte("s:"), func(key, val []byte) error {
		if id, ok := keys.ParseStoryMeta(key); ok {
			var rec storyRecord
			if err := cborStrictDecMode.Unmarshal(val, &rec); err != nil {
				return fmt.Errorf("decode story record %s: %w", id, err)
			}
			records[id] = rec
			return nil
		}
		if id, ok := keys.ParseStoryHot(key); ok {
			var hot storyHot
			if err := cborStrictDecMode.Unmarshal(val, &hot); err != nil {
				return fmt.Errorf("decode story hot %s: %w", id, err)
			}
			hots[id] = hot
			return nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	dim := int(t.dim.Load())
	if dim == 0 {
		for _, rec := range records {
			if len(rec.Centroid) > 0 {
				dim = len(rec.Centroid)
				t.dim.Store(int32(dim))
				break
			}
		}
	}
	if dim == 0 {
		return &activeStoryIndex{}, nil
	}

	ids := make([]uuid.UUID, 0, len(records))
	for id, rec := range records {
		state := rec.State
		if hot, ok := hots[id]; ok {
			state = hot.State
		}
		if state == StoryStateArchived || len(rec.Centroid) == 0 {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	idx := &activeStoryIndex{
		dim:     dim,
		ids:     ids,
		recents: make([]float32, len(ids)*dim),
		metas:   make([]activeStoryMeta, len(ids)),
	}

	for i, id := range ids {
		rec := records[id]
		hot := hots[id]

		state := rec.State
		lastAt := rec.LastSignalAt
		reactAt := rec.ReactivatedAt

		if hot.State != 0 || !hot.LastSignalAt.IsZero() {
			state = hot.State
			if hot.LastSignalAt.After(lastAt) {
				lastAt = hot.LastSignalAt
			}
			if !hot.ReactivatedAt.IsZero() {
				reactAt = hot.ReactivatedAt
			}
		}

		recent := rec.RecentCentroid
		if len(recent) == 0 {
			recent = rec.Centroid
		}

		copy(idx.recents[i*dim:(i+1)*dim], recent)

		idx.metas[i] = activeStoryMeta{
			state:              state,
			createdAt:          rec.CreatedAt,
			lastSignalAt:       lastAt,
			reactivatedAt:      reactAt,
			statsAt:            rec.StatsAt,
			radius:             rec.Radius,
			meanDistance:       rec.MeanDistance,
			sigma:              rec.Sigma,
			signalCount:        rec.SignalCount,
			frozenMeanDistance: rec.FrozenMeanDistance,
			frozenSigma:        rec.FrozenSigma,
		}
	}

	return idx, nil
}

type nearestStory struct {
	id       uuid.UUID
	distance float64
	story    StoryMeta
}

// findNearestStories searches activeStoryIndex in memory for candidate stories
// that match the given projected embedding and admission threshold.
func (t *Tracker[T]) findNearestStories(emb Embedding, now time.Time) []nearestStory {
	idx := t.storyIndex.Load()
	if idx == nil || len(idx.ids) == 0 {
		return nil
	}

	cutoff := now.Add(-t.cfg.ActiveContextWindow)

	idx.mu.RLock()
	type cand struct {
		id     uuid.UUID
		offset int
		meta   activeStoryMeta
	}
	cands := make([]cand, 0, len(idx.ids))
	for i, id := range idx.ids {
		meta := idx.metas[i]
		if meta.state == StoryStateArchived {
			continue
		}
		if !meta.lastSignalAt.After(cutoff) {
			continue
		}
		cands = append(cands, cand{
			id:     id,
			offset: i * idx.dim,
			meta:   meta,
		})
	}
	idx.mu.RUnlock()

	var matches []nearestStory
	for _, c := range cands {
		vec := idx.recents[c.offset : c.offset+idx.dim]
		d := dist.CosineDistance(emb, vec)

		sm := StoryMeta{
			ID:                 c.id,
			State:              c.meta.state,
			Radius:             c.meta.radius,
			CreatedAt:          c.meta.createdAt,
			LastSignalAt:       c.meta.lastSignalAt,
			MeanDistance:       c.meta.meanDistance,
			Sigma:              c.meta.sigma,
			SignalCount:        c.meta.signalCount,
			FrozenMeanDistance: c.meta.frozenMeanDistance,
			FrozenSigma:        c.meta.frozenSigma,
			ReactivatedAt:      c.meta.reactivatedAt,
			StatsAt:            c.meta.statsAt,
		}
		thresh := t.calcThreshold(sm)
		if d <= thresh {
			matches = append(matches, nearestStory{
				id:       c.id,
				distance: d,
				story:    sm,
			})
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		if matches[i].distance != matches[j].distance {
			return matches[i].distance < matches[j].distance
		}
		return matches[i].id.String() < matches[j].id.String()
	})

	return matches
}

func (t *Tracker[T]) provisionalStory(emb Embedding, now time.Time) uuid.UUID {
	matches := t.findNearestStories(emb, now)
	if len(matches) > 0 {
		return matches[0].id
	}
	return uuid.Nil
}

// patchStoryIndex updates the in-memory metadata for a story after an Ingest commit.
func (t *Tracker[T]) patchStoryIndex(id uuid.UUID, lastAt time.Time, reactivated bool, reactAt time.Time) {
	idx := t.storyIndex.Load()
	if idx == nil {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for i, sid := range idx.ids {
		if sid == id {
			if lastAt.After(idx.metas[i].lastSignalAt) {
				idx.metas[i].lastSignalAt = lastAt
			}
			if reactivated {
				idx.metas[i].state = StoryStateActive
				idx.metas[i].reactivatedAt = reactAt
			}
			break
		}
	}
}
