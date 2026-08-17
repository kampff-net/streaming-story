package story

// The in-memory form of a collected signal, and the views the geometry and
// clustering packages take of it. Every conversion lives here, so no algorithm
// has to know how a signal is stored and no caller has to build the view itself.

import (
	"time"

	"github.com/google/uuid"

	"go.kvsh.ch/streaming-story/internal/cluster"
	"go.kvsh.ch/streaming-story/internal/geom"
)

// batchSignal is one signal collected for a batch run. Its embedding is in
// centred space from the moment collection finishes; the stored copy stays raw.
type batchSignal struct {
	id      uuid.UUID
	at      time.Time
	emb     []float32
	storyID uuid.UUID // current story assignment; uuid.Nil for outlier signals
	outlier bool      // stored under the o: prefix
}

// projector returns the geometry currently in force. It is copied out under the
// lock and used without one, so a decision cannot see the mean change mid-flight.
func (t *Tracker[T]) projector() geom.Projector {
	t.calibMu.RLock()
	defer t.calibMu.RUnlock()
	return geom.Projector{Mean: t.mean, Strength: float32(t.cfg.MeanRemoval)}
}

// projectAll centres a batch's collected signals in place, so every downstream
// decision in that run is computed in one consistent geometry.
func projectAll(p geom.Projector, signals []batchSignal) {
	if len(p.Mean) == 0 || p.Strength == 0 {
		return
	}
	for i := range signals {
		signals[i].emb = p.Project(signals[i].emb)
	}
}

// corpusMeanOf returns the mean direction of a batch's collected signals, which
// becomes the geometry every distance in that run is measured against.
func corpusMeanOf(signals []batchSignal) []float32 {
	vecs := make([][]float32, len(signals))
	for i := range signals {
		vecs[i] = signals[i].emb
	}
	return geom.Mean(vecs)
}

// clusterParams packages the configured thresholds for the clustering decisions.
func (t *Tracker[T]) clusterParams() cluster.Params {
	return cluster.Params{
		Assign:  t.cfg.AssignThreshold,
		Merge:   t.cfg.MergeThreshold,
		Split:   t.cfg.SplitThreshold,
		MinSize: t.cfg.MinStorySize,
	}
}

// clusterPoints is the view the clustering decisions take: identity, timestamp,
// and vector, with the storage details left behind.
func clusterPoints(group []*batchSignal) []cluster.Point {
	out := make([]cluster.Point, len(group))
	for i, m := range group {
		out[i] = cluster.Point{ID: m.id, At: m.at, Vec: m.emb}
	}
	return out
}

// clusterMembers is the same view for a whole membership map.
func clusterMembers(members map[uuid.UUID][]*batchSignal) map[uuid.UUID][]cluster.Point {
	out := make(map[uuid.UUID][]cluster.Point, len(members))
	for id, group := range members {
		out[id] = clusterPoints(group)
	}
	return out
}

// pickSignals maps indices returned by a clustering decision back to the signals
// they refer to.
func pickSignals(group []*batchSignal, idx []int) []*batchSignal {
	out := make([]*batchSignal, len(idx))
	for i, j := range idx {
		out[i] = group[j]
	}
	return out
}

// embeddingsOf is the view the geometry helpers take.
func embeddingsOf(group []*batchSignal) [][]float32 {
	out := make([][]float32, len(group))
	for i, m := range group {
		out[i] = m.emb
	}
	return out
}

// timesOf pairs with embeddingsOf so a measurement can also report the group's
// latest signal time.
func timesOf(group []*batchSignal) []time.Time {
	out := make([]time.Time, len(group))
	for i, m := range group {
		out[i] = m.at
	}
	return out
}

// centroidOf returns the unweighted mean of the members' embeddings.
func centroidOf(group []*batchSignal) []float32 {
	return geom.Centroid(embeddingsOf(group))
}

// radiusOf returns the greatest distance from the group's centroid, which is the
// quantity the split gate tests.
func radiusOf(group []*batchSignal) float64 {
	return geom.Radius(embeddingsOf(group))
}

// measure summarises a group's geometry: centroid, radius, mean distance, sigma,
// and latest signal time.
func measure(group []*batchSignal) geom.Stats {
	return geom.Measure(embeddingsOf(group), timesOf(group))
}

// recentCentroidOf returns the mean of members at or after cutoff, falling back
// to the supplied lifetime centroid when none qualify.
func recentCentroidOf(group []*batchSignal, cutoff time.Time, fallback []float32) []float32 {
	recent := make([]*batchSignal, 0, len(group))
	for _, m := range group {
		if !m.at.Before(cutoff) {
			recent = append(recent, m)
		}
	}
	if len(recent) == 0 {
		return fallback
	}
	return centroidOf(recent)
}
