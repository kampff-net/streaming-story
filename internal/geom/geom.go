// Package geom is the vector geometry the tracker measures in: the corpus mean
// it centres against, and the summary statistics of a group of embeddings.
//
// # Why anything is subtracted
//
// Text embeddings are anisotropic: every vector carries a large component along
// one shared direction, so the whole corpus occupies a narrow cone rather than
// the sphere. Two consequences, both fatal to centroid-based clustering:
//
//  1. The mean of a large group converges on that shared direction, so two
//     groups that share nothing still have near-identical centroids. Measured on
//     a 596-signal news corpus: two halves whose closest members sat 0.84 apart
//     had centroids 0.06 apart. Every centroid-distance test — a split test above
//     all — reads "identical" and never fires.
//
//  2. Because every centroid looks like every signal, a group that grows by
//     admitting whatever is nearest its centroid snowballs. Same corpus, radius
//     0.50 of the median pairwise distance: raw geometry produced a 229-signal
//     group, centred geometry a 25-signal one.
//
// Subtracting the corpus mean removes that shared component and both effects
// with it. Removing further principal components was measured and rejected: the
// separation between a signal's neighbourhood and the corpus background peaks at
// mean-centring alone and degrades monotonically as components two onward are
// stripped. Those carry topic signal, not anisotropy.
//
// Distances are therefore measured in centred space, and every threshold the
// caller configures is a centred-space distance. That scale is roughly twice raw
// cosine — the reference corpus has a median pairwise distance of 1.02 centred
// against 0.45 raw.
package geom

import (
	"math"
	"time"

	"go.kvsh.ch/streaming-story/internal/dist"
)

// Projector centres embeddings against a fixed mean direction. A zero-value
// Projector is the identity, which is the geometry in force before the first
// batch run measures a mean.
//
// A Projector is a value, so a caller copies one out from under its lock and
// uses it without one: a run's geometry cannot shift underneath it mid-decision.
type Projector struct {
	Mean []float32

	// Strength is the fraction of the mean actually subtracted.
	//
	// It is deliberately below 1. Full removal is degenerate when the corpus is
	// itself one tight group: the mean then sits on top of every signal, the
	// residuals are whatever noise is left, and a single coherent group shatters
	// into antipodal halves. Keeping a fraction of the mean leaves every residual
	// a shared component to agree on, which costs little separation on a diverse
	// corpus and keeps a narrow one intact.
	Strength float32
}

// ProjectInPlace centres v against mean and renormalizes it to unit length,
// without allocating. v must be unit on entry and is unit on return.
func ProjectInPlace(v, mean []float32, strength float32) {
	if len(v) != len(mean) || len(v) == 0 || strength == 0 {
		return
	}
	var sum float64
	for i := range v {
		v[i] -= strength * mean[i]
		sum += float64(v[i]) * float64(v[i])
	}
	if sum == 0 {
		return
	}
	inv := float32(1.0 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}

// Project returns emb as a unit vector with Strength × Mean subtracted and
// renormalized to unit length.
func (p Projector) Project(emb []float32) []float32 {
	if len(p.Mean) != len(emb) || len(emb) == 0 || p.Strength == 0 {
		// No mean established yet, or a dimensionality mismatch the caller
		// rejects on its own. Either way, leave the geometry alone.
		return emb
	}
	out := Unit(emb)
	ProjectInPlace(out, p.Mean, p.Strength)
	return out
}

// Unit returns a copy of emb scaled to unit length. A zero vector is returned as
// a zero copy: it has no direction to preserve.
func Unit(emb []float32) []float32 {
	out := make([]float32, len(emb))
	copy(out, emb)
	var sum float64
	for _, v := range emb {
		sum += float64(v) * float64(v)
	}
	if sum == 0 {
		return out
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range out {
		out[i] *= inv
	}
	return out
}

// Mean returns the mean of the vectors' unit-normalized directions, or nil when
// there are none. Vectors whose length differs from the first are skipped.
//
// Callers recompute this from full membership on every run rather than updating
// it incrementally. An EMA would make the geometry depend on how many runs had
// happened, so a re-run over unchanged data would shift every distance slightly
// and flip membership on groups sitting near a threshold.
func Mean(vecs [][]float32) []float32 {
	if len(vecs) == 0 {
		return nil
	}
	mean := make([]float32, len(vecs[0]))
	var n float32
	for _, v := range vecs {
		if len(v) != len(mean) {
			continue
		}
		u := Unit(v)
		for d, x := range u {
			mean[d] += x
		}
		n++
	}
	if n == 0 {
		return nil
	}
	for d := range mean {
		mean[d] /= n
	}
	return mean
}

// Centroid returns the unit-normalized mean direction of the vectors.
func Centroid(vecs [][]float32) []float32 {
	if len(vecs) == 0 {
		return nil
	}
	c := make([]float32, len(vecs[0]))
	for _, v := range vecs {
		for d, x := range v {
			c[d] += x
		}
	}
	return Unit(c)
}

// Radius returns the greatest distance from the group's centroid to a member.
func Radius(vecs [][]float32) float64 {
	c := Centroid(vecs)
	var r float64
	for _, v := range vecs {
		if d := dist.CosineDistanceUnit(v, c); d > r {
			r = d
		}
	}
	return r
}

// Stats is the geometry of a group, computed from its members alone so that two
// stores holding the same vectors derive the same numbers.
type Stats struct {
	Centroid []float32
	Radius   float64
	Mean     float64 // mean member distance to Centroid
	Sigma    float64 // population standard deviation of those distances
	Dists    []float64
	LatestAt time.Time
}

// Measure summarises a group against its own centroid. Times are optional: pass
// nil to skip LatestAt, otherwise one timestamp per vector, in the same order.
func Measure(vecs [][]float32, times []time.Time) Stats {
	if len(vecs) == 0 {
		return Stats{}
	}
	st := Stats{
		Centroid: Centroid(vecs),
		Dists:    make([]float64, 0, len(vecs)),
	}
	var sum, sumSq float64
	for i, v := range vecs {
		d := dist.CosineDistanceUnit(v, st.Centroid)
		st.Dists = append(st.Dists, d)
		sum += d
		sumSq += d * d
		if d > st.Radius {
			st.Radius = d
		}
		if times != nil && times[i].After(st.LatestAt) {
			st.LatestAt = times[i]
		}
	}
	n := float64(len(vecs))
	st.Mean = sum / n
	if variance := sumSq/n - st.Mean*st.Mean; variance > 0 {
		st.Sigma = math.Sqrt(variance)
	}
	return st
}

// MaxAngularSeparation returns the largest cosine distance possible between two
// directions that both lie within cosine distance r of a common centre. It is
// 1-cos(2*arccos(1-r)), expanded to avoid the trigonometry.
//
// The bound is not the Euclidean 2r. Cosine distance is not a metric and 1-cos
// grows quadratically in the angle, so a pair each at distance r from the centre
// can be far further apart than 2r: at r = 0.122 this returns 0.458 against the
// 0.245 a Euclidean bound predicts. Using 2r as a split pre-filter would skip
// groups that genuinely split.
func MaxAngularSeparation(r float64) float64 {
	if r <= 0 {
		return 0
	}
	if r >= 1 {
		return 2
	}
	return 4*r - 2*r*r
}
