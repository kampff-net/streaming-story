package geom

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kvsh.ch/streaming-story/internal/dist"
)

// arc returns n unit vectors spaced evenly from center, optionally offset by a
// shared component large enough to push them all into a narrow cone.
func arc(center float64, n int, shared float32) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		s, c := math.Sincos(center + 0.02*float64(i))
		out[i] = []float32{shared + float32(c), float32(s)}
	}
	return out
}

func TestUnit(t *testing.T) {
	t.Run("scales_to_unit_length", func(t *testing.T) {
		got := Unit([]float32{3, 4})
		assert.InDelta(t, 0.6, got[0], 1e-6)
		assert.InDelta(t, 0.8, got[1], 1e-6)
	})

	t.Run("leaves_input_untouched", func(t *testing.T) {
		in := []float32{3, 4}
		_ = Unit(in)
		assert.Equal(t, []float32{3, 4}, in)
	})

	t.Run("zero_vector_has_no_direction", func(t *testing.T) {
		assert.Equal(t, []float32{0, 0}, Unit([]float32{0, 0}))
	})
}

func TestMean(t *testing.T) {
	t.Run("averages_directions_not_magnitudes", func(t *testing.T) {
		// Same two directions, wildly different lengths. The mean must not
		// follow the longer vector.
		mean := Mean([][]float32{{100, 0}, {0, 1}})
		require.Len(t, mean, 2)
		assert.InDelta(t, 0.5, mean[0], 1e-6)
		assert.InDelta(t, 0.5, mean[1], 1e-6)
	})

	t.Run("empty_has_no_mean", func(t *testing.T) {
		assert.Nil(t, Mean(nil))
	})

	t.Run("mismatched_dimensions_are_skipped", func(t *testing.T) {
		mean := Mean([][]float32{{1, 0}, {1, 2, 3}})
		require.Len(t, mean, 2)
		assert.InDelta(t, 1.0, mean[0], 1e-6)
	})
}

func TestProjector(t *testing.T) {
	t.Run("zero_value_is_identity", func(t *testing.T) {
		emb := []float32{3, 4}
		assert.Equal(t, emb, Projector{}.Project(emb))
	})

	t.Run("zero_strength_is_identity", func(t *testing.T) {
		emb := []float32{3, 4}
		assert.Equal(t, emb, Projector{Mean: []float32{1, 0}}.Project(emb))
	})

	t.Run("dimension_mismatch_is_identity", func(t *testing.T) {
		emb := []float32{3, 4, 5}
		p := Projector{Mean: []float32{1, 0}, Strength: 1}
		assert.Equal(t, emb, p.Project(emb))
	})

	t.Run("subtracts_the_scaled_mean_of_a_unit_input", func(t *testing.T) {
		p := Projector{Mean: []float32{1, 0}, Strength: 0.5}
		got := p.Project([]float32{10, 0}) // unit-normalizes to {1, 0}
		assert.InDelta(t, 0.5, got[0], 1e-6)
		assert.InDelta(t, 0.0, got[1], 1e-6)
	})

	t.Run("leaves_the_caller_slice_untouched", func(t *testing.T) {
		emb := []float32{1, 0}
		_ = Projector{Mean: []float32{1, 0}, Strength: 1}.Project(emb)
		assert.Equal(t, []float32{1, 0}, emb)
	})
}

// projectAll is the shape callers use: centre a whole group against one mean.
func projectAll(p Projector, vecs [][]float32) [][]float32 {
	out := make([][]float32, len(vecs))
	for i, v := range vecs {
		out[i] = p.Project(v)
	}
	return out
}

// Centring exists to separate groups whose raw centroids are indistinguishable
// because both are dominated by a shared component. Two arcs 0.6 rad apart, each
// pushed into a narrow cone by a large common offset, stand in for the corpus.
func TestProjector_SeparatesGroupsSharedComponentHides(t *testing.T) {
	groupA, groupB := arc(0.0, 5, 6), arc(0.6, 5, 6)
	all := append(append([][]float32{}, groupA...), groupB...)

	rawSep := dist.CosineDistance(Centroid(groupA), Centroid(groupB))

	p := Projector{Mean: Mean(all), Strength: 1}
	centredSep := dist.CosineDistance(
		Centroid(projectAll(p, groupA)), Centroid(projectAll(p, groupB)))

	assert.Less(t, rawSep, 0.02, "fixture must start out hard to separate")
	assert.Greater(t, centredSep, 10*rawSep,
		"centring did not expose the separation (raw %.4f, centred %.4f)", rawSep, centredSep)
}

// Partial removal exists for the opposite case: a corpus that is itself one tight
// group. Full removal leaves only noise, which reads as opposition; keeping a
// fraction of the mean keeps the group together.
func TestProjector_PartialRemovalKeepsANarrowCorpusIntact(t *testing.T) {
	spread := func(strength float32) float64 {
		g := arc(0.0, 6, 0)
		centred := projectAll(Projector{Mean: Mean(g), Strength: strength}, g)
		return Radius(centred)
	}

	full, partial := spread(1.0), spread(0.9)
	assert.Greater(t, full, 0.5, "full removal should shatter a narrow corpus")
	assert.Less(t, partial, 0.5,
		"partial removal must hold a narrow corpus together (radius %.3f)", partial)
}

func TestMeasure(t *testing.T) {
	t.Run("reports_geometry_of_the_group", func(t *testing.T) {
		st := Measure(arc(0.0, 5, 0), nil)
		assert.Len(t, st.Dists, 5)
		assert.Greater(t, st.Radius, 0.0)
		assert.GreaterOrEqual(t, st.Radius, st.Mean)
		assert.GreaterOrEqual(t, st.Sigma, 0.0)
		assert.True(t, st.LatestAt.IsZero(), "no times supplied, so none reported")
	})

	t.Run("identical_members_have_no_spread", func(t *testing.T) {
		st := Measure([][]float32{{1, 0}, {1, 0}, {1, 0}}, nil)
		assert.InDelta(t, 0, st.Radius, 1e-6)
		assert.InDelta(t, 0, st.Sigma, 1e-6)
	})

	t.Run("empty_group", func(t *testing.T) {
		assert.Equal(t, Stats{}, Measure(nil, nil))
	})
}

// The gate must be the quadratic bound, not the Euclidean 2r: a Euclidean bound
// under-predicts how far apart two members of one ball can be, so it would skip
// groups that genuinely split.
func TestMaxAngularSeparation(t *testing.T) {
	assert.InDelta(t, 0.0, MaxAngularSeparation(0), 1e-9)
	assert.InDelta(t, 2.0, MaxAngularSeparation(1), 1e-9)
	assert.InDelta(t, 0.458, MaxAngularSeparation(0.122), 1e-3)
	assert.Greater(t, MaxAngularSeparation(0.122), 2*0.122,
		"the quadratic bound must exceed the Euclidean one")
}
