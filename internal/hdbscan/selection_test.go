package hdbscan_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kvsh.ch/streaming-story/internal/hdbscan"
)

// arcBlob returns n points spread evenly over an arc of the given angular
// width, centred on center. A small spread makes a tight blob, a larger one a
// diffuse region.
func arcBlob(n int, center, spread float64) [][]float32 {
	pts := make([][]float32, n)
	for i := range pts {
		a := center + spread*(float64(i)/float64(n-1)-0.5)
		s, c := math.Sincos(a)
		pts[i] = []float32{float32(c), float32(s)}
	}
	return pts
}

// nestedRegion builds three neighbouring sub-blobs that together form one
// broad region, plus a distant fourth blob. The sub-blobs are close enough
// that their common parent in the condensed tree out-masses them, which is
// the structure excess-of-mass collapses and leaf selection does not.
func nestedRegion() [][]float32 {
	const (
		sub    = 0.05
		spread = 0.06
	)
	var pts [][]float32
	pts = append(pts, arcBlob(10, 0.30, spread)...)
	pts = append(pts, arcBlob(10, 0.30+sub, spread)...)
	pts = append(pts, arcBlob(10, 0.30+2*sub, spread)...)
	pts = append(pts, arcBlob(10, 2.50, spread)...)
	return pts
}

func largestCluster(labels []int) int {
	big := 0
	for _, c := range labelCounts(labels) {
		if c > big {
			big = c
		}
	}
	return big
}

// --- parameter validation ---

func TestClusterWithOptions_unknown_selection_returns_error(t *testing.T) {
	_, err := hdbscan.ClusterWithOptions(makeDenseBlob(5, 0), hdbscan.Options{
		MinClusterSize: 3,
		MinSamples:     3,
		Selection:      hdbscan.Selection(42),
	})
	require.Error(t, err)
}

func TestClusterWithOptions_negative_maxClusterSize_returns_error(t *testing.T) {
	_, err := hdbscan.ClusterWithOptions(makeDenseBlob(5, 0), hdbscan.Options{
		MinClusterSize: 3,
		MinSamples:     3,
		MaxClusterSize: -1,
	})
	require.Error(t, err)
}

// TestCluster_is_ClusterWithOptions_with_defaults pins the shorthand to
// excess-of-mass with no size cap, so existing callers keep their behaviour.
func TestCluster_is_ClusterWithOptions_with_defaults(t *testing.T) {
	pts := nestedRegion()

	short, err := hdbscan.Cluster(pts, 5, 5)
	require.NoError(t, err)

	long, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{MinClusterSize: 5, MinSamples: 5})
	require.NoError(t, err)

	assert.Equal(t, short, long)
}

// --- leaf selection ---

// TestSelectionLeaf_splits_a_region_excess_of_mass_collapses is the
// regression this mode exists for. Three adjacent sub-blobs form one broad
// region; EOM prefers the parent and reports all 30 points as a single
// cluster, which in a story-tracking context is a blob that swallows
// unrelated material. Leaf selection keeps the sub-blobs apart.
func TestSelectionLeaf_splits_a_region_excess_of_mass_collapses(t *testing.T) {
	pts := nestedRegion()

	eom, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{MinClusterSize: 5, MinSamples: 5})
	require.NoError(t, err)
	require.Equal(t, 30, largestCluster(eom),
		"fixture no longer reproduces the parent collapse: %v", labelCounts(eom))

	leaf, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{
		MinClusterSize: 5,
		MinSamples:     5,
		Selection:      hdbscan.SelectionLeaf,
	})
	require.NoError(t, err)

	assert.Greater(t, len(labelCounts(leaf)), len(labelCounts(eom)),
		"leaf selection must not be coarser than EOM: eom=%v leaf=%v",
		labelCounts(eom), labelCounts(leaf))
	assert.Less(t, largestCluster(leaf), 30,
		"leaf selection must break up the collapsed region: %v", labelCounts(leaf))
}

// TestSelectionLeaf_is_never_coarser_than_EOM checks the ordering property
// across several shapes rather than one fixture: leaves are a refinement of
// whatever EOM picks, so leaf mode can never return fewer clusters.
func TestSelectionLeaf_is_never_coarser_than_EOM(t *testing.T) {
	cases := map[string][][]float32{
		"nested_region": nestedRegion(),
		"two_tight_blobs": append(
			arcBlob(8, 0, 0.01),
			arcBlob(8, math.Pi/2, 0.01)...,
		),
		"single_blob": arcBlob(12, 0.7, 0.02),
	}

	for name, pts := range cases {
		t.Run(name, func(t *testing.T) {
			eom, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{MinClusterSize: 4, MinSamples: 4})
			require.NoError(t, err)

			leaf, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{
				MinClusterSize: 4,
				MinSamples:     4,
				Selection:      hdbscan.SelectionLeaf,
			})
			require.NoError(t, err)

			assert.GreaterOrEqual(t, len(labelCounts(leaf)), len(labelCounts(eom)),
				"eom=%v leaf=%v", labelCounts(eom), labelCounts(leaf))
		})
	}
}

// TestSelectionLeaf_keeps_zero_stability_leaves guards the decision to apply
// no stability floor in leaf mode. A cluster of identical points dies the
// instant it is born, giving it zero stability; filtering on stability would
// send the densest possible grouping to noise.
func TestSelectionLeaf_keeps_zero_stability_leaves(t *testing.T) {
	var pts [][]float32
	pts = append(pts, makeDenseBlob(6, 0)...)
	pts = append(pts, makeDenseBlob(6, math.Pi/2)...)

	labels, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{
		MinClusterSize: 3,
		MinSamples:     3,
		Selection:      hdbscan.SelectionLeaf,
	})
	require.NoError(t, err)

	assert.Zero(t, noiseCount(labels), "identical points must not be noise: %v", labels)
	assert.Len(t, labelCounts(labels), 2)
}

// --- max cluster size ---

func TestMaxClusterSize_forces_descent_past_an_oversized_parent(t *testing.T) {
	pts := nestedRegion()

	uncapped, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{MinClusterSize: 5, MinSamples: 5})
	require.NoError(t, err)
	require.Equal(t, 30, largestCluster(uncapped))

	capped, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{
		MinClusterSize: 5,
		MinSamples:     5,
		MaxClusterSize: 15,
	})
	require.NoError(t, err)

	assert.LessOrEqual(t, largestCluster(capped), 15,
		"no cluster may exceed the cap: %v", labelCounts(capped))
	assert.Greater(t, len(labelCounts(capped)), len(labelCounts(uncapped)))
}

func TestMaxClusterSize_zero_means_unlimited(t *testing.T) {
	pts := nestedRegion()

	unlimited, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{
		MinClusterSize: 5,
		MinSamples:     5,
		MaxClusterSize: 0,
	})
	require.NoError(t, err)

	plain, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{MinClusterSize: 5, MinSamples: 5})
	require.NoError(t, err)

	assert.Equal(t, plain, unlimited)
}

// TestMaxClusterSize_oversized_leaf_becomes_noise documents the deliberate
// trade-off: a leaf larger than the cap has no children to descend into, so
// its points are dropped rather than reported as a cluster the caller said
// was too wide to be meaningful.
func TestMaxClusterSize_oversized_leaf_becomes_noise(t *testing.T) {
	pts := makeDenseBlob(12, 0)

	labels, err := hdbscan.ClusterWithOptions(pts, hdbscan.Options{
		MinClusterSize: 3,
		MinSamples:     3,
		MaxClusterSize: 5,
	})
	require.NoError(t, err)

	assert.Equal(t, len(pts), noiseCount(labels),
		"an oversized leaf has no smaller alternative: %v", labels)
}
