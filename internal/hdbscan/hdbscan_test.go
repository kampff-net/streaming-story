package hdbscan_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kvsh.ch/streaming-story/internal/hdbscan"
)

// labelCounts returns a map of label → count, excluding noise (−1).
func labelCounts(labels []int) map[int]int {
	m := make(map[int]int)
	for _, l := range labels {
		if l >= 0 {
			m[l]++
		}
	}
	return m
}

func noiseCount(labels []int) int {
	n := 0
	for _, l := range labels {
		if l == -1 {
			n++
		}
	}
	return n
}

// makeDenseBlob returns n identical points (maximum density cluster).
func makeDenseBlob(n int, angle float64) [][]float32 {
	pts := make([][]float32, n)
	s, c := math.Sincos(angle)
	for i := range pts {
		pts[i] = []float32{float32(c), float32(s)}
	}
	return pts
}

// --- parameter validation ---

func TestCluster_nil_input_returns_error(t *testing.T) {
	_, err := hdbscan.Cluster(nil, 2, 2)
	require.Error(t, err)
}

func TestCluster_empty_input_returns_error(t *testing.T) {
	_, err := hdbscan.Cluster([][]float32{}, 2, 2)
	require.Error(t, err)
}

func TestCluster_zero_minClusterSize_returns_error(t *testing.T) {
	pts := makeDenseBlob(5, 0)
	_, err := hdbscan.Cluster(pts, 0, 2)
	require.Error(t, err)
}

func TestCluster_zero_minSamples_returns_error(t *testing.T) {
	pts := makeDenseBlob(5, 0)
	_, err := hdbscan.Cluster(pts, 2, 0)
	require.Error(t, err)
}

func TestCluster_inconsistent_dimensions_returns_error(t *testing.T) {
	pts := [][]float32{{1, 2}, {3, 4, 5}}
	_, err := hdbscan.Cluster(pts, 2, 2)
	require.Error(t, err)
}

// --- result shape ---

func TestCluster_returns_one_label_per_point(t *testing.T) {
	pts := makeDenseBlob(10, 0)
	labels, err := hdbscan.Cluster(pts, 2, 2)
	require.NoError(t, err)
	assert.Len(t, labels, len(pts))
}

// --- core cluster detection ---

func TestCluster_two_tight_blobs_produce_two_clusters(t *testing.T) {
	// Two clusters with different angles.
	pts := make([][]float32, 0, 10)
	// Blob A around 0 degrees.
	for i := 0; i < 5; i++ {
		angle := float64(i) * 0.001
		s, c := math.Sincos(angle)
		pts = append(pts, []float32{float32(c), float32(s)})
	}
	// Blob B around 90 degrees.
	for i := 0; i < 5; i++ {
		angle := math.Pi/2 + float64(i)*0.001
		s, c := math.Sincos(angle)
		pts = append(pts, []float32{float32(c), float32(s)})
	}
	labels, err := hdbscan.Cluster(pts, 3, 3)
	require.NoError(t, err)
	counts := labelCounts(labels)
	assert.Len(t, counts, 2, "expected exactly 2 clusters, got %v", counts)
	for _, c := range counts {
		assert.GreaterOrEqual(t, c, 3)
	}
}

func TestCluster_three_tight_blobs_produce_three_clusters(t *testing.T) {
	pts := make([][]float32, 0, 15)
	for _, centerAngle := range []float64{0, math.Pi / 2, math.Pi} {
		for i := 0; i < 5; i++ {
			angle := centerAngle + float64(i)*0.001
			s, c := math.Sincos(angle)
			pts = append(pts, []float32{float32(c), float32(s)})
		}
	}
	labels, err := hdbscan.Cluster(pts, 3, 3)
	require.NoError(t, err)
	counts := labelCounts(labels)
	assert.Len(t, counts, 3, "expected exactly 3 clusters, got %v", counts)
}

// --- noise handling ---

func TestCluster_isolated_point_joins_main_cluster(t *testing.T) {
	// 5 tight points near (1,0) + 1 far outlier at (-1,0).
	// The outlier cannot form its own cluster (size 1 < minClusterSize 3), so it
	// falls under the single selected cluster — matching Python hdbscan behaviour
	// where points that fall out of a cluster because their side is too small are
	// still assigned to that cluster rather than being left as noise.
	pts := make([][]float32, 0, 6)
	for i := 0; i < 5; i++ {
		angle := float64(i) * 0.001
		s, c := math.Sincos(angle)
		pts = append(pts, []float32{float32(c), float32(s)})
	}
	// Far outlier at 180 degrees.
	pts = append(pts, []float32{-1, 0})

	labels, err := hdbscan.Cluster(pts, 3, 3)
	require.NoError(t, err)
	// The outlier is assigned to the same (only) cluster as the tight points.
	assert.GreaterOrEqual(t, labels[5], 0, "outlier that cannot form its own cluster should be assigned to the nearest cluster")
}

// --- edge cases ---

func TestCluster_single_point_is_noise(t *testing.T) {
	labels, err := hdbscan.Cluster([][]float32{{1, 0}}, 2, 2)
	require.NoError(t, err)
	require.Len(t, labels, 1)
	assert.Equal(t, -1, labels[0])
}

func TestCluster_fewer_points_than_minClusterSize(t *testing.T) {
	pts := [][]float32{{1, 0}, {0, 1}}
	labels, err := hdbscan.Cluster(pts, 5, 5)
	require.NoError(t, err)
	assert.Equal(t, 2, noiseCount(labels))
}

func TestCluster_all_identical_points_form_one_cluster(t *testing.T) {
	pts := makeDenseBlob(8, 0.5)
	labels, err := hdbscan.Cluster(pts, 3, 3)
	require.NoError(t, err)
	assert.Zero(t, noiseCount(labels), "identical points must not be noise")
	counts := labelCounts(labels)
	assert.Len(t, counts, 1, "identical points must form exactly one cluster")
}

func TestCluster_labels_are_zero_indexed_and_contiguous(t *testing.T) {
	// Two blobs → labels should be 0 and 1 with no gaps.
	pts := make([][]float32, 0)
	for i := 0; i < 5; i++ {
		angle := float64(i) * 0.001
		s, c := math.Sincos(angle)
		pts = append(pts, []float32{float32(c), float32(s)})
	}
	for i := 0; i < 5; i++ {
		angle := math.Pi/2 + float64(i)*0.001
		s, c := math.Sincos(angle)
		pts = append(pts, []float32{float32(c), float32(s)})
	}
	labels, err := hdbscan.Cluster(pts, 3, 3)
	require.NoError(t, err)
	counts := labelCounts(labels)
	require.Len(t, counts, 2)
	_, has0 := counts[0]
	_, has1 := counts[1]
	assert.True(t, has0 && has1, "cluster labels must be 0-indexed: got %v", counts)
}

func TestCluster_single_point_minClusterSize_1(t *testing.T) {
	pts := [][]float32{{1, 0}}
	labels, err := hdbscan.Cluster(pts, 1, 1)
	require.NoError(t, err)
	require.Len(t, labels, 1)
}
