// Package hdbscan implements the HDBSCAN* clustering algorithm.
//
// Reference: Campello et al., "Density-Based Clustering Based on Hierarchical
// Density Estimates" (2013).
//
// Input:  n×d float32 matrix (one embedding per row)
// Output: []int label slice; −1 denotes noise.
//
// Distance metric: cosine distance (1 − cosine_similarity). Points are
// normalised internally; callers need not pre-normalise.
package hdbscan

import (
	"errors"
	"math"
	"sort"

	"go.kvsh.ch/streaming-story/internal/dist"
)

// Selection picks which nodes of the condensed cluster tree become clusters.
type Selection int

const (
	// SelectionEOM is excess-of-mass selection: a node wins over its whole
	// subtree when its own stability is at least the sum of its descendants'.
	// This is the method described in Campello et al. and the historical
	// default.
	//
	// EOM favours breadth. A large, diffuse, long-lived region accumulates
	// stability across the entire lambda range it survives, which can exceed
	// the summed stability of the tighter sub-clusters inside it. The whole
	// region then emerges as one cluster. Raising MinClusterSize does not
	// counteract this: density pruning happens earlier, during condensation,
	// and a broad region that is genuinely dense only becomes more stable.
	SelectionEOM Selection = iota

	// SelectionLeaf selects every leaf of the condensed tree, ignoring
	// stability comparisons between a node and its descendants. It yields
	// more, smaller, more homogeneous clusters, and never collapses distinct
	// sub-clusters into their common parent.
	//
	// Prefer it when the domain wants the finest stable grouping — distinct
	// news stories rather than a subject area, for instance — and accept that
	// a genuinely broad topic is reported as several clusters instead of one.
	SelectionLeaf
)

// Options configures a clustering run.
type Options struct {
	// MinClusterSize is the minimum number of points in a persistent
	// cluster. Required, must be >= 1.
	MinClusterSize int

	// MinSamples is the neighbourhood size used to compute core distances.
	// Required, must be >= 1.
	MinSamples int

	// Selection chooses the cluster extraction method. The zero value is
	// SelectionEOM.
	Selection Selection

	// MaxClusterSize rejects any candidate cluster holding more than this
	// many points, forcing selection to descend into its children instead.
	// Zero means unlimited.
	//
	// It applies to SelectionEOM only: leaf selection has no larger candidate
	// to fall back from, so capping there would discard points with nothing
	// to replace them. An over-sized leaf under EOM likewise has no children
	// to descend into, so its points become noise — the cap trades coverage
	// for cohesion, deliberately.
	MaxClusterSize int
}

// Cluster runs HDBSCAN* on pts with excess-of-mass selection and returns a
// cluster label per point. Labels are 0-indexed integers; −1 means noise.
//
//   - minClusterSize: minimum number of points in a persistent cluster.
//   - minSamples:     neighbourhood size used to compute the core distance.
//
// It is shorthand for ClusterWithOptions with the default Options.
func Cluster(pts [][]float32, minClusterSize, minSamples int) ([]int, error) {
	return ClusterWithOptions(pts, Options{
		MinClusterSize: minClusterSize,
		MinSamples:     minSamples,
	})
}

// ClusterWithOptions runs HDBSCAN* on pts and returns a cluster label per
// point. Labels are 0-indexed integers; −1 means noise.
func ClusterWithOptions(pts [][]float32, opts Options) ([]int, error) {
	minClusterSize, minSamples := opts.MinClusterSize, opts.MinSamples
	if len(pts) == 0 {
		return nil, errors.New("hdbscan: empty input")
	}
	if opts.Selection != SelectionEOM && opts.Selection != SelectionLeaf {
		return nil, errors.New("hdbscan: unknown selection method")
	}
	if opts.MaxClusterSize < 0 {
		return nil, errors.New("hdbscan: maxClusterSize must be >= 0")
	}
	if minClusterSize < 1 {
		return nil, errors.New("hdbscan: minClusterSize must be ≥ 1")
	}
	if minSamples < 1 {
		return nil, errors.New("hdbscan: minSamples must be ≥ 1")
	}
	dim := len(pts[0])
	for _, p := range pts[1:] {
		if len(p) != dim {
			return nil, errors.New("hdbscan: all points must have the same dimension")
		}
	}

	n := len(pts)
	labels := make([]int, n)
	for i := range labels {
		labels[i] = -1
	}

	if n < minClusterSize {
		return labels, nil
	}

	dist := pairwiseCosine(pts)
	core := coreDistances(dist, n, minSamples)
	mst := primMST(dist, core, n)

	// Sort MST edges in ascending order of MRD weight: closest first.
	sort.Slice(mst, func(i, j int) bool { return mst[i].w < mst[j].w })

	dn := buildDendrogram(mst, n)
	clusters, pointFallout := condense(dn, n, minClusterSize)

	if len(clusters) == 0 {
		return labels, nil
	}

	computeStability(clusters, pointFallout)
	selected := selectClusters(clusters, opts.Selection, opts.MaxClusterSize)

	labelID := 0
	for i := range clusters {
		if selected[i] {
			labelSubtree(clusters, i, labelID, labels, pointFallout)
			labelID++
		}
	}
	return labels, nil
}

func labelSubtree(clusters []cCluster, idx int, labelID int, labels []int, pointFallout []fallout) {
	for _, f := range pointFallout {
		if f.clusterIdx == idx {
			labels[f.pointIdx] = labelID
		}
	}
	for _, childIdx := range clusters[idx].children {
		labelSubtree(clusters, childIdx, labelID, labels, pointFallout)
	}
}

// ── Step 1: pairwise cosine distances ────────────────────────────────────────

func pairwiseCosine(pts [][]float32) [][]float64 {
	n := len(pts)

	// Normalize in float32, matching Python's: embs_n = embs / norm(embs, axis=1).
	// Python computes norms and the matrix multiply in float32, then converts to
	// float64.  Replicating this keeps our MRD values bit-compatible with the
	// reference, which matters for MST tie-breaking when min_samples > 1.
	normalized := make([][]float32, n)
	for i, p := range pts {
		norm := dist.Norm(p)
		normalized[i] = make([]float32, len(p))
		if norm > 0 {
			for k, v := range p {
				normalized[i][k] = v / norm
			}
		}
	}

	distMatrix := make([][]float64, n)
	for i := range distMatrix {
		distMatrix[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			// Float32 dot product matches Python's float32 matmul (BLAS SGEMM).
			dot := dist.Dot(normalized[i], normalized[j])
			if dot > 1 {
				dot = 1
			} else if dot < -1 {
				dot = -1
			}
			distMatrix[i][j] = float64(1 - dot)
		}
	}
	return distMatrix
}

// ── Step 2: core distances ───────────────────────────────────────────────────

func coreDistances(dist [][]float64, n, minSamples int) []float64 {
	core := make([]float64, n)
	row := make([]float64, n-1)
	for i := 0; i < n; i++ {
		idx := 0
		for j := 0; j < n; j++ {
			if j != i {
				row[idx] = dist[i][j]
				idx++
			}
		}
		// Python hdbscan's mutual_reachability(dist_matrix, k) uses the
		// k-th nearest neighbour *excluding* self (0-indexed: k-1).
		// Empirically confirmed: min_samples=5 → sorted_excl_self[4].
		core[i] = kthSmallest(row, minSamples-1)
	}
	return core
}

func kthSmallest(a []float64, k int) float64 {
	if len(a) == 0 || k < 0 {
		return 0
	}
	if k >= len(a) {
		k = len(a) - 1
	}
	cp := make([]float64, len(a))
	copy(cp, a)
	sort.Float64s(cp)
	return cp[k]
}

// ── Step 3: Prim's MST ───────────────────────────────────────────────────────

type edge struct {
	u, v int
	w    float64
}

func primMST(dist [][]float64, core []float64, n int) []edge {
	inTree := make([]bool, n)
	minW := make([]float64, n)
	parent := make([]int, n)
	for i := range minW {
		minW[i] = math.MaxFloat64
		parent[i] = -1
	}
	minW[0] = 0
	edges := make([]edge, 0, n-1)
	for range n {
		u := -1
		for v := 0; v < n; v++ {
			if !inTree[v] && (u == -1 || minW[v] < minW[u]) {
				u = v
			}
		}
		inTree[u] = true
		if parent[u] >= 0 {
			edges = append(edges, edge{u: parent[u], v: u, w: minW[u]})
		}
		for v := 0; v < n; v++ {
			if inTree[v] {
				continue
			}
			mrdUV := dist[u][v]
			if core[u] > mrdUV {
				mrdUV = core[u]
			}
			if core[v] > mrdUV {
				mrdUV = core[v]
			}
			if mrdUV < minW[v] {
				minW[v] = mrdUV
				parent[v] = u
			}
		}
	}
	return edges
}

// ── Step 4: single-linkage dendrogram ───────────────────────────────────────

type dendro struct {
	left, right int
	lambda      float64
	size        int
}

func buildDendrogram(mst []edge, n int) []dendro {
	nodes := make([]dendro, 2*n-1)
	for i := 0; i < n; i++ {
		nodes[i] = dendro{left: -1, right: -1, size: 1}
	}
	uf := newUF(n)
	repNode := make([]int, n)
	for i := range repNode {
		repNode[i] = i
	}
	next := n
	for _, e := range mst {
		ra, rb := uf.find(e.u), uf.find(e.v)
		la, lb := repNode[ra], repNode[rb]
		lam := math.Inf(1)
		if e.w > 0 {
			lam = 1.0 / e.w
		}
		nodes[next] = dendro{
			left:   la,
			right:  lb,
			lambda: lam,
			size:   nodes[la].size + nodes[lb].size,
		}
		merged := uf.union(ra, rb)
		repNode[merged] = next
		next++
	}
	return nodes[:next]
}

// ── Step 5: condensed cluster tree ──────────────────────────────────────────

type cCluster struct {
	lambdaBirth float64
	lambdaDeath float64
	size        int
	children    []int
	stability   float64
}

type fallout struct {
	pointIdx   int
	clusterIdx int
	lambda     float64
}

func condense(nodes []dendro, n, minClusterSize int) ([]cCluster, []fallout) {
	if len(nodes) == 0 {
		return nil, nil
	}
	root := len(nodes) - 1
	if nodes[root].size < minClusterSize {
		return nil, nil
	}

	clusters := []cCluster{{
		lambdaBirth: 0,
		lambdaDeath: 0,
		size:        nodes[root].size,
	}}
	var pointFallout []fallout

	walkDendro(nodes, root, 0, &clusters, &pointFallout, minClusterSize)
	return clusters, pointFallout
}

func walkDendro(nodes []dendro, idx, clusterIdx int, clusters *[]cCluster, pointFallout *[]fallout, mcs int) {
	nd := nodes[idx]
	if nd.left == -1 {
		// This point reached a leaf in the hierarchy.
		*pointFallout = append(*pointFallout, fallout{pointIdx: idx, clusterIdx: clusterIdx, lambda: math.Inf(1)})
		return
	}

	left, right := nodes[nd.left], nodes[nd.right]
	leftBig := left.size >= mcs
	rightBig := right.size >= mcs

	if leftBig && rightBig {
		// True split.
		(*clusters)[clusterIdx].lambdaDeath = nd.lambda

		li := len(*clusters)
		*clusters = append(*clusters, cCluster{lambdaBirth: nd.lambda, lambdaDeath: 0, size: left.size})
		ri := len(*clusters)
		*clusters = append(*clusters, cCluster{lambdaBirth: nd.lambda, lambdaDeath: 0, size: right.size})

		(*clusters)[clusterIdx].children = append((*clusters)[clusterIdx].children, li, ri)

		walkDendro(nodes, nd.left, li, clusters, pointFallout, mcs)
		walkDendro(nodes, nd.right, ri, clusters, pointFallout, mcs)
	} else if leftBig {
		// Right is noise fallout.
		collectFallout(nodes, nd.right, nd.lambda, clusterIdx, pointFallout)
		walkDendro(nodes, nd.left, clusterIdx, clusters, pointFallout, mcs)
	} else if rightBig {
		// Left is noise fallout.
		collectFallout(nodes, nd.left, nd.lambda, clusterIdx, pointFallout)
		walkDendro(nodes, nd.right, clusterIdx, clusters, pointFallout, mcs)
	} else {
		// Both are small, cluster dies here.
		(*clusters)[clusterIdx].lambdaDeath = nd.lambda
		collectFallout(nodes, nd.left, math.Inf(1), clusterIdx, pointFallout)
		collectFallout(nodes, nd.right, math.Inf(1), clusterIdx, pointFallout)
	}
}

func collectFallout(nodes []dendro, idx int, lambda float64, clusterIdx int, pointFallout *[]fallout) {
	nd := nodes[idx]
	if nd.left == -1 {
		*pointFallout = append(*pointFallout, fallout{pointIdx: idx, clusterIdx: clusterIdx, lambda: lambda})
		return
	}
	collectFallout(nodes, nd.left, lambda, clusterIdx, pointFallout)
	collectFallout(nodes, nd.right, lambda, clusterIdx, pointFallout)
}

// ── Step 6: cluster stability ────────────────────────────────────────────────

func computeStability(clusters []cCluster, pointFallout []fallout) {
	for i := range clusters {
		birth := clusters[i].lambdaBirth
		s := 0.0
		for _, f := range pointFallout {
			if f.clusterIdx == i {
				lamP := f.lambda
				if math.IsInf(lamP, 1) {
					lamP = clusters[i].lambdaDeath
				}
				d := lamP - birth
				if d > 0 {
					s += d
				}
			}
		}
		// Points in children are also in parent at parent's birth.
		for _, childIdx := range clusters[i].children {
			child := clusters[childIdx]
			s += float64(child.size) * (child.lambdaBirth - birth)
		}
		clusters[i].stability = s
	}
}

// ── Step 7: cluster selection ────────────────────────────────────────────────

// viable reports whether a cluster's stability qualifies it for selection.
func viable(c cCluster) bool {
	return c.stability > 0 || math.IsInf(c.stability, 1)
}

func selectClusters(clusters []cCluster, method Selection, maxClusterSize int) []bool {
	if method == SelectionLeaf {
		return selectLeaves(clusters)
	}
	return selectExcessOfMass(clusters, maxClusterSize)
}

// selectLeaves selects every leaf of the condensed tree. Leaves are disjoint
// and cover every point that survived condensation, so no stability
// comparison between levels is needed — the tradeoff a node-versus-subtree
// test would decide has already been settled in favour of the subtree.
//
// Unlike EOM, leaf selection applies no stability floor. A leaf with zero
// stability is one whose points all fall out at its own birth lambda, which
// happens whenever a cluster is tight enough to die immediately after
// splitting. Those are exactly the tightest groupings in the data, and
// discarding them would send the densest points to noise while looser ones
// survive.
func selectLeaves(clusters []cCluster) []bool {
	selected := make([]bool, len(clusters))
	for i := range clusters {
		if len(clusters[i].children) == 0 {
			selected[i] = true
		}
	}
	return selected
}

func selectExcessOfMass(clusters []cCluster, maxClusterSize int) []bool {
	n := len(clusters)
	propStability := make([]float64, n)
	for i := range propStability {
		propStability[i] = clusters[i].stability
	}

	// Bottom-up pass to propagate stability.
	for i := n - 1; i >= 0; i-- {
		if len(clusters[i].children) > 0 {
			childSum := 0.0
			for _, childIdx := range clusters[i].children {
				childSum += propStability[childIdx]
			}
			if childSum > propStability[i] {
				propStability[i] = childSum
			}
		}
	}

	oversized := func(idx int) bool {
		return maxClusterSize > 0 && clusters[idx].size > maxClusterSize
	}

	selected := make([]bool, n)
	// Top-down pass to select. Skip root if it split.
	var bfs []int
	if len(clusters[0].children) > 0 {
		bfs = append(bfs, clusters[0].children...)
	} else {
		// Root is leaf, so it's the only potential cluster.
		if viable(clusters[0]) && !oversized(0) {
			selected[0] = true
		}
		return selected
	}

	for len(bfs) > 0 {
		curr := bfs[0]
		bfs = bfs[1:]

		if len(clusters[curr].children) == 0 {
			// An oversized leaf has no children to descend into, so its
			// points fall through to noise rather than forming a cluster
			// wider than the caller allows.
			if viable(clusters[curr]) && !oversized(curr) {
				selected[curr] = true
			}
			continue
		}

		childSum := 0.0
		for _, childIdx := range clusters[curr].children {
			childSum += propStability[childIdx]
		}

		if clusters[curr].stability >= childSum && !oversized(curr) {
			selected[curr] = true
		} else {
			bfs = append(bfs, clusters[curr].children...)
		}
	}
	return selected
}

// ── Step 8: Union-Find ────────────────────────────────────────────────────────

type uf struct {
	parent, rank, size []int
}

func newUF(n int) *uf {
	u := &uf{
		parent: make([]int, n),
		rank:   make([]int, n),
		size:   make([]int, n),
	}
	for i := range u.parent {
		u.parent[i] = i
		u.size[i] = 1
	}
	return u
}

func (u *uf) find(x int) int {
	for u.parent[x] != x {
		u.parent[x] = u.parent[u.parent[x]]
		x = u.parent[x]
	}
	return x
}

func (u *uf) union(a, b int) int {
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return ra
	}
	if u.rank[ra] < u.rank[rb] {
		ra, rb = rb, ra
	}
	u.parent[rb] = ra
	u.size[ra] += u.size[rb]
	if u.rank[ra] == u.rank[rb] {
		u.rank[ra]++
	}
	return ra
}
