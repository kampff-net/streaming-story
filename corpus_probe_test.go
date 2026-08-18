package story

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"go.kvsh.ch/streaming-story/internal/cluster"
	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/geom"
)

func TestCorpusProbe(t *testing.T) {
	path := os.Getenv("CORPUS")
	if path == "" {
		t.Skip("set CORPUS")
	}
	pts := loadCorpus(t, path)
	t.Logf("loaded %d vectors, dim %d", len(pts), len(pts[0]))

	tr := newTestTracker(t)
	tr.dim.Store(int32(len(pts[0])))

	now := time.Now()
	for i, emb := range pts {
		id := uuid.NewSHA1(TrackerNamespace, []byte(fmt.Sprintf("corpus-%d", i)))
		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID: id, At: now, Embeddings: []Embedding{emb}, Data: fmt.Sprintf("s%d", i),
		})
		require.NoError(t, err)
	}
	probeReport(t, tr, len(pts), "after ingest (draft only)")
	for run := 1; run <= 5; run++ {
		tr.runBatch()
		probeReport(t, tr, len(pts), fmt.Sprintf("after batch %d", run))
	}
}

func probeReport(t *testing.T, tr *Tracker[string], total int, label string) {
	var sizes []int
	assigned := 0
	for meta := range tr.Stories(StoryStateAny) {
		n := 0
		for range tr.SignalsOf(meta.ID) {
			n++
		}
		sizes = append(sizes, n)
		assigned += n
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sizes)))
	shown := sizes
	if len(shown) > 12 {
		shown = shown[:12]
	}
	largest := 0
	if len(sizes) > 0 {
		largest = sizes[0]
	}
	t.Logf("%-22s stories=%3d assigned=%3d outliers=%3d largest=%3d %v",
		label, len(sizes), assigned, total-assigned, largest, shown)

	// Diagnose the largest story: why is split declining to cut it?
	//
	// Signals are stored raw, so every embedding read back here has to be put
	// through the projector before it is measured. Without that these numbers
	// describe a geometry the maintenance pass never sees.
	p := tr.projector()
	var big []*batchFacet
	for meta := range tr.Stories(StoryStateAny) {
		var group []*batchFacet
		for sig, err := range tr.SignalsOf(meta.ID) {
			if err == nil {
				group = append(group, &batchFacet{
					id: sig.ID, at: sig.At, emb: p.Project(sig.Embeddings[0]),
				})
			}
		}
		if len(group) > len(big) {
			big = group
		}
	}
	if len(big) >= 2*tr.cfg.MinStorySize {
		r := radiusOf(big)
		gate := geom.MaxAngularSeparation(r)
		pts := clusterPoints(big)
		a, b := cluster.TwoMedoids(pts)
		seedSep := dist.CosineDistance(big[a].emb, big[b].emb)
		res, ok := tr.splitStory(big, r)
		sep := 0.0
		if ok {
			sep = dist.CosineDistance(centroidOf(res.keep), centroidOf(res.spawn))
		} else {
			var l, rr []*batchFacet
			for _, m := range big {
				if dist.CosineDistance(m.emb, big[a].emb) <= dist.CosineDistance(m.emb, big[b].emb) {
					l = append(l, m)
				} else {
					rr = append(rr, m)
				}
			}
			if len(l) > 0 && len(rr) > 0 {
				sep = dist.CosineDistance(centroidOf(l), centroidOf(rr))
			}
		}
		t.Logf("    largest: n=%d radius=%.4f gate=%.4f(>%.2f?) seedSep=%.4f bestPartSep=%.4f split=%v",
			len(big), r, gate, tr.cfg.SplitThreshold, seedSep, sep, ok)
	}
}

func loadCorpus(t *testing.T, path string) [][]float32 {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	require.True(t, sc.Scan())
	var n, dim int
	fmt.Sscanf(sc.Text(), "%d %d", &n, &dim)
	var pts [][]float32
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != dim {
			continue
		}
		v := make([]float32, dim)
		for i, s := range fields {
			x, _ := strconv.ParseFloat(s, 32)
			v[i] = float32(x)
		}
		pts = append(pts, v)
	}
	return pts
}
