package story

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/google/uuid"

	"go.kvsh.ch/streaming-story/internal/geom"
)

const benchDim = 8

// benchBlob returns a point near the given axis pair, jittered so the cluster
// has a non-zero radius and therefore a usable σ.
func benchBlob(rng *rand.Rand, axis int) []float32 {
	v := make([]float32, benchDim)
	for i := range v {
		v[i] = float32(rng.NormFloat64()) * 0.02
	}
	v[axis]++
	v[(axis+1)%benchDim]++
	return v
}

// BenchmarkBatch measures a full batch cycle — collect, cluster, map, apply —
// over a full batch window.
func BenchmarkBatch(b *testing.B) {
	const signals = 400

	rng := rand.New(rand.NewSource(7))
	now := time.Now()

	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		Codec:         CBORCodec[string]{},
		BatchInterval: time.Hour, // the ticker must not fire mid-benchmark
		MinStorySize:  3,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = tr.Close() })

	for i := range signals {
		sig := Signal[string]{
			ID:         uuid.New(),
			At:         now.Add(-time.Duration(i) * time.Second),
			Embeddings: []Embedding{benchBlob(rng, (i%4)*2)},
		}
		if _, err := tr.Ingest(context.Background(), sig); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for b.Loop() {
		tr.runBatch()
	}
}

// BenchmarkBatchFacets measures a full batch cycle as a function of how many
// facets each signal carries. A signal is named by one marker per facet, so a
// batch that decoded per marker decoded the record once per facet; this is what
// the signal cache in collectBatch removes, and multi-facet is the only shape
// that shows it.
func BenchmarkBatchFacets(b *testing.B) {
	for _, facets := range []int{1, 3} {
		b.Run(fmt.Sprintf("F%d", facets), func(b *testing.B) {
			const signals = 400

			rng := rand.New(rand.NewSource(7))
			now := time.Now()

			tr, err := NewTracker[string](Config[string]{
				Store:         newMemStore(),
				Codec:         CBORCodec[string]{},
				BatchInterval: time.Hour,
				MinStorySize:  3,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = tr.Close() })

			// A payload with some bulk: the decode the cache saves is dominated
			// by the data, not by the vector.
			payload := strings.Repeat("lorem ipsum dolor sit amet consectetur ", 40)

			for i := range signals {
				embs := make([]Embedding, facets)
				for f := range embs {
					embs[f] = benchBlob(rng, ((i+f)%4)*2)
				}
				sig := Signal[string]{
					ID:         uuid.New(),
					At:         now.Add(-time.Duration(i) * time.Second),
					Embeddings: embs,
					Data:       payload,
				}
				if _, err := tr.Ingest(context.Background(), sig); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for b.Loop() {
				tr.runBatch()
			}
		})
	}
}

// BenchmarkIngestDuringApply measures the Ingest latency seen by callers while
// a batch Apply holds the store's write lock: the staging-channel path.
func BenchmarkIngestDuringApply(b *testing.B) {
	rng := rand.New(rand.NewSource(11))

	tr, err := NewTracker[string](Config[string]{
		Store:           newMemStore(),
		Codec:           CBORCodec[string]{},
		BatchInterval:   time.Hour,
		IngestBufferCap: 1 << 20, // large enough that the benchmark never blocks
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = tr.Close() })
	tr.applyInProgress.Store(true)

	// Drain the staging channel for the duration. Without a consumer the
	// benchmark fills IngestBufferCap and blocks forever on the send — which
	// is not the path under test: in production the batch goroutine drains
	// this channel as the Apply proceeds.
	stop := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case <-tr.ingestBuffer:
			case <-stop:
				return
			}
		}
	}()
	b.Cleanup(func() { close(stop); <-drained })

	ctx := context.Background()
	now := time.Now()

	b.ResetTimer()
	for b.Loop() {
		sig := Signal[string]{ID: uuid.New(), At: now, Embeddings: []Embedding{benchBlob(rng, 0)}}
		if _, err := tr.Ingest(ctx, sig); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIngestSteadyState measures the ordinary Draft-phase write path:
// nearest-story lookup plus the store transaction.
func BenchmarkIngestSteadyState(b *testing.B) {
	rng := rand.New(rand.NewSource(13))
	now := time.Now()

	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		Codec:         CBORCodec[string]{},
		BatchInterval: time.Hour,
		MinStorySize:  3,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = tr.Close() })

	// Seed stories so the lookup has real candidates to scan.
	for i := range 200 {
		sig := Signal[string]{
			ID:         uuid.New(),
			At:         now.Add(-time.Duration(i) * time.Second),
			Embeddings: []Embedding{benchBlob(rng, (i%4)*2)},
		}
		if _, err := tr.Ingest(context.Background(), sig); err != nil {
			b.Fatal(err)
		}
	}
	tr.runBatch()

	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		sig := Signal[string]{ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{benchBlob(rng, 0)}}
		if _, err := tr.Ingest(ctx, sig); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSignalsOf measures the story-to-signals read path. Spec 007 moves
// the signal payload out from under the story prefix into a canonical record,
// which turns this from one sequential prefix scan into a scan plus a random
// read per member. The baseline captured here is what that change is measured
// against.
func BenchmarkSignalsOf(b *testing.B) {
	const signals = 400

	rng := rand.New(rand.NewSource(17))
	now := time.Now()

	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		Codec:         CBORCodec[string]{},
		BatchInterval: time.Hour,
		MinStorySize:  3,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = tr.Close() })

	for i := range signals {
		sig := Signal[string]{
			ID:         uuid.New(),
			At:         now.Add(-time.Duration(i) * time.Second),
			Embeddings: []Embedding{benchBlob(rng, (i%4)*2)},
		}
		if _, err := tr.Ingest(context.Background(), sig); err != nil {
			b.Fatal(err)
		}
	}
	tr.runBatch()

	// Read back the largest story, so the benchmark measures a member list
	// worth iterating rather than whichever story happened to come first.
	var target uuid.UUID
	best := -1
	for meta := range tr.Stories(StoryStateAny) {
		n := 0
		for _, err := range tr.SignalsOf(meta.ID) {
			if err != nil {
				b.Fatal(err)
			}
			n++
		}
		if n > best {
			best, target = n, meta.ID
		}
	}
	if best <= 0 {
		b.Fatal("no story with members")
	}
	b.Logf("largest story holds %d signals", best)

	b.ResetTimer()
	for b.Loop() {
		for _, err := range tr.SignalsOf(target) {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// TestStoreFootprint measures the sum of encoded value bytes across all keys
// in a populated store after a batch pass, matching the §2.7 footprint metric.
func TestStoreFootprint(t *testing.T) {
	const signals = 400

	rng := rand.New(rand.NewSource(7))
	now := time.Now()

	ms := newMemStore()
	tr, err := NewTracker[string](Config[string]{
		Store:         ms,
		Codec:         CBORCodec[string]{},
		BatchInterval: time.Hour,
		MinStorySize:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	for i := range signals {
		sig := Signal[string]{
			ID:         uuid.New(),
			At:         now.Add(-time.Duration(i) * time.Second),
			Embeddings: []Embedding{benchBlob(rng, (i%4)*2)},
		}
		if _, err := tr.Ingest(context.Background(), sig); err != nil {
			t.Fatal(err)
		}
	}
	tr.runBatch()

	var totalBytes int
	for _, v := range ms.data {
		totalBytes += len(v)
	}
	t.Logf("STORE_FOOTPRINT_BYTES: %d (across %d keys)", totalBytes, len(ms.data))
}

// collectAllocFixture builds a tracker holding a batch's worth of multi-facet
// signals at a realistic dimension. benchDim is deliberately tiny, which hides
// the per-facet vector allocations the collect phase is measured for.
func collectAllocFixture(t *testing.T) (*Tracker[string], time.Time) {
	t.Helper()

	const (
		signals = 400
		facets  = 3
		dim     = 256
	)

	rng := rand.New(rand.NewSource(7))
	now := time.Now()

	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		Codec:         CBORCodec[string]{},
		BatchInterval: time.Hour,
		MinStorySize:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	for i := range signals {
		embs := make([]Embedding, facets)
		for f := range embs {
			v := make([]float32, dim)
			for j := range v {
				v[j] = float32(rng.NormFloat64()) * 0.02
			}
			v[((i+f)%8)*2]++
			embs[f] = v
		}
		sig := Signal[string]{
			ID:         uuid.New(),
			At:         now.Add(-time.Duration(i) * time.Second),
			Embeddings: embs,
		}
		if _, err := tr.Ingest(context.Background(), sig); err != nil {
			t.Fatal(err)
		}
	}
	tr.runBatch()

	return tr, now
}

// TestCollectPhaseAllocs measures the allocation cost of the batch collect
// phase — collection plus the mean-removal projection — against the §2.7
// target of a 60% reduction. The figure is reported, not asserted: the
// comparison lives in the spec's comparison file, since a threshold here would
// pin a number that only means something beside its baseline.
func TestCollectPhaseAllocs(t *testing.T) {
	tr, now := collectAllocFixture(t)

	const runs = 5
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range runs {
		var (
			signals []batchFacet
			mean    []float32
		)
		if err := tr.cfg.Store.View(func(tx Tx) error {
			var err error
			signals, _, _, mean, _, err = tr.collectBatch(tx, now)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if len(mean) > 0 && tr.cfg.MeanRemoval > 0 {
			strength := float32(tr.cfg.MeanRemoval)
			for i := range signals {
				geom.ProjectInPlace(signals[i].emb, mean, strength)
			}
		}
	}
	runtime.ReadMemStats(&after)

	t.Logf("COLLECT_PHASE: %d B/op, %d allocs/op",
		(after.TotalAlloc-before.TotalAlloc)/runs,
		(after.Mallocs-before.Mallocs)/runs)
}

// TestActiveStoryIndexMemory reports the resident cost of activeStoryIndex at
// the two populations §2.7 names, against its ceiling of 8*dim+128 bytes per
// non-archived story. The index is built directly rather than through a batch:
// the cost is a function of story count and dimension, and ingesting 10,000
// stories to learn that would measure the ingest path instead.
func TestActiveStoryIndexMemory(t *testing.T) {
	const dim = 1536

	perStoryCeiling := 8*dim + 128

	for _, stories := range []int{500, 10000} {
		idx := &activeStoryIndex{
			dim:     dim,
			ids:     make([]uuid.UUID, stories),
			recents: make([]float32, stories*dim),
			metas:   make([]activeStoryMeta, stories),
		}

		total := int(unsafe.Sizeof(*idx)) +
			len(idx.ids)*int(unsafe.Sizeof(uuid.UUID{})) +
			len(idx.recents)*int(unsafe.Sizeof(float32(0))) +
			len(idx.metas)*int(unsafe.Sizeof(activeStoryMeta{}))
		perStory := total / stories

		t.Logf("INDEX_MEMORY: stories=%d dim=%d total=%d bytes, per-story=%d bytes (ceiling %d)",
			stories, dim, total, perStory, perStoryCeiling)

		if perStory > perStoryCeiling {
			t.Errorf("index costs %d bytes per story, over the %d ceiling",
				perStory, perStoryCeiling)
		}
	}
}
