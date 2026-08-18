package story

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
		Codec:         JSONCodec[string]{},
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
				Codec:         JSONCodec[string]{},
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
		Codec:           JSONCodec[string]{},
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
		Codec:         JSONCodec[string]{},
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
		Codec:         JSONCodec[string]{},
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
