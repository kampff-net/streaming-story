package story

import (
	"context"
	"math/rand"
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
			ID:        uuid.New(),
			At:        now.Add(-time.Duration(i) * time.Second),
			Embedding: benchBlob(rng, (i%4)*2),
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

	ctx := context.Background()
	now := time.Now()

	b.ResetTimer()
	for b.Loop() {
		sig := Signal[string]{ID: uuid.New(), At: now, Embedding: benchBlob(rng, 0)}
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
			ID:        uuid.New(),
			At:        now.Add(-time.Duration(i) * time.Second),
			Embedding: benchBlob(rng, (i%4)*2),
		}
		if _, err := tr.Ingest(context.Background(), sig); err != nil {
			b.Fatal(err)
		}
	}
	tr.runBatch()

	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		sig := Signal[string]{ID: uuid.New(), At: time.Now(), Embedding: benchBlob(rng, 0)}
		if _, err := tr.Ingest(ctx, sig); err != nil {
			b.Fatal(err)
		}
	}
}
