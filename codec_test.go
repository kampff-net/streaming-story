package story

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type codecPayload struct {
	Title string
	Score int
}

func TestSignalCBORRoundTrip(t *testing.T) {
	t.Run("round_trips_a_signal", func(t *testing.T) {
		want := Signal[codecPayload]{
			ID:         uuid.New(),
			At:         time.Date(2024, 6, 1, 12, 0, 0, 123456789, time.UTC),
			Embeddings: []Embedding{[]float32{0.5, -0.25, 1}},
			Data:       codecPayload{Title: "headline", Score: 7},
		}

		b, err := cborEncMode.Marshal(want)
		require.NoError(t, err)

		var got Signal[codecPayload]
		err = cborDecMode.Unmarshal(b, &got)
		require.NoError(t, err)
		assert.Equal(t, want.ID, got.ID)
		assert.True(t, want.At.Equal(got.At))
		assert.Equal(t, want.At.Nanosecond(), got.At.Nanosecond(), "nanoseconds must match")
		assert.Equal(t, want.Embeddings[0], got.Embeddings[0])
		assert.Equal(t, want.Data, got.Data)
	})

	t.Run("malformed_input_returns_an_error", func(t *testing.T) {
		var got Signal[codecPayload]
		err := cborDecMode.Unmarshal([]byte{0xff, 0xff}, &got)
		require.Error(t, err)
	})
}

func TestRecordRoundTrips_NanosecondFidelity(t *testing.T) {
	now := time.Now().UTC()
	if now.Nanosecond() == 0 {
		now = now.Add(987654321 * time.Nanosecond)
	}

	t.Run("storyRecord", func(t *testing.T) {
		rec := storyRecord{
			State:              StoryStateActive,
			Centroid:           []float32{0.1, 0.2, 0.3},
			RecentCentroid:     []float32{0.11, 0.22, 0.33},
			Radius:             0.45,
			CreatedAt:          now,
			LastSignalAt:       now.Add(time.Second + 123456*time.Nanosecond),
			MeanDistance:       0.12,
			Sigma:              0.34,
			SignalCount:        15,
			FrozenMeanDistance: 0.10,
			FrozenSigma:        0.30,
		}
		b, err := cborEncMode.Marshal(rec)
		require.NoError(t, err)

		var got storyRecord
		err = cborStrictDecMode.Unmarshal(b, &got)
		require.NoError(t, err)
		assert.Equal(t, rec.State, got.State)
		assert.Equal(t, rec.Centroid, got.Centroid)
		assert.Equal(t, rec.RecentCentroid, got.RecentCentroid)
		assert.Equal(t, rec.Radius, got.Radius)
		assert.True(t, rec.CreatedAt.Equal(got.CreatedAt))
		assert.Equal(t, rec.CreatedAt.Nanosecond(), got.CreatedAt.Nanosecond())
		assert.True(t, rec.LastSignalAt.Equal(got.LastSignalAt))
		assert.Equal(t, rec.LastSignalAt.Nanosecond(), got.LastSignalAt.Nanosecond())
		assert.Equal(t, rec.MeanDistance, got.MeanDistance)
		assert.Equal(t, rec.Sigma, got.Sigma)
		assert.Equal(t, rec.SignalCount, got.SignalCount)
	})

	t.Run("calibState", func(t *testing.T) {
		cs := calibState{
			SigmaGlobal: 0.85,
			Dim:         1536,
			LastBatchAt: now,
			Mean:        []float32{0.01, 0.02},
		}
		b, err := cborEncMode.Marshal(cs)
		require.NoError(t, err)

		var got calibState
		err = cborStrictDecMode.Unmarshal(b, &got)
		require.NoError(t, err)
		assert.Equal(t, cs.SigmaGlobal, got.SigmaGlobal)
		assert.Equal(t, cs.Dim, got.Dim)
		assert.True(t, cs.LastBatchAt.Equal(got.LastBatchAt))
		assert.Equal(t, cs.LastBatchAt.Nanosecond(), got.LastBatchAt.Nanosecond())
		assert.Equal(t, cs.Mean, got.Mean)
	})
}

func TestTrackerSignalID(t *testing.T) {
	t.Run("is_stable_for_the_same_domain_key", func(t *testing.T) {
		tr := newTestTracker(t)
		assert.Equal(t, tr.SignalID("article-42"), tr.SignalID("article-42"))
		assert.NotEqual(t, tr.SignalID("article-42"), tr.SignalID("article-43"))
	})

	t.Run("defaults_to_TrackerNamespace", func(t *testing.T) {
		tr := newTestTracker(t)
		assert.Equal(t, uuid.NewSHA1(TrackerNamespace, []byte("k")), tr.SignalID("k"))
	})

	t.Run("honours_a_configured_namespace", func(t *testing.T) {
		ns := uuid.MustParse("11111111-2222-3333-4444-555555555555")
		tr, err := NewTracker[string](Config[string]{
			Store:         newMemStore(),
			BatchInterval: time.Hour,
			Namespace:     ns,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })

		assert.Equal(t, uuid.NewSHA1(ns, []byte("k")), tr.SignalID("k"))
		assert.NotEqual(t, uuid.NewSHA1(TrackerNamespace, []byte("k")), tr.SignalID("k"))
	})
}

func TestCBORSignalHeader_SkipsPayload(t *testing.T) {
	// Create small vs large payload signals
	smallPayload := "small"
	largePayload := make([]byte, 1024*1024) // 1MB payload
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	sigSmall := Signal[string]{
		ID:         uuid.New(),
		At:         time.Now(),
		Embeddings: []Embedding{[]float32{1.0, 2.0, 3.0}},
		Data:       smallPayload,
	}
	sigLarge := Signal[[]byte]{
		ID:         uuid.New(),
		At:         time.Now(),
		Embeddings: []Embedding{[]float32{1.0, 2.0, 3.0}},
		Data:       largePayload,
	}

	bSmall, err := cborEncMode.Marshal(sigSmall)
	require.NoError(t, err)

	bLarge, err := cborEncMode.Marshal(sigLarge)
	require.NoError(t, err)

	// Decode both using cborSignalHeader
	var hdrSmall cborSignalHeader
	require.NoError(t, cborDecMode.Unmarshal(bSmall, &hdrSmall))
	assert.Equal(t, sigSmall.Embeddings, hdrSmall.Embeddings)
	assert.True(t, sigSmall.At.Equal(hdrSmall.At))

	var hdrLarge cborSignalHeader
	require.NoError(t, cborDecMode.Unmarshal(bLarge, &hdrLarge))
	assert.Equal(t, sigLarge.Embeddings, hdrLarge.Embeddings)
	assert.True(t, sigLarge.At.Equal(hdrLarge.At))

	// Measure allocations: decoding the 1MB payload into cborSignalHeader
	// must NOT allocate proportional to 1MB.
	allocsLarge := testing.AllocsPerRun(100, func() {
		var h cborSignalHeader
		_ = cborDecMode.Unmarshal(bLarge, &h)
	})
	allocsSmall := testing.AllocsPerRun(100, func() {
		var h cborSignalHeader
		_ = cborDecMode.Unmarshal(bSmall, &h)
	})

	t.Logf("Allocs: small=%f, large=%f", allocsSmall, allocsLarge)
	assert.InDelta(t, allocsSmall, allocsLarge, 1.0, "header decode must allocate constant memory regardless of payload size")
}
