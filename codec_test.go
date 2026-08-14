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

func TestJSONCodec(t *testing.T) {
	t.Run("round_trips_a_signal", func(t *testing.T) {
		var c JSONCodec[codecPayload]
		want := Signal[codecPayload]{
			ID:        uuid.New(),
			At:        time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
			Embedding: []float32{0.5, -0.25, 1},
			Data:      codecPayload{Title: "headline", Score: 7},
		}

		b, err := c.Encode(want)
		require.NoError(t, err)

		got, err := c.Decode(b)
		require.NoError(t, err)
		assert.Equal(t, want.ID, got.ID)
		assert.True(t, want.At.Equal(got.At))
		assert.Equal(t, want.Embedding, got.Embedding)
		assert.Equal(t, want.Data, got.Data)
	})

	t.Run("malformed_input_returns_an_error", func(t *testing.T) {
		var c JSONCodec[codecPayload]
		_, err := c.Decode([]byte("{not json"))
		require.Error(t, err)
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
			Codec:         JSONCodec[string]{},
			BatchInterval: time.Hour,
			Namespace:     ns,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })

		assert.Equal(t, uuid.NewSHA1(ns, []byte("k")), tr.SignalID("k"))
		assert.NotEqual(t, uuid.NewSHA1(TrackerNamespace, []byte("k")), tr.SignalID("k"))
	})
}
