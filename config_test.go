package story

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kvsh.ch/streaming-story/internal/keys"
)

// minimalConfig returns a Config with only the required fields set,
// suitable as a starting point for validate() tests.
func minimalConfig() Config[string] {
	return Config[string]{
		Store: newMemStore(),
		Codec: CBORCodec[string]{},
	}
}

func TestConfig_validate(t *testing.T) {
	t.Run("nil_store_returns_error", func(t *testing.T) {
		cfg := Config[string]{Codec: CBORCodec[string]{}}
		require.Error(t, cfg.validate())
	})

	t.Run("nil_codec_returns_error", func(t *testing.T) {
		cfg := Config[string]{Store: newMemStore()}
		require.Error(t, cfg.validate())
	})

	t.Run("zero_namespace_defaults_to_TrackerNamespace", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, TrackerNamespace, cfg.Namespace)
	})

	t.Run("explicit_namespace_preserved", func(t *testing.T) {
		ns := uuid.MustParse("00000000-0000-0000-0000-000000000001")
		cfg := minimalConfig()
		cfg.Namespace = ns
		require.NoError(t, cfg.validate())
		assert.Equal(t, ns, cfg.Namespace)
	})

	t.Run("default_BatchWindow", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 24*time.Hour, cfg.BatchWindow)
	})

	t.Run("explicit_BatchWindow_preserved", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.BatchWindow = 2 * time.Hour
		require.NoError(t, cfg.validate())
		assert.Equal(t, 2*time.Hour, cfg.BatchWindow)
	})

	t.Run("default_BatchInterval", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 30*time.Minute, cfg.BatchInterval)
	})

	t.Run("default_SilenceWindow", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 7*24*time.Hour, cfg.SilenceWindow)
	})

	t.Run("default_ArchiveWindow", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 30*24*time.Hour, cfg.ArchiveWindow)
	})

	t.Run("OutlierTTL_defaults_to_2x_BatchWindow", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.BatchWindow = 3 * time.Hour
		require.NoError(t, cfg.validate())
		assert.Equal(t, 6*time.Hour, cfg.OutlierTTL)
	})

	t.Run("explicit_OutlierTTL_preserved", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.OutlierTTL = 5 * time.Hour
		require.NoError(t, cfg.validate())
		assert.Equal(t, 5*time.Hour, cfg.OutlierTTL)
	})

	t.Run("explicit_MinStorySize_preserved", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.MinStorySize = 5
		require.NoError(t, cfg.validate())
		assert.Equal(t, 5, cfg.MinStorySize)
	})

	t.Run("default_AssignmentK", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 2.0, cfg.AssignmentK)
	})

	t.Run("default_ColdStartMinSignals", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 5, cfg.ColdStartMinSignals)
	})

	t.Run("default_SigmaFloor", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.InDelta(t, 0.1, cfg.SigmaFloor, 1e-9)
	})

	t.Run("default_EMAAlpha", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.InDelta(t, 0.1, cfg.EMAAlpha, 1e-9)
	})

	t.Run("default_IngestBufferCap", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 10_000, cfg.IngestBufferCap)
	})

	t.Run("default_EventBufferSize", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 512, cfg.EventBufferSize)
	})

	t.Run("default_thresholds", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		// Centred-space distances, roughly twice the raw-cosine scale these
		// values used to carry; see internal/geom.
		assert.InDelta(t, 0.50, cfg.AssignThreshold, 1e-9)
		assert.InDelta(t, 0.40, cfg.MergeThreshold, 1e-9)
		assert.InDelta(t, 0.55, cfg.SplitThreshold, 1e-9)
		assert.Equal(t, 3, cfg.MinStorySize)
		assert.InDelta(t, 0.9, cfg.MeanRemoval, 1e-9)
	})

	t.Run("MeanRemoval_bounds", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.MeanRemoval = 1.5
		assert.Error(t, cfg.validate())

		cfg = minimalConfig()
		cfg.MeanRemoval = -0.1
		assert.Error(t, cfg.validate())

		cfg = minimalConfig()
		cfg.MeanRemoval = 1.0
		assert.NoError(t, cfg.validate())
	})

	t.Run("explicit_thresholds_preserved", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.AssignThreshold = 0.4
		cfg.MergeThreshold = 0.1
		cfg.SplitThreshold = 0.5
		cfg.MinStorySize = 7
		require.NoError(t, cfg.validate())
		assert.InDelta(t, 0.4, cfg.AssignThreshold, 1e-9)
		assert.InDelta(t, 0.1, cfg.MergeThreshold, 1e-9)
		assert.InDelta(t, 0.5, cfg.SplitThreshold, 1e-9)
		assert.Equal(t, 7, cfg.MinStorySize)
	})

	// The hysteresis band is the whole reason split and merge do not undo
	// each other, so collapsing it must fail loudly rather than silently
	// producing story-ID churn.
	t.Run("SplitThreshold_equal_to_MergeThreshold_returns_error", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.MergeThreshold = 0.22
		cfg.SplitThreshold = 0.22
		require.Error(t, cfg.validate())
	})

	t.Run("SplitThreshold_below_MergeThreshold_returns_error", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.MergeThreshold = 0.25
		cfg.SplitThreshold = 0.20
		require.Error(t, cfg.validate())
	})

	t.Run("MergeThreshold_at_or_above_AssignThreshold_returns_error", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.AssignThreshold = 0.2
		cfg.MergeThreshold = 0.2
		require.Error(t, cfg.validate())
	})

	t.Run("AssignThreshold_out_of_range_returns_error", func(t *testing.T) {
		for _, v := range []float64{-0.1, 1.5} {
			cfg := minimalConfig()
			cfg.AssignThreshold = v
			require.Error(t, cfg.validate(), "AssignThreshold=%g", v)
		}
	})

	t.Run("MinStorySize_below_two_returns_error", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.MinStorySize = 1
		require.Error(t, cfg.validate())
	})
}

// --- MaxFacetsPerSignal (spec 007 §2.2.5) ---

func TestConfig_MaxFacetsPerSignalDefault(t *testing.T) {
	cfg := Config[string]{Store: newMemStore(), Codec: CBORCodec[string]{}}
	require.NoError(t, cfg.validate())
	assert.Equal(t, 8, cfg.MaxFacetsPerSignal)
}

func TestConfig_MaxFacetsPerSignalBounds(t *testing.T) {
	for _, n := range []int{-1, keys.MaxFacet + 1} {
		cfg := Config[string]{Store: newMemStore(), Codec: CBORCodec[string]{}, MaxFacetsPerSignal: n}
		err := cfg.validate()
		require.Error(t, err, "MaxFacetsPerSignal %d must be rejected", n)
		assert.Contains(t, err.Error(), "MaxFacetsPerSignal")
	}

	cfg := Config[string]{Store: newMemStore(), Codec: CBORCodec[string]{}, MaxFacetsPerSignal: keys.MaxFacet}
	assert.NoError(t, cfg.validate())
}

func TestConfig_IngestRejectsTooManyFacets(t *testing.T) {
	tr, err := NewTracker[string](Config[string]{
		Store: newMemStore(), Codec: CBORCodec[string]{},
		BatchInterval: time.Hour, MaxFacetsPerSignal: 2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = tr.Close() })

	_, err = tr.Ingest(context.Background(), Signal[string]{
		ID: uuid.New(), At: time.Now(),
		Embeddings: []Embedding{{1, 0}, {0, 1}, {1, 1}},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTooManyFacets)
}
