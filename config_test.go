package story

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimalConfig returns a Config with only the required fields set,
// suitable as a starting point for validate() tests.
func minimalConfig() Config[string] {
	return Config[string]{
		Store: newMemStore(),
		Codec: JSONCodec[string]{},
	}
}

func TestConfig_validate(t *testing.T) {
	t.Run("nil_store_returns_error", func(t *testing.T) {
		cfg := Config[string]{Codec: JSONCodec[string]{}}
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

	t.Run("default_BatchSampleCap", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 50_000, cfg.BatchSampleCap)
	})

	t.Run("default_SampleGuaranteeMaxFraction", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 0.5, cfg.SampleGuaranteeMaxFraction)
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

	t.Run("default_MinClusterSize", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.Equal(t, 3, cfg.MinClusterSize)
	})

	t.Run("MinSamples_defaults_to_MinClusterSize", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.MinClusterSize = 7
		require.NoError(t, cfg.validate())
		assert.Equal(t, 7, cfg.MinSamples)
	})

	t.Run("explicit_MinSamples_preserved", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.MinSamples = 5
		require.NoError(t, cfg.validate())
		assert.Equal(t, 5, cfg.MinSamples)
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

	t.Run("default_MappingMinJaccard", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.InDelta(t, 0.6, cfg.MappingMinJaccard, 1e-9)
	})

	t.Run("default_SplitMinJaccard", func(t *testing.T) {
		cfg := minimalConfig()
		require.NoError(t, cfg.validate())
		assert.InDelta(t, 0.3, cfg.SplitMinJaccard, 1e-9)
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
}
