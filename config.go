package story

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Codec encodes and decodes Signal[T] values for persistence.
type Codec[T any] interface {
	Encode(sig Signal[T]) ([]byte, error)
	Decode(b []byte) (Signal[T], error)
}

// Config holds all configuration for a Tracker.
//
// Default values are calibrated for low-to-medium frequency news ingestion
// (1–10 signals/day per topic). High-frequency sources (social media,
// metrics) should reduce BatchWindow, BatchInterval, SilenceWindow, and
// ArchiveWindow, and raise MinClusterSize accordingly.
type Config[T any] struct {
	// Store is the persistence backend. Required.
	Store Store

	// Namespace is the UUID v5 namespace root for Signal IDs.
	// If zero-value, TrackerNamespace is used.
	Namespace uuid.UUID

	// Temporal windows.
	BatchWindow   time.Duration // span of signals fed to each batch run (default: 24h)
	BatchInterval time.Duration // how often to run a batch (default: 30m)
	SilenceWindow time.Duration // Active → Dormant (default: 7d)
	ArchiveWindow time.Duration // Dormant → Archived (default: 30d)

	// ActiveContextWindow is how far back the t: time index is consulted for
	// nearest-story lookup (Tier 3 Active Context). Stories whose last signal
	// is older than this window are not considered as Draft-phase anchors.
	// It defaults to ArchiveWindow.
	ActiveContextWindow time.Duration

	// Sampling.
	BatchSampleCap             int           // max signals per HDBSCAN run (default: 50_000)
	SampleGuaranteeMaxFraction float64       // max fraction of cap for per-story minimums (default: 0.5)
	OutlierTTL                 time.Duration // max outlier age relative to last batch (default: 2×BatchWindow)

	// HDBSCAN — MinClusterSize is a fixed constant, not derived from window population.
	MinClusterSize int // minimum points to form a cluster (default: 3)
	MinSamples     int // core-point density; defaults to MinClusterSize

	// Draft-phase assignment.
	AssignmentK         float64 // σ multiplier for per-story assignment radius (default: 2.0)
	ColdStartMinSignals int     // signals before per-story σ is trusted (default: 5)
	SigmaFloor          float64 // per-story σ floor as fraction of σ_global (default: 0.1)
	EMAAlpha            float64 // EMA decay for σ_global updates (default: 0.1)

	// Cluster mapping.
	MappingMinJaccard float64 // Jaccard threshold for primary cluster continuation (default: 0.6)
	SplitMinJaccard   float64 // Jaccard threshold for split/merge detection (default: 0.3)

	// Concurrency.
	IngestBufferCap int // signals buffered in memory during batch Apply (default: 10_000)
	EventBufferSize int // per-subscriber channel buffer depth (default: 512)

	// Codec encodes and decodes Signal[T] for persistence. Required.
	Codec Codec[T]

	// OnBatchError, if set, is called with any error that aborts a batch run.
	// A failed run leaves the store untouched and the next tick retries, so
	// this is the only way to observe batch failures. It is called from the
	// batch goroutine and must not block.
	OnBatchError func(error)
}

// validate checks required fields and applies defaults for zero-value fields.
func (c *Config[T]) validate() error {
	if c.Store == nil {
		return fmt.Errorf("story: Config.Store is required")
	}
	if c.Codec == nil {
		return fmt.Errorf("story: Config.Codec is required")
	}
	if c.Namespace == (uuid.UUID{}) {
		c.Namespace = TrackerNamespace
	}
	if c.BatchWindow == 0 {
		c.BatchWindow = 24 * time.Hour
	}
	if c.BatchInterval == 0 {
		c.BatchInterval = 30 * time.Minute
	}
	if c.SilenceWindow == 0 {
		c.SilenceWindow = 7 * 24 * time.Hour
	}
	if c.ArchiveWindow == 0 {
		c.ArchiveWindow = 30 * 24 * time.Hour
	}
	if c.ActiveContextWindow == 0 {
		c.ActiveContextWindow = c.ArchiveWindow
	}
	if c.BatchSampleCap == 0 {
		c.BatchSampleCap = 50_000
	}
	if c.SampleGuaranteeMaxFraction == 0 {
		c.SampleGuaranteeMaxFraction = 0.5
	}
	if c.OutlierTTL == 0 {
		c.OutlierTTL = 2 * c.BatchWindow
	}
	if c.MinClusterSize == 0 {
		c.MinClusterSize = 3
	}
	if c.MinSamples == 0 {
		c.MinSamples = c.MinClusterSize
	}
	if c.AssignmentK == 0 {
		c.AssignmentK = 2.0
	}
	if c.ColdStartMinSignals == 0 {
		c.ColdStartMinSignals = 5
	}
	if c.SigmaFloor == 0 {
		c.SigmaFloor = 0.1
	}
	if c.EMAAlpha == 0 {
		c.EMAAlpha = 0.1
	}
	if c.MappingMinJaccard == 0 {
		c.MappingMinJaccard = 0.6
	}
	if c.SplitMinJaccard == 0 {
		c.SplitMinJaccard = 0.3
	}
	if c.IngestBufferCap == 0 {
		c.IngestBufferCap = 10_000
	}
	if c.EventBufferSize == 0 {
		c.EventBufferSize = 512
	}
	return nil
}
