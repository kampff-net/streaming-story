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

// ClusterSelection picks how the batch phase extracts clusters from the
// condensed cluster tree.
type ClusterSelection uint8

const (
	// ClusterSelectionEOM is excess-of-mass selection, the method described
	// in Campello et al. and the historical default. A node wins over its
	// whole subtree when its own stability is at least the sum of its
	// descendants'.
	//
	// EOM favours breadth. A large, diffuse, long-lived region accumulates
	// stability across the entire lambda range it survives, which can exceed
	// the summed stability of the tighter clusters nested inside it — so the
	// whole region emerges as one cluster and every story inside it is
	// reported as the same story. Raising MinClusterSize does not counteract
	// this: density pruning happens earlier, and a broad region that is
	// genuinely dense only becomes more stable.
	ClusterSelectionEOM ClusterSelection = iota

	// ClusterSelectionLeaf selects every leaf of the condensed tree,
	// ignoring stability comparisons between a node and its descendants. It
	// yields more, smaller, more homogeneous clusters and never collapses
	// nested clusters into their common parent.
	//
	// For news tracking this is usually the better choice: it reports "the
	// summit walkout" rather than "European politics". The cost is that a
	// genuinely broad story may be split across several stories, which the
	// batch phase's merge detection can later reunite.
	ClusterSelectionLeaf
)

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

	// ClusterSelection chooses how clusters are extracted from the condensed
	// tree (default: ClusterSelectionEOM). See the constants for the
	// trade-off. ClusterSelectionLeaf suits corpora where a broad subject
	// area would otherwise absorb the distinct stories inside it.
	ClusterSelection ClusterSelection

	// MaxClusterSize rejects candidate clusters holding more than this many
	// sampled signals, forcing extraction to descend into their children.
	// Zero means unlimited (default). Applies to ClusterSelectionEOM only.
	MaxClusterSize int

	// Draft-phase assignment.
	AssignmentK float64 // σ multiplier for per-story assignment radius (default: 2.0)

	// InitialSigmaGlobal is the σ_global stand-in used before the first batch
	// run measures one (default: 0.25). Until then every story is in
	// cold-start, so this value alone decides the assignment radius: raise it
	// to make the first signals cluster together more readily, lower it to
	// make the Draft phase hold them as outliers until the first batch run
	// resolves the structure. It is ignored once σ_global is seeded.
	InitialSigmaGlobal float64

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
	if c.ClusterSelection != ClusterSelectionEOM && c.ClusterSelection != ClusterSelectionLeaf {
		return fmt.Errorf("story: Config.ClusterSelection %d is not a known selection method", c.ClusterSelection)
	}
	if c.MaxClusterSize < 0 {
		return fmt.Errorf("story: Config.MaxClusterSize must be >= 0, got %d", c.MaxClusterSize)
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
	if c.InitialSigmaGlobal == 0 {
		c.InitialSigmaGlobal = 0.25
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
