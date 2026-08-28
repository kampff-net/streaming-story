package story

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"

	"go.kvsh.ch/streaming-story/internal/keys"
)

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Config holds all configuration for a Tracker.
//
// Default values are calibrated for low-to-medium frequency news ingestion
// (1–10 signals/day per topic). High-frequency sources (social media, metrics)
// should adjust BatchSchedule, SilenceWindow, and ArchiveWindow, and raise
// MinStorySize accordingly.
type Config[T any] struct {
	// Store is the persistence backend. Required.
	Store Store

	// Namespace is the UUID v5 namespace root for Signal IDs.
	// If zero-value, TrackerNamespace is used.
	Namespace uuid.UUID

	// Temporal windows.

	// BatchWindow is the reference span for outlier retention: it sets the
	// default OutlierTTL to 2×BatchWindow and has no other effect (default:
	// 24h). It does not bound the clustering input — story membership is read
	// in full on every run, because the lifetime centroid is the mean of every
	// member.
	BatchWindow time.Duration

	// BatchSchedule defines the cron expression for batch clustering runs
	// (e.g. "*/30 * * * *", "0 */2 * * *", "@hourly", "@every 30m").
	// Default: "*/30 * * * *".
	BatchSchedule string
	SilenceWindow time.Duration // Active → Dormant (default: 7d)
	ArchiveWindow time.Duration // Dormant → Archived (default: 30d)

	// ActiveContextWindow is how far back the t: time index is consulted for
	// nearest-story lookup (Tier 3 Active Context). Stories whose last signal
	// is older than this window are not considered as Draft-phase anchors.
	// It defaults to ArchiveWindow.
	ActiveContextWindow time.Duration

	// OutlierTTL is the maximum outlier age relative to the last batch run
	// (default: 2×BatchWindow).
	OutlierTTL time.Duration

	// Clustering thresholds, in cosine distance.

	// AssignThreshold is the maximum centroid distance for a signal to join a
	// story (default: 0.50). It is the cold-start assignment radius and the
	// ceiling for the adaptive per-story threshold, so a diffuse story cannot
	// widen its own catchment without bound.
	//
	// Like every threshold here it is a distance in centred space, not raw
	// cosine: the tracker subtracts the corpus mean before measuring anything
	// (see the internal/geom package). That scale is roughly twice raw cosine — the
	// reference corpus has a median pairwise distance of 1.02 centred against
	// 0.45 raw — so a value carried over from a raw-cosine configuration will
	// be far too tight.
	AssignThreshold float64

	// MergeThreshold is the centroid distance at or below which two stories
	// are the same story and are merged (default: 0.40).
	MergeThreshold float64

	// SplitThreshold is the centroid distance above which a story's best
	// two-way partition is two stories and is split (default: 0.55).
	//
	// It must exceed MergeThreshold. The band between them is hysteresis:
	// merge tests one historically-given partition while split searches for
	// the best one, so equal thresholds let a merge and a split undo each
	// other along different seams, churning story IDs without the underlying
	// data having changed.
	SplitThreshold float64

	// MeanRemoval is the fraction of the corpus mean direction subtracted from
	// every embedding before a distance is measured (default: 0.9). See the
	// internal/geom package for why anything is subtracted at all.
	//
	// It is not 1.0. Full removal is degenerate when the corpus is itself one
	// tight group: the mean then sits on top of every signal, the residuals are
	// whatever noise is left, and a single coherent story shatters into
	// antipodal halves. Keeping a tenth of the mean leaves every residual a
	// shared component to agree on, which costs little separation on a diverse
	// corpus and keeps a narrow one intact.
	//
	// 0 disables centring entirely, restoring raw cosine geometry — and with
	// it the chaining that collapses a corpus into one story. Raising it toward
	// 1.0 sharpens separation between genuinely distinct topics at the cost of
	// stability on narrow corpora.
	MeanRemoval float64

	// MinStorySize is the number of *distinct signals* a group must hold to
	// exist as a story (default: 3). It gates outlier promotion and both sides
	// of a split; a smaller group stays in the outlier bucket or stays put.
	//
	// It counts signals rather than facets on purpose: one signal decomposed
	// into MinStorySize facets is still one signal, and must not found a story
	// by itself.
	MinStorySize int

	// MaxFacetsPerSignal bounds the facets one signal may carry (default: 8).
	// Ingest cost is linear in facet count, split cost is quadratic in a
	// story's facet count, and the key schema encodes the index in four
	// digits — so the bound is real rather than defensive. Ingest returns
	// ErrTooManyFacets above it. Must be in [1, 9999].
	MaxFacetsPerSignal int

	// Draft-phase assignment.
	AssignmentK float64 // σ multiplier for per-story assignment radius (default: 2.0)

	// InitialSigmaGlobal is the σ_global stand-in used before a batch run has
	// measured one (default: 0.25). It is ignored once σ_global is seeded.
	//
	// Its reach is narrow. A story can only exist after a batch run, and a run
	// seeds σ_global, so the Draft phase almost never sees this value. What it
	// does decide is the admission radius during the very first run, for the
	// stories that run has just created.
	InitialSigmaGlobal float64

	// ColdStartMinSignals is how many members a story needs before its own σ is
	// trusted. Below it, the assignment radius is AssignmentK × σ_global
	// (default: 5).
	ColdStartMinSignals int

	// SigmaFloor floors a story's σ at this fraction of σ_global (default: 0.1),
	// so a story whose first signals are near-identical cannot collapse its own
	// assignment radius to zero. It applies after cold-start too.
	SigmaFloor float64

	// EMAAlpha is the weight given to the *previous* σ_global when a batch run
	// updates it:
	//
	//	σ_global ← EMAAlpha×σ_global + (1−EMAAlpha)×mean_distance_this_run
	//
	// The default 0.1 therefore tracks the latest run closely (90% of the new
	// measurement) rather than smoothing heavily. Raise it toward 1 to make
	// σ_global sluggish, lower it to make each run's measurement decisive.
	EMAAlpha float64

	// Concurrency.
	IngestBufferCap int // signals buffered in memory during batch Apply (default: 10_000)
	EventBufferSize int // per-subscriber channel buffer depth (default: 512)

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

	if c.Namespace == (uuid.UUID{}) {
		c.Namespace = TrackerNamespace
	}
	if c.BatchWindow == 0 {
		c.BatchWindow = 24 * time.Hour
	}
	if c.BatchSchedule == "" {
		c.BatchSchedule = "*/30 * * * *"
	}
	if _, err := cronParser.Parse(c.BatchSchedule); err != nil {
		return fmt.Errorf("story: Config.BatchSchedule is invalid %q: %w", c.BatchSchedule, err)
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
	if c.OutlierTTL == 0 {
		c.OutlierTTL = 2 * c.BatchWindow
	}
	if c.AssignThreshold == 0 {
		c.AssignThreshold = 0.50
	}
	if c.MergeThreshold == 0 {
		c.MergeThreshold = 0.40
	}
	if c.SplitThreshold == 0 {
		c.SplitThreshold = 0.55
	}
	if c.MeanRemoval == 0 {
		c.MeanRemoval = 0.9
	}
	if c.MinStorySize == 0 {
		c.MinStorySize = 3
	}
	if c.MaxFacetsPerSignal == 0 {
		c.MaxFacetsPerSignal = 8
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
	if c.IngestBufferCap == 0 {
		c.IngestBufferCap = 10_000
	}
	if c.EventBufferSize == 0 {
		c.EventBufferSize = 512
	}

	// Threshold coherence. Checked after defaulting so a caller who sets none
	// of them still gets a valid configuration.
	if c.AssignThreshold <= 0 || c.AssignThreshold > 1 {
		return fmt.Errorf("story: Config.AssignThreshold must be in (0, 1], got %g", c.AssignThreshold)
	}
	if c.MergeThreshold <= 0 {
		return fmt.Errorf("story: Config.MergeThreshold must be > 0, got %g", c.MergeThreshold)
	}
	if c.MergeThreshold >= c.AssignThreshold {
		return fmt.Errorf(
			"story: Config.MergeThreshold (%g) must be below AssignThreshold (%g): stories may not merge at a distance wider than a signal may sit from a centroid",
			c.MergeThreshold, c.AssignThreshold)
	}
	if c.SplitThreshold > 1 {
		return fmt.Errorf("story: Config.SplitThreshold must be <= 1, got %g", c.SplitThreshold)
	}
	if c.SplitThreshold <= c.MergeThreshold {
		return fmt.Errorf(
			"story: Config.SplitThreshold (%g) must exceed MergeThreshold (%g): without a hysteresis band a merge and a split undo each other along different seams",
			c.SplitThreshold, c.MergeThreshold)
	}
	if c.MeanRemoval < 0 || c.MeanRemoval > 1 {
		return fmt.Errorf("story: Config.MeanRemoval must be in [0, 1], got %g", c.MeanRemoval)
	}
	if c.MinStorySize < 2 {
		return fmt.Errorf("story: Config.MinStorySize must be >= 2, got %d", c.MinStorySize)
	}
	if c.MaxFacetsPerSignal < 1 || c.MaxFacetsPerSignal > keys.MaxFacet {
		return fmt.Errorf("story: Config.MaxFacetsPerSignal must be in [1, %d], got %d",
			keys.MaxFacet, c.MaxFacetsPerSignal)
	}
	return nil
}
