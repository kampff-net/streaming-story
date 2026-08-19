package story

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// TrackerNamespace is the root namespace for all Signal IDs.
// It is a fixed compile-time constant so IDs are stable regardless of
// deployment path, working directory, or how the store is configured.
var TrackerNamespace = uuid.MustParse("d4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f80")

// ErrDimensionMismatch is returned by Ingest when the signal's embedding
// length differs from the dimensionality established by the first ingested signal.
var ErrDimensionMismatch = errors.New("story: embedding dimension mismatch")

// ErrZeroEmbedding is returned by Ingest when a signal carries an embedding
// with zero magnitude.
var ErrZeroEmbedding = errors.New("story: zero embedding")

// ErrTooManyFacets is returned by Ingest when a signal carries more facets
// than Config.MaxFacetsPerSignal permits.
var ErrTooManyFacets = errors.New("story: too many facets")

// ErrNotFound is returned when a requested story does not exist in the store.
var ErrNotFound = errors.New("story: not found")

// StoryState is the lifecycle state of a story.
type StoryState uint8

const (
	// StoryStateAny is a sentinel for Stories() — iterates all states.
	StoryStateAny StoryState = iota

	StoryStateActive   // receiving signals or within SilenceWindow
	StoryStateDormant  // no signals for SilenceWindow; membership locked; can reactivate
	StoryStateArchived // no signals for ArchiveWindow; terminal; signals retained
)

// Embedding is one facet's vector. It is an alias rather than a defined type:
// every vector in this library and in its callers is already []float32, and a
// defined type would force a conversion at each of those boundaries while
// adding nothing a conversion could catch. The alias names the concept —
// [][]float32 does not say what its outer dimension means — and stays freely
// interchangeable with []float32, so internal/geom, internal/dist, and
// internal/cluster accept one without an edit.
type Embedding = []float32

// Signal is the atomic unit of input.
type Signal[T any] struct {
	ID uuid.UUID `cbor:"0,keyasint"`
	At time.Time `cbor:"1,keyasint"`

	// Embeddings holds one Embedding per facet. Embeddings are unit-normalized
	// upon ingest and stored as unit vectors. Lossless for replay; direction
	// is preserved exactly, magnitude is not.
	Embeddings []Embedding `cbor:"2,keyasint"`

	Data T `cbor:"3,keyasint"`
}

// StoryMeta holds the current metadata for a persistent story.
type StoryMeta struct {
	ID    uuid.UUID
	State StoryState

	// Centroid is the unweighted mean of every member's embedding. It is the
	// story's identity geometry: merge, split, radius, and sigma are all
	// measured against it, so it must not chase recent traffic.
	Centroid []float32

	// RecentCentroid is the unweighted mean of members within
	// ActiveContextWindow, and is what the Draft phase compares an arriving
	// signal against. A developing story therefore keeps admitting current
	// coverage even as Centroid stays anchored on its whole history. Stories
	// with no recent members carry a copy of Centroid.
	RecentCentroid []float32

	Radius       float64
	CreatedAt    time.Time
	LastSignalAt time.Time

	// MeanDistance and Sigma are the live per-story statistics computed at
	// the last batch run from the story's BatchWindow signals: the mean and
	// population standard deviation of the distances from each window signal
	// to the story centroid. They are meaningful only when SignalCount is at
	// least ColdStartMinSignals; below that the Draft phase falls back to
	// σ_global. They are cleared when the story goes Dormant.
	MeanDistance float64
	Sigma        float64
	SignalCount  int

	// FrozenMeanDistance and FrozenSigma are captured on the Dormant
	// transition and used for Draft-phase threshold calculation until
	// reactivation, at which point they are cleared.
	FrozenMeanDistance float64
	FrozenSigma        float64
	ReactivatedAt      time.Time
	StatsAt            time.Time
}

// EventKind identifies the type of a StoryEvent.
type EventKind uint8

const (
	EventDraftAssigned    EventKind = iota // real-time: signal provisionally assigned to story
	EventSignalReassigned                  // batch: signal moved to a different story
	EventStoryCreated                      // new story persisted after batch run
	EventStorySplit                        // one story split into two
	EventStoryMerged                       // two stories merged; StoryID2 is the retired ID
	EventStoryRetired                      // batch emptied the story; its record was deleted
	EventStoryDormant                      // story crossed SilenceWindow
	EventStoryArchived                     // story crossed ArchiveWindow
	EventBatchComplete                     // one per batch run; BatchSummary is populated
	EventBufferOverflow                    // subscriber channel full; events were dropped
)

// StoryEvent is emitted to subscribers on every story state change and
// per-signal assignment decision.
type StoryEvent[T any] struct {
	Kind     EventKind
	StoryID  uuid.UUID // primary story
	StoryID2 uuid.UUID // merged-away story (Merged) or new child story (Split)
	SignalID uuid.UUID // set for per-signal events (DraftAssigned, SignalReassigned)
	At       time.Time

	// BatchSummary is populated only for EventBatchComplete.
	BatchSummary *BatchSummary
}

// BatchSummary carries aggregate counts for a completed batch run.
//
// Subscribers that cannot handle high per-signal event rates should consume
// EventBatchComplete for coarse-grained progress and query the store for
// details rather than relying on individual EventSignalReassigned events.
// The four story-scoped counters count stories. The rest count *facets*, which
// is the unit maintenance operates on: a two-facet signal admitted into two
// stories advances OutliersAdmitted by two. Callers reporting on signals should
// derive that from the events, which are per (signal, story).
type BatchSummary struct {
	StoriesCreated int
	StoriesMerged  int
	StoriesSplit   int
	StoriesRetired int

	SignalsReassigned          int
	OutliersEvicted            int
	OutliersPromoted           int
	OutliersAdmitted           int
	DimensionMismatchesDropped int
}
