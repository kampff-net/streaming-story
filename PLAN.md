# Implementation Plan: Streaming Story Tracker

This plan outlines the steps required to finalize the `go.kvsh.ch/streaming-story` library according to the specifications in `DESIGN.md`.

## Phase 1: Core Infrastructure

- [x] **Core Types**: `Signal`, `StoryMeta`, `StoryEvent`, `StoryState` defined.
- [x] **Config**: Full configuration with validation and defaults implemented.
- [x] **UUID Namespace**: Fixed `TrackerNamespace` and configuration-based override implemented.
- [x] **Key Schema**: KV key generation helpers for all prefixes implemented.
- [x] **Persistence Models**: Internal `storyRecord` and `calibState` defined.
- [x] **Store Interface**: Generic `Store` and `Tx` interfaces defined.
- [x] **In-memory Mock Store**: `MemStore` implemented for testing.
- [x] **Algorithms**: `hdbscan` and `hungarian` algorithms implemented with tests.
- [x] **Tracker Lifecycle**: `NewTracker`, `Close`, and subscriber management implemented.
- [x] **Optimized Vector Math**: `gonum.org/v1/gonum` integrated for SIMD-accelerated `float32` operations (via `blas32`) in `internal/dist`.

## Phase 2: Draft Phase (Real-time Ingestion)

- [x] **Distance Metrics**: `internal/dist` provides `CosineDistance`/`CosineSimilarity` via optimized BLAS.
- [x] **Story Selection**: `findNearestStory` implemented.
  - [x] Uses the `t:{unix_sec}:{storyID}` index to find Tier 3 Active Context stories (bounded by `ActiveContextWindow`).
  - [x] Calculates distances to all Active/Dormant story centroids within the context window.
- [x] **Thresholding**: `calcThreshold(story StoryMeta) float64` implemented.
  - [x] Uses $T_{assign}(story) = mean\_distance(story) + AssignmentK \times \sigma(story)$.
  - [x] Handles **Cold Start**: uses `AssignmentK * sigmaGlobal` if story signals < `ColdStartMinSignals`.
  - [x] Handles **Sigma Floor**: floors $\sigma(story)$ at `SigmaFloor * sigmaGlobal`.
  - [x] Handles **Dormant Stories**: uses `FrozenMeanDistance` and `FrozenSigma`.
- [x] **Ingest Logic**:
  - [x] Establish/validate dimensionality.
  - [x] Perform nearest story lookup.
  - [x] If `applyInProgress`, write to `ingestBuffer`.
  - [x] Else, write to store (signal and updated story metadata).
  - [x] Emit `EventDraftAssigned`.
  - [x] Idempotent re-ingestion: no duplicate emit or storage for re-delivered signals.

## Phase 3: Refinement Phase (Batch Processing)

- [x] **Batch Loop**: `batchLoop` and `runBatch` implemented.
- [x] **Collection & Sampling**:
  - [x] Collect signals from `BatchWindow`.
  - [x] Collect outliers within `OutlierTTL` (anchored to `lastBatch`).
  - [x] Implement two-pass stratified sampling down to `BatchSampleCap`.
- [x] **HDBSCAN Run**: `internal/hdbscan` integrated with collected signals.
- [x] **Cluster Mapping (Phase 1)**:
  - [x] Build Jaccard cost matrix (cost = 1 - Jaccard), restricted to `BatchWindow` signals.
  - [x] Run Hungarian algorithm (`internal/hungarian`).
- [x] **Cluster Mapping (Phase 2)**:
  - [x] Detect splits (N-way) for unmatched batch clusters.
  - [x] Detect merges (N-way) for unmatched persistent stories.
  - [x] Rule: Oldest `StoryID` survives.
- [x] **Apply Phase**:
  - [x] Set `applyInProgress` flag during Apply.
  - [x] Persist all updates in a single `Update` transaction.
    - [x] Update story centroids and radii.
    - [x] Migrate re-assigned signals (within `BatchWindow`).
    - [x] Migrate merged story signals (key-space migration).
    - [x] Create new stories for unmatched clusters.
    - [x] Promote outliers to stories.
    - [x] Evict stale outliers.
    - [x] Transition stories to Dormant/Archived based on `SilenceWindow`/`ArchiveWindow`.
  - [x] Update `sigmaGlobal` using EMA.
  - [x] Clear `applyInProgress` and drain `ingestBuffer`.
  - [x] Emit `EventBatchComplete` and all change events.

## Phase 4: Iterators & Public API

- [x] **Go 1.22 Iterators**:
  - [x] Implement `Stories(state StoryState) iter.Seq[StoryMeta]`.
  - [x] Implement `SignalsOf(storyID uuid.UUID) iter.Seq2[Signal[T], error]`.
- [x] **Story Lookup**: `Story(id uuid.UUID)` implemented.

## Phase 5: Verification & Testing

- [x] **Unit Tests**:
  - [x] Test distance metrics.
  - [x] Test sampling logic.
  - [x] Test cluster mapping (Hungarian + Phase 2 splits/merges).
- [x] **Integration Tests**:
  - [x] Full Ingest -> Batch cycle.
  - [x] Story lifecycle transitions.
  - [x] Signal re-assignment validation.
  - [x] Outlier TTL eviction.
- [ ] **Benchmarks**:
  - [ ] Ingest latency during Batch Apply (buffer behavior).
  - [ ] Batch performance with `BatchSampleCap` signals.
