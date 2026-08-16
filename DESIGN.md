# Streaming Story Tracker — Design Document

## Overview

`go.kvsh.ch/streaming-story` is a Go library for ingesting a continuous stream of signals and grouping them into evolving stories. It uses a **Hybrid Clustering** approach:
1.  **Real-time Ingestion**: Immediate assignment to the nearest story for low-latency feedback (the "Draft" assignment).
2.  **Periodic Batch Re-clustering**: A background process that uses HDBSCAN to refine the story structure, identifying splits and merges based on statistical density rather than hardcoded distance thresholds.

`Ingest` is goroutine-safe. `Subscribe` channels are per-caller and independently buffered.

---

## Core Concepts

### Signal

The atomic unit of input. Defined as a generic type so the caller can attach arbitrary domain fields.

```go
type Signal[T any] struct {
    ID        uuid.UUID // UUID v5, provided by caller; see UUID Namespace below
    At        time.Time
    Embedding []float32 // dimensionality fixed on first Ingest call; mismatch returns error
    Data      T         // opaque caller payload
}
```

### Story

A persistent cluster of signals sharing semantic proximity.
- **Centroid**: The mean embedding of signals in the active window.
- **Story Radius**: The distance from the centroid to the furthest signal in the active window.
- **Stability**: Signals in **Active** stories are "liquid" and may be re-assigned to a different story if a batch run finds a better structural fit. Once a story is **Dormant** or **Archived**, its membership is locked (see exceptions in [Lifecycle Rules](#lifecycle-rules)).

### UUID Namespace

Signal IDs are UUID v5. Callers should derive them as:

```go
uuid.NewV5(TrackerNamespace, domainKey)
```

`TrackerNamespace` is a fixed UUID constant exported by the library:

```go
// TrackerNamespace is the root namespace for all Signal IDs.
// It is a fixed compile-time constant so IDs are stable regardless of
// deployment path, working directory, or how the store Dir is expressed.
var TrackerNamespace = uuid.MustParse("d4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f80")
```

Multi-tenant deployments that need namespace isolation per tenant can set `Config.Namespace`; if zero-value, `TrackerNamespace` is used. The `trackerDir`-based derivation that appeared in earlier drafts is **removed**: a path that changes (relative vs. absolute, symlink, container mount) would silently produce different IDs for the same `domainKey`.

---

## Tiered Window Strategy

The system maintains three temporal tiers to balance performance and semantic accuracy:

| Tier | Name | Size (Default) | Role |
|---|---|---|---|
| **Tier 1** | Ingestion | 1 Signal | Immediate "best-guess" assignment to the nearest existing story centroid. |
| **Tier 2** | Batch Window | Last `BatchWindow` (default: 24h) | Periodic re-clustering (HDBSCAN) on all signals received within the window to discover the "true" structure of the recent stream. |
| **Tier 3** | Active Context | Last 30 Days | All Active/Dormant story centroids used as anchors for mapping batch results. |

The batch window is time-bounded. The number of signals it contains varies with ingestion rate, which is taken into account when choosing HDBSCAN parameters (see [Batch Re-clustering](#2-batch-re-clustering-refinement-phase)).

---

## Story Lifecycle & Mapping

### States

```
Outlier ──► Active ──► Dormant ──► Archived (terminal)
                ▲          │
                └──────────┘
```

- **Outlier**: Signal has no story match yet; held pending the next batch run.
- **Active**: Story is receiving signals or was last seen within `SilenceWindow`.
- **Dormant**: No new signals for `SilenceWindow`. Membership locked. Centroid retained. Can reactivate.
- **Archived**: No new signals for `ArchiveWindow`. Signal data and centroid are **retained**. Terminal state — no reactivation. New signals on the same topic will form a fresh story.

### 1. Real-time Ingestion (Draft Phase)

When a signal arrives:
0.  **Locate any existing copy**: Consult the `l:{signalID}` location index. If the signal already belongs to a story — including one a batch run moved it to — the ingest is a strict no-op returning that story ID (see [Signal Location Index](#key-schema)).
1.  **Centroid Match**: Find the nearest **Active** or **Dormant** story centroid via Cosine Similarity.
2.  **Assignment**: Assign the signal to that story if the distance is within the per-story adaptive threshold:

    ```
    T_assign(story) = mean_distance(story) + AssignmentK × σ(story)
    ```

    where distances are from each signal in the active window to its story centroid.

    Before the first batch run completes there is no measured σ_global; `InitialSigmaGlobal` (default: 0.25) stands in until one is seeded. Every story is in cold-start at that point, so this single value determines whether early signals join a story or are held as outliers for the first batch to resolve.

    **σ_global** is the exponential moving average of per-signal centroid distances across all Active stories, updated at the end of each batch run:
    ```
    σ_global ← EMAAlpha × σ_global_prev + (1 − EMAAlpha) × mean_distance_all_active_stories
    ```
    with `EMAAlpha = 0.1` (configurable). It is persisted in `c:state` (see [Key Schema](#key-schema)) and bootstrapped from the first completed batch run that contains at least one Active story.

    Before a story accumulates `ColdStartMinSignals` signals (default: 5), the per-story σ is unreliable; fall back to `AssignmentK × σ_global`.

    **Tightness trap prevention**: To guard against new stories whose first few signals are nearly identical (driving σ(story) ≈ 0 and collapsing the threshold), σ(story) is floored at `SigmaFloor × σ_global` (default `SigmaFloor = 0.1`). This floor applies even after the cold-start period ends.

3.  **Outlier**: If no story is within threshold, write the signal to the outlier bucket (`o:{signalID}`) and hold it for the next batch run.

**Centroid currency**: Centroids are recalculated only at the end of each batch run. During the Draft phase the system uses the centroid from the last completed batch. For a `BatchInterval` of 30 minutes, assignment decisions may use a centroid up to 30 minutes stale. This lag is accepted: Draft assignments are explicitly provisional, and the next batch run corrects any misassignments.

**Dormant story thresholds**: The `T_assign` formula requires `mean_distance(story)` and `σ(story)`. For Dormant stories — which have zero signals in the current `BatchWindow` — these statistics are undefined from live data. To allow Dormant stories to participate in Draft assignment (and thus reactivate when semantically relevant signals arrive), `mean_distance` and `σ` are **frozen in story metadata on the Dormant transition** and reused for threshold calculation until the story becomes Active again. On reactivation (transition back to Active), the frozen values are **cleared from metadata** and the story re-enters the cold-start period: it uses `AssignmentK × σ_global` until it accumulates `ColdStartMinSignals` signals, at which point live per-story statistics take over. This prevents a story that reactivated around a different topic distribution from inheriting stale thresholds.

### 2. Batch Re-clustering (Refinement Phase)

Triggered when either condition fires first:
- `BatchInterval` has elapsed since the last run (default: 30m).

Steps:
1.  **Collect**: Gather all signals with `At ≥ now − BatchWindow`, plus all outlier signals with `At ≥ lastBatchTimestamp − OutlierTTL`. Outlier signals older than that threshold are evicted (deleted from the `o:` prefix) and dropped — they have failed to cluster across enough successive batch runs that retaining them would only grow the outlier bucket indefinitely. Using `lastBatchTimestamp` rather than wall-clock `now` as the reference point prevents mass eviction after a maintenance pause: if the system is offline for an extended period, `lastBatchTimestamp` does not advance, so outliers are not penalised for time the batch goroutine was not running.
2.  **Parameterise HDBSCAN**:
    - Use the configured `MinClusterSize` (default: 3) directly. It is a **fixed constant**, not derived from window population. A dynamic formula risks blinding the algorithm to small-but-distinct stories that may have few but high-value signals.
    - `MinSamples = MinClusterSize` (configurable).
    - **Cluster extraction**: `ClusterSelection` chooses how clusters are read out of the condensed cluster tree.
      - `ClusterSelectionEOM` (default) is excess of mass, the method in Campello et al.: a node is selected when its own stability is at least the summed stability of its descendants.
      - `ClusterSelectionLeaf` selects every leaf of the condensed tree instead.

      EOM favours breadth, and that is a hazard for story tracking. A large, diffuse, long-lived region accumulates stability across the whole λ range it survives, which can exceed the summed stability of the tighter clusters nested inside it. The parent is then selected and every distinct story within it is labelled as one cluster — the apply phase subsequently moves each of those stories' window signals into the single winning story and retires the ones left empty. Raising `MinClusterSize` does **not** counteract this: density pruning happens earlier, during condensation, and a broad region that is genuinely dense only becomes more stable.

      Leaf extraction yields more, smaller, more homogeneous clusters and never collapses nested clusters into their parent. The cost is that a genuinely broad story may be split across several stories, which Phase 2 merge detection can reunite on a later run. Unlike EOM, leaf extraction applies no stability floor: a leaf with zero stability is one whose points all fall out at its own birth λ, which is precisely the tightest grouping in the data.
    - **`MaxClusterSize`** (default: 0, unlimited) rejects any candidate cluster holding more points, forcing extraction to descend into its children. It applies to EOM only — leaf extraction has no larger candidate to fall back from. An over-sized leaf under EOM has no children to descend into, so its points become noise: the cap trades coverage for cohesion.
    - **Sampling**: If `len(signals) > BatchSampleCap` (default: 50,000), apply stratified reservoir sampling down to `BatchSampleCap` using the following two-pass allocation:
      1. **Guaranteed minimum pass**: Compute `totalGuarantee = numActiveStories × MinClusterSize`. If `totalGuarantee ≤ SampleGuaranteeMaxFraction × BatchSampleCap` (default `SampleGuaranteeMaxFraction = 0.5`), reserve exactly `MinClusterSize` signals per Active story (drawn in reverse-chronological order). If the budget is exceeded, scale the per-story reservation down proportionally to `floor(BatchSampleCap × SampleGuaranteeMaxFraction / numActiveStories)`, with a minimum of 1 per story that has any signals in the window. This ensures the guarantee never consumes more than half of `BatchSampleCap`, leaving room for new signal discovery.
      2. **Proportional pass**: Fill the remaining capacity (`BatchSampleCap − guaranteed slots used`) proportionally across all stories by their signal count, topping up stories that received fewer than their proportional share in pass 1.
      This prevents small but active stories from falling below the viable cluster density while also preserving capacity for emerging signals. It bounds HDBSCAN's $O(N \log N)$ cost and prevents a run from exceeding `BatchInterval`, which would cause a backlog.
3.  **Re-cluster**: Run HDBSCAN on the collected signals.
4.  **Map**: Match new batch clusters ($C_B$) to persistent stories ($S_P$) using Jaccard overlap and the Hungarian algorithm (see [Cluster Mapping](#cluster-mapping)).
5.  **Apply**: Persist the resulting updates (1-to-1, merge, split, new story) and emit events.
6.  **Promote outliers**: Any outlier signal that ended up in a batch cluster is migrated from `o:{signalID}` to `s:{storyID}:s:{signalID}`.

### Cluster Mapping

Mapping is a **two-phase** process. The Hungarian algorithm finds optimal 1-to-1 continuity links; a separate post-assignment scan detects splits and merges that a strict 1-to-1 constraint would suppress.

For each pair $(C_B, S_P)$ compute:

```
Jaccard(C_B, S_P) = |signals(C_B) ∩ signals(S_P)| / |signals(C_B) ∪ signals(S_P)|
```

Both sets are **restricted to signals within the `BatchWindow`** — the same signal population fed into HDBSCAN. Using `S_P`'s full lifetime signal set would make `|C_B ∪ S_P|` very large for mature stories (thousands of signals vs. a handful in the current window), driving Jaccard near zero and causing valid continuity links to fail the `MappingMinJaccard` check. Concretely, `signals(S_P)` is the set of signals whose key is `s:{storyID}:s:*` **and** whose `At ≥ now − BatchWindow`, including Draft-assigned signals from the current window. A brand-new story created by a Draft assignment just before the batch run will therefore have its current-window signals included in the Jaccard calculation, giving it a fair chance at a continuity match.

**Phase 1 — Primary Continuity (Hungarian assignment)**

Build a cost matrix (cost = 1 − Jaccard) restricted to pairs where Jaccard ≥ `MappingMinJaccard` (default: 0.6). Solve with the Hungarian algorithm to find the optimal 1-to-1 pairing — each $C_B$ is paired with at most one $S_P$ and vice versa. Pairs below `MappingMinJaccard` are left unmatched by construction.

| Phase 1 outcome | Action |
|---|---|
| Matched pair | **Update** $S_P$: recalculate centroid and radius. |

**Phase 2 — Split and Merge Detection (post-assignment Jaccard scan)**

After Phase 1, scan the full Jaccard matrix for secondary overlaps that the 1-to-1 constraint suppressed. In a split, the Hungarian algorithm assigns $S_P$ to its strongest match $C_B$ and leaves the weaker child $C_B'$ unmatched; in a merge, it assigns $C_B$ to its strongest match $S_P$ and leaves $S_P'$ unmatched.

Phase 2 operates over the **full unmatched set**, not just individual pairs, enabling N-way outcomes:
- **N-way split**: A single $S_P$ may overlap with multiple unmatched batch clusters $C_{B1}', C_{B2}', \ldots$ — each child that independently satisfies the conditions below is promoted as a separate new story.
- **N-way merge**: A single matched $C_B$ may overlap with multiple unmatched persistent stories $S_{P1}', S_{P2}', \ldots$ — all qualifying stories are merged into $S_P$ simultaneously, retaining the earliest creation time across all merged stories.

| Phase 2 condition | Action |
|---|---|
| Unmatched $C_B'$ with Jaccard($C_B'$, $S_P$) ≥ `SplitMinJaccard` (default: 0.3) for an already-matched $S_P$, and combined coverage of $C_B$ + $C_B'$ over $S_P$ > 0.7 | **Split** $S_P$ (only if Active): retain $S_P$ for $C_B$, promote $C_B'$ as a new story. For N-way splits, each qualifying unmatched cluster produces a separate new story; the condition is evaluated independently per child. |
| Unmatched $S_P'$ with Jaccard($C_B$, $S_P'$) ≥ `SplitMinJaccard` for an already-matched $C_B$, and combined coverage of $S_P$ + $S_P'$ from $C_B$ > 0.7 | **Merge** $S_P'$ into $S_P$. The **surviving StoryID is that of the story with the earliest creation time** across all merging stories; if $S_P'$ is older than $S_P$, then $S_P'$ becomes the survivor and $S_P$ is retired instead — the convention is consistent: the oldest story's identity persists. A **merge is a key-space migration**: all signal keys are moved from the retired story's prefix to the surviving story's prefix, including signals older than `BatchWindow`. This is an identity-level operation and is **exempt from the re-assignment stability rule** — that rule governs batch-derived label changes, not story consolidation. |
| $C_B$ still unmatched after both phases, signal count ≥ `MinClusterSize` | **Create** a new persistent story. |
| $C_B$ still unmatched after both phases, signal count < `MinClusterSize` | Retain signals as outliers. |

### 3. Stability & Re-assignment

If a batch run determines that a signal previously assigned to Story A belongs in Story B, the `Tracker` updates the persistence layer and emits `EventSignalReassigned`. Re-assignment is limited to signals within the current **`BatchWindow`**: only signals that were fed into the HDBSCAN run have a batch-derived label and are therefore eligible for re-assignment. Signals older than `BatchWindow` were not part of the clustering calculation and must not be moved — doing so would produce a re-assignment decision with no algorithmic basis.

The `StabilityWindow` config field is **removed**; `BatchWindow` is the authoritative and consistent scope for re-assignment.

**Emptied stories are retired.** When a batch run reassigns (or demotes) every signal of a persistent story — for example a story created from a premature partial merge whose signals are later reclustered into separate stories — the story is left with no data under its `s:{storyID}:` prefix. The Apply phase deletes such a story's metadata and time-index entry, increments `StoriesRetired`, and emits `EventStoryRetired`. A story that retains any historical signals outside `BatchWindow` is untouched: those signals were never part of the clustering input, so the retirement scan sees them and the story survives.

### Lifecycle Rules

- A **Dormant** story may be the *target* of a merge (absorbing an active story reactivates it) but may not be *split*. If a split is detected on a Dormant story, the split is suppressed: the story remains intact and the diverging signals are promoted as a new story instead.
- **Archived** stories are excluded from cluster mapping entirely. The batch Apply phase skips them when building the $S_P$ candidate set; any new signals on the same topic will form a new story via the unmatched-$C_B$ path.
- **Noise retention**: If a signal already assigned to a story is labeled as Noise (label −1) by a batch run, it is **retained** in its current story and not evicted to the outlier bucket. HDBSCAN's noise label reflects local density insufficiency, not evidence of a better cluster. Evicting such signals would produce "flickering" — the same signal repeatedly entering and leaving a story across successive batch runs.

---

## Persistence: Prefix-based KV Store

The library interacts with a minimal `Store` interface.

### Key Schema

| Purpose | Key Pattern | Value |
|---|---|---|
| Calibrator State | `c:state` | JSON: $\sigma_{global}$, dimensionality, last batch timestamp |
| Story Metadata | `s:{storyID}:m` | JSON: centroid, radius, state, timestamps, frozen_mean_distance, frozen_sigma |
| Signal Data | `s:{storyID}:s:{signalID}` | Encoded `Signal[T]` |
| Outlier Signal | `o:{signalID}` | Encoded `Signal[T]` |
| Signal Location Index | `l:{signalID}` | `s:{storyID}` (story member) or `o` (outlier) |
| Story Time Index | `t:{unix_sec}:{storyID}` | empty |

**Story Time Index**: Written/updated on every story metadata write. Deleted and re-inserted on each update so the timestamp stays current. A range scan from `t:{cutoff}:` to `t:{now}:` efficiently retrieves all recently active stories for Tier 3 without a full metadata scan.

**Signal Location Index**: Written when a signal is stored and updated by every batch move (including merge migrations), and deleted on outlier eviction. `Ingest` consults it before the centroid match: a copy that lives in *any* story is a strict no-op, returning the stored story ID. This closes a duplicate-creation window where a signal ingested once, then re-moved to a different story by a batch run, would be re-ingested into a second copy under the nearest story.

**Signal retention**: Signal data is retained for all story states, including Archived. No signal keys are deleted on archival; only the metadata state field changes.

**Deletion/Merge**: Performed using range deletes on the `s:{storyID}:` prefix.

---

## Concurrency Model

### Ingest vs. Batch Apply

The underlying KV store is assumed to permit only one write transaction at a time. A Batch `Apply` phase that rewrites thousands of signal keys (moving signals between stories, updating indices) will block all concurrent `Ingest` calls for its full duration, causing latency spikes proportional to batch size.

**Strategy — In-memory Ingest Buffer during Apply**

1. When the Batch goroutine begins its `Apply` phase it sets an atomic `applyInProgress` flag.
2. `Ingest` calls that arrive while `applyInProgress` is set write their signals to an in-memory staging channel (`ingestBuffer`, bounded to `IngestBufferCap` signals, default: 10,000) instead of directly to the KV store. The caller still receives a provisional story ID, computed against an in-memory snapshot of the story metadata the batch collected. The lookup deliberately does not read the KV store: the `Store` interface does not require `View` to run concurrently with `Update`, so on a single-lock backend a store read here would block the caller for the whole Apply.
3. Once the `Apply` transaction commits, the batch goroutine drains `ingestBuffer` into the store in a follow-up write transaction before clearing `applyInProgress`.
4. If `ingestBuffer` is full, `Ingest` blocks until space is available (or `ctx` is cancelled). This provides natural back-pressure without data loss.

Ordering guarantee: all signals from the batch window precede all buffer-drain signals in the KV store, which is consistent with their temporal ordering and means Draft assignments made against the staged signals remain valid.

**Crash semantics (at-most-once)**: The `ingestBuffer` is in-memory and not persisted. A process crash after the Apply transaction commits but before the buffer-drain write completes will lose all buffered signals. This is an explicit **at-most-once** delivery guarantee for signals received during Apply. Callers requiring stronger guarantees should implement their own write-ahead log or rely on idempotent re-ingestion: UUID v5 signal IDs ensure that re-ingesting the same signal is a KV-level no-op, though `EventDraftAssigned` will not be re-emitted for already-stored signals.

---

## Public API

> **Default tuning**: values are calibrated for low-to-medium frequency news ingestion (1–10 signals/day per topic). High-frequency sources (social media, metrics) should reduce `BatchWindow` (e.g. 30m), `BatchInterval` (e.g. 5m), `SilenceWindow` (e.g. 6h), `ArchiveWindow` (e.g. 7d), and raise `MinClusterSize` (e.g. 5–10) accordingly.

```go
type Config[T any] struct {
    Dir              string
    Namespace        uuid.UUID     // UUID v5 namespace root for Signal IDs; zero → TrackerNamespace
    BatchWindow      time.Duration // Time span of signals fed to each batch run (default: 24h)
    BatchInterval    time.Duration // Run batch every this duration (default: 30m)
    SilenceWindow    time.Duration // Active -> Dormant (default: 7d)
    ArchiveWindow    time.Duration // Dormant -> Archived (default: 30d)
    // StabilityWindow removed: re-assignment scope is BatchWindow (see Stability & Re-assignment).
    BatchSampleCap              int           // Max signals fed to HDBSCAN per run; excess is reservoir-sampled (default: 50_000)
    SampleGuaranteeMaxFraction  float64       // Max fraction of BatchSampleCap reserved for per-story minimums; remainder is proportional (default: 0.5)
    OutlierTTL                  time.Duration // Max age of an outlier signal (relative to last batch run) before eviction (default: 2 × BatchWindow)
    MinClusterSize   int           // HDBSCAN min points to form a cluster; fixed constant (default: 3)
    MinSamples       int           // HDBSCAN core-point density (default: MinClusterSize)
    AssignmentK      float64       // σ multiplier for per-story assignment radius (default: 2.0)
    InitialSigmaGlobal float64     // σ_global stand-in before the first batch run measures one (default: 0.25)
    ColdStartMinSignals int        // Signals needed before per-story σ is trusted; uses σ_global below (default: 5)
    SigmaFloor       float64       // Floor for per-story σ as a fraction of σ_global (default: 0.1)
    EMAAlpha         float64       // EMA decay for σ_global updates (default: 0.1)
    MappingMinJaccard float64      // Jaccard threshold for primary cluster continuation (default: 0.6)
    SplitMinJaccard  float64       // Jaccard threshold for secondary split/merge detection (default: 0.3)
    IngestBufferCap  int           // Max signals buffered in memory during batch Apply (default: 10_000)
    EventBufferSize  int           // Per-subscriber channel buffer depth (default: 512)
    Codec            Codec[T]
}

// NewTracker opens (or creates) the store and initializes the tiered windows.
func NewTracker[T any](cfg Config[T]) (*Tracker[T], error)

// Ingest processes a signal and returns its initial (draft) StoryID.
// Returns ErrDimensionMismatch if the embedding length differs from the first ingested signal.
// Goroutine-safe.
func (t *Tracker[T]) Ingest(ctx context.Context, sig Signal[T]) (storyID uuid.UUID, err error)

// Subscribe returns a channel of real-time and batch-refined events.
// Events are dropped (and EventBufferOverflow emitted) if the channel fills.
// Each call returns an independent channel.
func (t *Tracker[T]) Subscribe() <-chan StoryEvent[T]

// Close flushes any pending batch run and closes the store.
func (t *Tracker[T]) Close() error

// Story returns current metadata for a single story.
func (t *Tracker[T]) Story(id uuid.UUID) (StoryMeta, error)

// Stories iterates over stories in the given state. Pass StoryStateAny to iterate all.
func (t *Tracker[T]) Stories(state StoryState) iter.Seq[StoryMeta]

// SignalsOf iterates over all signals for a story across all states.
// Signal data is retained through archival, so Archived stories are fully iterable.
func (t *Tracker[T]) SignalsOf(storyID uuid.UUID) iter.Seq2[Signal[T], error]
```

### Events

```go
type EventKind uint8

const (
    EventDraftAssigned    EventKind = iota // real-time: signal -> story (may change)
    EventSignalReassigned                  // batch: signal moved to a different story
    EventStoryCreated                      // new story persisted after batch run
    EventStorySplit                        // one story became two
    EventStoryMerged                       // two stories became one (StoryID2 is the retired ID)
    EventStoryRetired                      // batch emptied the story; its record was deleted
    EventStoryDormant                      // story crossed SilenceWindow
    EventStoryArchived                     // story crossed ArchiveWindow; membership locked, signals retained
    EventBatchComplete                     // one per batch run; Count fields summarise the run (see StoryEvent)
    EventBufferOverflow                    // subscriber channel full; event(s) were dropped
)

type StoryEvent[T any] struct {
    Kind     EventKind
    StoryID  uuid.UUID // primary story
    StoryID2 uuid.UUID // secondary: merged-away story (Merged) or new child story (Split)
    SignalID uuid.UUID // set for per-signal events (DraftAssigned, SignalReassigned)
    At       time.Time
    // BatchSummary is populated only for EventBatchComplete.
    BatchSummary *BatchSummary
}

// BatchSummary carries aggregate counts for a completed batch run.
// Subscribers that cannot handle high per-signal event rates (e.g. EventBufferSize = 512)
// should consume EventBatchComplete for coarse-grained progress and selectively query
// the store for details, rather than relying on individual EventSignalReassigned events.
// An N-way merge of K stories or a batch run that reassigns thousands of signals will
// emit K EventStoryMerged + one EventBatchComplete; the per-signal EventSignalReassigned
// events for that batch are emitted before EventBatchComplete and may trigger
// EventBufferOverflow on slow subscribers. Sizing EventBufferSize to at least
// BatchSampleCap / average_signals_per_story avoids overflow under normal conditions.
type BatchSummary struct {
    StoriesCreated    int
    StoriesMerged     int
    StoriesSplit      int
    StoriesRetired    int
    SignalsReassigned int
    OutliersEvicted   int
    OutliersPromoted  int
}
```

---

## Non-Goals
- Distributed operation (single-node, embedded).
- Approximate Nearest Neighbor (ANN) indexing (brute-force centroids is $O(stories)$ and fast enough for the expected story count).
- Built-in embedding generation (caller provided).
