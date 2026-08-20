# Streaming Story Tracker — Design Document

## Overview

`go.kvsh.ch/streaming-story` ingests a continuous stream of embedding vectors
("signals") and groups them into evolving clusters ("stories"). Clustering is
hybrid:

1. **Draft phase** — real time, per signal. Each arriving signal is assigned to
   the nearest story centroid if it falls inside that story's adaptive radius,
   and bucketed as an outlier otherwise.
2. **Maintenance phase** — periodic, in the background. Existing stories are
   *maintained*, not re-derived: evict, promote, admit, split, merge, recentre,
   lifecycle. Story identity therefore survives every run.

`Ingest` is goroutine-safe. `Subscribe` channels are per-caller and
independently buffered.

Approaches that were tried and removed — HDBSCAN, Jaccard/Hungarian mapping,
sampling, raw cosine geometry, promotion cliques — are documented in
[`HISTORY.md`](HISTORY.md). This document describes only current behaviour.

## Code Layout

The public API and everything that touches the store live in the root `story`
package. The logic that can be stated without either lives in `internal/`:

- **`internal/geom`** — the corpus mean, the projector that centres against it,
  group statistics, and the quadratic angular bound. See
  [Geometry](#geometry-centred-space).
- **`internal/cluster`** — the grouping decisions, over an index-based `Point`
  type: growth, cliques, split, merge planning. Pure functions of points and
  thresholds; no store, no clock, no `Config`. See
  [Maintenance Pass](#maintenance-pass).
- **`internal/keys`** — the KV key schema and its parsers. See
  [Key Schema](#key-schema).
- **`internal/dist`** — cosine distance over BLAS.

Root files are grouped by function, not by type: `types.go` (public types and
events), `config.go` (knobs and validation), `store.go` (the `Store`/`Tx`
contracts plus `MemStore`), `cbor.go` (canonical CBOR encoding), `tracker.go` (lifecycle, batch loop,
subscriber fan-out), `ingest.go` (the Draft path), `threshold.go` (admission
radius policy, shared by Draft and by outlier admission), `batch.go` (collection,
apply window, snapshot, buffer drain), `maintain.go` (the pass itself),
`points.go` (the collected-signal form and every conversion into the `geom` /
`cluster` views), `record.go` (persisted shapes, their store access, and
calibration state), and `query.go` (the read API).

---

## Core Concepts

### Signal

The atomic unit of input, generic over a caller payload.

```go
type Embedding = []float32 // alias: names the concept, converts nowhere

type Signal[T any] struct {
    ID         uuid.UUID   // UUID v5; see UUID Namespace
    At         time.Time
    Embeddings []Embedding // one per facet; dimensionality fixed by the first Ingest, and shared by every facet
    Data       T           // opaque caller payload
}
```

**Facets.** A signal carries one vector per *facet* — one semantically distinct
component of the item. A facet, not a signal, is the unit of assignment and of
geometry: it is compared to centroids, admitted or held as an outlier, and
counted into a centroid, radius, and σ. A facet belongs to at most one story,
and a signal belongs to the union of its facets' stories, which is what makes
membership many-to-many.

The library never creates, reorders, merges, or drops a facet — decomposition
is the producer's judgment, and it depends on the item's own structure rather
than on corpus geometry. Facet order is significant and stable: facet `i` is
`Embeddings[i]`, and that index is its persistent identity in the store.

Sizes are counted in **distinct signals**, never facets: one signal split into
`MinStorySize` facets is still one signal and cannot found a story alone.

### Story

A persistent group of semantically proximate signals. Its metadata carries two
centroids, its geometry, its lifecycle state, and the statistics the Draft phase
needs.

Two centroids, because identity and admission are different questions:

- **`Centroid`** — unweighted mean of **every** member. This is the identity
  geometry: merge, split, radius, and σ all measure against it, so it must not
  chase recent traffic.
- **`RecentCentroid`** — unweighted mean of members within
  `ActiveContextWindow`. This is what admission compares against, so a
  developing story keeps admitting its own current coverage while its identity
  stays anchored on its whole history. Stories with no recent members carry a
  copy of `Centroid`.

Both are **recomputed from members on every run, never accumulated.** A running
mean or EMA depends on arrival order and elapsed run count, so two stores
holding identical signals would carry different centroids, and every threshold
comparison downstream would differ with them.

**`Radius`** is the distance from `Centroid` to the furthest member.
**`MeanDistance`** and **`Sigma`** are the mean and population standard
deviation of member distances to `Centroid`, meaningful only once the story
holds `ColdStartMinSignals` members.

### UUID Namespace

Signal IDs are UUID v5 and are supplied by the caller. Derive them with the
tracker, which honours a configured namespace:

```go
id := tracker.SignalID(domainKey)
```

Equivalent to `uuid.NewSHA1(cfg.Namespace, []byte(domainKey))`.
`Config.Namespace` defaults to `TrackerNamespace`, a fixed compile-time
constant, so IDs are stable regardless of deployment path or store location.
Multi-tenant deployments needing isolation set `Config.Namespace` per tenant.

Story IDs are UUID v5 under the same namespace, derived by the maintenance pass
rather than supplied: the name is the sorted list of the signal IDs the story
was founded on — the promoted outlier group, or the signals moving into a split
child — prefixed with the birth route, `promote` or `split:{parentID}`. Replaying
a signal stream against a fresh store therefore reproduces the same story IDs,
so a recorded run can be diffed against a replay.

An ID already held by another story is rederived with an incrementing salt. A
split can spawn exactly the member set some existing story was founded on, and
reusing that ID would silently fold the two together; the salt is part of the
derivation, so a replay meets the same occupied IDs in the same order.

Deterministic IDs make re-ingestion idempotent: a signal already stored under a
story is a strict no-op.

---

## Geometry: Centred Space

**Every distance in the library is measured in centred space, and every
threshold in `Config` is a centred-space distance.**

Before any comparison, an embedding is unit-normalized and has
`MeanRemoval` × corpus-mean subtracted:

```
project(x) = unit(x) − MeanRemoval · mean
```

The result is not renormalized — cosine distance is scale-invariant, so the
residual's length changes nothing.

**Why.** Text embeddings are anisotropic: every vector carries a large component
along one shared direction, so the corpus occupies a narrow cone rather than the
sphere. The mean of a large group converges on that shared direction, which makes
two unrelated groups' centroids nearly identical (measured: centroids 0.06 apart
for halves whose closest members sat 0.84 apart) and makes centroid-based growth
snowball. Split can never fire; groups collapse into a blob. Subtracting the mean
removes the shared component and both effects with it. The full measurement
table, and the alternatives that were rejected — further principal components,
UMAP, random projection, whitening — are in [`HISTORY.md`](HISTORY.md#8-raw-cosine-geometry).

**Why not 1.0.** Full removal is degenerate when the corpus is itself one tight
group: the mean lands on top of every signal, the residuals are whatever noise
remains, and a coherent story shatters into antipodal halves. Keeping a tenth of
the mean leaves every residual a shared component to agree on.

**Scale.** Centred distances run roughly twice raw cosine — median pairwise
distance 1.02 centred against 0.45 raw on the reference corpus. A threshold
carried over from a raw-cosine configuration is far too tight.

**The mean is state.** It is computed from the full collected membership of each
batch run, persisted in `c:state`, and **recomputed rather than accumulated**,
for the same reproducibility reason as centroids. Storage is unaffected: signals
persist raw and the projection is re-derived on read.

**Consistency across a run.** A batch establishes the mean, projects every
collected signal with it, recomputes every centroid in that same space, and
publishes mean and centroids together in one transaction. The Draft phase
therefore always centres an arriving signal against the same mean version the
stored centroids were built with.

**Cold start.** Before the first batch run there is no mean, and geometry is
raw. This is harmless: a story can only exist if a batch has run, so the Draft
phase has no centroid to compare against and every signal is an outlier
regardless of threshold.

---

## Tiered Window Strategy

| Tier | Name | Default | Role |
|---|---|---|---|
| **Tier 1** | Ingestion | 1 signal | Immediate provisional assignment to the nearest story. |
| **Tier 2** | Batch Window | `BatchWindow`, 24h | Bounds outlier retention and lifecycle transitions for the maintenance pass. |
| **Tier 3** | Active Context | `ActiveContextWindow`, 30d | Which stories may act as Draft anchors, and which members define `RecentCentroid`. |

Story membership itself is read **in full** on every run, not windowed: the
lifetime centroid is the mean of every member, so a windowed read would compute a
centre that slides as old signals age out.

---

## Story Lifecycle

```
Outlier ──► Active ──► Dormant ──► Archived (terminal)
                ▲          │
                └──────────┘
```

- **Outlier** — no story match yet. Held in the `o:` bucket for the next
  maintenance pass, which may promote it into a new story, admit it into an
  existing one, or evict it at `OutlierTTL`.
- **Active** — receiving signals, or last seen within `SilenceWindow`.
- **Dormant** — no new signals for `SilenceWindow`. May be a merge target
  (absorbing signals reactivates it) and may reactivate through Draft
  assignment. Centroid retained.
- **Archived** — no new signals for `ArchiveWindow`. Terminal: excluded from
  collection, never an anchor, never reactivated. New signals on the same topic
  form a fresh story. **Signal data and centroid are retained** — only the state
  field changes, and `SignalsOf` still iterates the members.

**Dormant thresholds.** `T_assign` needs `MeanDistance` and `Sigma`, which are
undefined for a story with no live window traffic. Both are therefore **frozen
in metadata on the Dormant transition** and used for threshold calculation until
reactivation, at which point they are **cleared** and the story re-enters
cold-start. That prevents a story reactivating around a different topic
distribution from inheriting stale thresholds.

---

## Draft Phase (real-time ingest)

For each arriving signal:

0. **Locate any existing copy.** Consult `l:{signalID}`. If the signal already
   belongs to a story — including one a batch run moved it to — the ingest is a
   strict no-op returning that story ID.
1. **Project** the embedding into centred space.
2. **Find the nearest anchor.** Scan the `t:` time index from
   `now − ActiveContextWindow` and compare against each candidate's
   `RecentCentroid`. Archived stories are skipped. Cost is proportional to
   candidate stories, not to stored signals.
3. **Test the adaptive threshold.**

   ```
   T_assign(story) = MeanDistance(story) + AssignmentK × σ(story)
   ```

   with these rules:

   - **Cold start.** Below `ColdStartMinSignals` members the per-story σ is not
     trusted; the threshold is `AssignmentK × σ_global`.
   - **σ floor.** σ is floored at `SigmaFloor × σ_global`, so a story whose
     first few signals are nearly identical cannot collapse its own threshold to
     zero.
   - **Ceiling.** The result is clamped to `AssignThreshold`, so a story that has
     drifted wide cannot keep widening its own catchment.
   - **Dormant.** Uses the frozen statistics described above.
   - **Before the first batch.** σ_global has never been measured, so
     `InitialSigmaGlobal` stands in.

4. **Assign or bucket, per facet.** Every facet is scored against every
   candidate story in a single walk of the time index — once for the whole
   signal, not once per facet. Within threshold, the facet's membership marker
   is written under the story, any stale outlier marker is dropped, and the
   location index is updated; otherwise the facet is held at
   `o:{signalID}:{facet}`. A signal may therefore be partly placed.
   `LastSignalAt` advances monotonically, once per touched story, and
   `EventDraftAssigned` is emitted once per `(signal, story)` — several facets
   landing in one story is still one signal joining one story.

   Re-ingest is a no-op at the signal level once **any** facet is placed: batch
   placements are authoritative and a late duplicate must not partially
   overwrite one.

**Centroid currency.** Centroids are recomputed only at the end of a batch run,
so a Draft decision may use a centroid up to `BatchInterval` old. Accepted:
Draft assignments are explicitly provisional, and the next maintenance pass
corrects the structure.

---

## Maintenance Pass

Runs every `BatchInterval` on a background goroutine. Collection is read-only;
every decision is computed from state already in hand and applied in a single
write transaction.

**Collect.** Every member of every non-Archived story, read in full, plus
outliers newer than `lastBatch − OutlierTTL`. Older outliers are returned for
eviction. The reference is `lastBatch`, not wall-clock `now`, so a maintenance
pause does not cause mass eviction: if the goroutine was not running,
`lastBatch` did not advance, and outliers are not penalised for that time.

**Establish geometry.** Compute the corpus mean from the collected signals and
project all of them.

### Operations, in order

| # | Operation | Rule |
|---|---|---|
| 1 | **Evict** | Delete outliers older than `lastBatch − OutlierTTL`, with their location index entries. |
| 2 | **Promote** | Group remaining outliers by nearest-centroid growth; each group that fits inside a ball of radius `AssignThreshold` and holds at least `MinStorySize` signals becomes a new story. |
| 3 | **Admit** | Outliers no group claimed join the nearest existing story whose adaptive threshold covers them. |
| 4 | **Split** | A story whose best two-way partition has part centroids more than `SplitThreshold` apart, both parts at least `MinStorySize`, divides in two. At most one split per story per run. |
| 5 | **Merge** | Mutually-close stories within `MergeThreshold` unify, provided the union is one step 4 would leave whole. Oldest `CreatedAt` survives. |
| 6 | **Recentre** | Recompute both centroids, radius, `MeanDistance`, σ, and `SignalCount` for every surviving story; retire any left empty. |
| 7 | **Lifecycle** | Active → Dormant → Archived on `SilenceWindow` / `ArchiveWindow`; update σ_global and persist the mean. |

Order matters. Promotion precedes admission so a fresh group competes for the
same outliers. Admission precedes split so a story widened by new members is cut
in the same run rather than staying diffuse until the next. Split precedes merge
so both see consistent post-split centroids.

### Promotion: growth with compaction

Growth seeds on the outlier with the most neighbours within `AssignThreshold`,
then repeatedly admits whichever candidate is nearest the **running** centroid
while it stays within the threshold. The closing **compaction** then drops any
member the finished centroid left outside the threshold, recentring until every
survivor is inside.

The compaction is what makes non-chaining a *guarantee* rather than an
observation: whatever path the centroid took while growing, every surviving
member ends within `AssignThreshold` of the final centre, so the group's diameter
is bounded by `maxAngularSeparation(AssignThreshold)` and a ladder of
near-neighbours cannot walk out of it.

Connected components are forbidden — they are the transitive linkage that chains
a corpus into one blob. Cliques were the previous rule and were measurably too
strict; see [`HISTORY.md`](HISTORY.md#7-mutual-neighbour-cliques-for-outlier-promotion).

### Admission

The Draft phase runs once, at ingest. A signal arriving before the batch that
creates its story is bucketed and never re-tested, so it would expire even when
an established story covered it perfectly — 342 of 596 reference-corpus signals
were stranded this way before admission existed.

Admission re-applies the Draft test against stories that now exist: nearest
`RecentCentroid`, same adaptive threshold formula. Every threshold and centroid
is computed **once, from pre-admission membership**, so the outcome is
independent of the order outliers are visited and no admission can widen a story
enough to admit the next.

This is not the per-signal reassignment the stability rule forbids. That rule
protects story *membership* from being reshuffled run to run; an outlier has no
membership to disturb, and admission never moves a signal between stories.

### Split: gate, then decision

Testing every story's best partition each run is wasteful, so a **necessary
condition** runs first: attempt a split only when `4r − 2r² > SplitThreshold`,
where *r* is the story radius.

The bound is **not** the Euclidean `2r`. Cosine distance is not a metric, and
`1 − cos` grows quadratically in the angle, so two members each at distance *r*
from the centroid can be `1 − cos(2·arccos(1−r))` apart, which expands to
`4r − 2r²`. At *r* = 0.122 that is 0.458 against the 0.245 a Euclidean bound
predicts, so `2r` would skip stories that genuinely split.

Past the gate, the story is partitioned by a two-medoid Lloyd loop seeded on its
two most distant members, bounded to ten iterations. The split is accepted only
if both parts hold at least `MinStorySize` members and the two part centroids are
more than `SplitThreshold` apart. A story with no internal gap is left whole:
cutting it would produce halves inside the hysteresis band with nothing to
reunite them.

The larger part keeps the story ID so identity survives for the majority of
holders; ties go to the part holding the older signal.

### Merge: the exact inverse of split

Candidate groups are **cliques** over story centroids within `MergeThreshold` —
every member within threshold of every other. Story-level chaining is real: a
ladder of 12 stories each 0.005 from its neighbour once merged into one whose
ends were 0.55 apart.

Each candidate group is then compacted until the story it would produce is one
the split step would leave whole, dropping the member furthest from the union
centroid each round. If no subset of two or more survives, no merge happens.

The test is `splitStory` on the union, **not** the split radius gate. The gate is
only a necessary condition for splitting, so using it here rejects unions no
split would ever touch — at `SplitThreshold` 0.55 it demands a union radius under
0.15, and no merge fires at all
([`HISTORY.md`](HISTORY.md#9-radius-gate-as-the-merge-admission-test)).

The **oldest `CreatedAt` survives**; ties break on story ID. A merge is a
key-space migration: every signal key moves from the retired prefix to the
survivor's, **including signals older than `BatchWindow`**. This is an
identity-level operation and the documented exception to the membership rule.

### Thresholds and hysteresis

`MergeThreshold` (0.40) and `SplitThreshold` (0.55) are the two edges of one
hysteresis band, and `SplitThreshold` must be strictly greater — `validate()`
rejects a collapsed band. Merge tests one historically-given partition; split
*searches* for the best one. A search can beat a fixed arrangement on the same
signals, so with a shared value a merge and a split undo each other along
different seams and story IDs churn while the data is unchanged.

`AssignThreshold` (0.50) governs a different comparison — one point against one
centroid, rather than two centroids against each other — and so keeps its own
value. `validate()` requires `MergeThreshold < AssignThreshold`: stories may not
merge at a distance wider than a signal may sit from a centroid.

### Membership stability

No signal is relocated individually. Story membership changes only through a
split or a merge, each of which moves a whole group and emits an event. An
individual misassignment is therefore not individually correctable — it is
repaired only when enough similar signals accumulate to form a group clearing
`MinStorySize`, which is exactly when the error is worth acting on.

**Emptied stories are retired.** A story left with nothing under its
`s:{storyID}:` prefix — emptied by a merge of its last signals — has its
metadata and time-index entry deleted, increments `StoriesRetired`, and emits
`EventStoryRetired`.

### Determinism

A run is a pure function of the stored state. Nothing depends on map iteration
order: story and signal sets are sorted by ID before any decision, ties break on
ID, and both the mean and every centroid are recomputed from full membership
rather than accumulated. A second run over unchanged data changes nothing —
asserted by `TestStability_IdempotentRerun` and, on the real corpus, by the
closing no-op pass of `TestStreaming_IncrementalArrivalsAreStable`.

**Convergence takes one extra pass, not zero.** Promotion creates stories and
moves σ_global, so an outlier that no story covered when a pass began may be
covered by the next pass — measured, exactly one extra pass absorbs the
stragglers (three signals promoted, one admitted on a 400-signal store), after
which the store is a hard fixpoint that repeated passes do not touch. Absorption
only ever *adds* assignments; a settling pass never moves a signal between
stories. `TestStreaming_IncrementalArrivalsAreStable` asserts both halves.

**That guarantee now extends across stores.** Two *fresh* ingests of the same
corpus into two empty stores used to differ slightly: a new story's ID came from
`uuid.New()`, and decisions are ordered by story ID, so which of two otherwise
equivalent stories was visited first varied — worth 0.4% to 1.0% of total churn
run to run on the streaming suite. Story IDs are derived from their founding
signals (see [UUID Namespace](#uuid-namespace)), which removes that source of
variation: the 300-seed shape now reports the same 3 of 791 carried assignments
on every fresh run, and the other three shapes report zero.

### Global calibration

σ_global is the exponential moving average of per-signal centroid distances
across all Active stories, updated at the end of each run:

```
σ_global ← EMAAlpha × σ_global_prev + (1 − EMAAlpha) × mean_distance_all_active
```

It is persisted in `c:state` and bootstrapped from the first run containing at
least one Active story. Until then `InitialSigmaGlobal` stands in.

---

## Persistence

The library talks to a minimal prefix-scannable KV store through `Store` and
`Tx`. Implementations must provide **lexicographic byte ordering** — what bbolt,
LevelDB, and most embedded stores give by default — because the range scans
depend on it. `MemStore` ships for tests and small deployments.

### Key Schema

| Purpose | Key | Value |
|---|---|---|
| Calibration state | `c:state` | JSON: σ_global, dimensionality, last batch timestamp, corpus mean |
| Story metadata | `s:{storyID}:m` | JSON: both centroids, radius, state, timestamps, live and frozen statistics |
| Canonical signal record | `g:{signalID}` | Encoded `Signal[T]`; the one authoritative copy |
| Facet membership | `s:{storyID}:f:{signalID}:{facet}` | empty marker |
| Unplaced facet | `o:{signalID}:{facet}` | empty marker |
| Signal location index | `l:{signalID}` | JSON array, one entry per facet: `s:{storyID}`, `o`, or empty |
| Story time index | `t:{unix_sec}:{storyID}` | empty |

**Story time index.** Deleted and re-inserted on every metadata write so the
timestamp stays current. A range scan from `t:{cutoff}:` retrieves recently
active stories for Tier 3 without a full metadata scan.

**Canonical signal record.** The payload lives once at `g:{signalID}`,
independently of where its facets sit, so a signal in several stories is stored
once rather than copied per membership — and so the whole corpus can be
enumerated by one prefix scan. It is written on first ingest and never
rewritten, and deleted only when no facet of the signal remains anywhere: no
membership under any story, and nothing in the outlier bucket. That delete runs
in the same transaction as the one that removed the last facet.

**Signal location index.** Derived state, rebuildable in full from the facet
membership and outlier key spaces. It carries one entry per facet and is
updated by every placement change, in the same transaction as the markers it
mirrors, so the two cannot disagree. It exists so `Ingest` can find where a
signal's facets live without scanning, which is what makes re-ingestion after a
batch move a no-op rather than a duplicate.

**Signal retention.** Signal data is retained in all story states, Archived
included. No signal keys are deleted on archival.

---

## Concurrency Model

The store is assumed to permit one write transaction at a time, so an Apply phase
rewriting thousands of keys would otherwise block every concurrent `Ingest` for
its full duration.

1. The batch goroutine sets an atomic `applyInProgress` flag around **only the
   write transaction**. Collection is read-only and clustering touches no store,
   so writers are not stalled for those phases.
2. `Ingest` calls arriving while the flag is set write to `ingestBuffer`, an
   in-memory channel bounded to `IngestBufferCap`, instead of to the store. If
   the buffer is full, `Ingest` blocks until space frees or `ctx` is cancelled —
   back-pressure without loss.
3. The caller still receives a provisional story ID, computed against
   `draftSnapshot`: an immutable copy of the story metadata the batch already
   collected, published for the Apply window. This lookup **must not touch the
   store** — the `Store` contract does not promise `View` may run concurrently
   with `Update`, and a single-lock backend would block the caller for the whole
   Apply, the exact stall the buffer exists to prevent.
4. Once Apply commits, the flag clears and the goroutine drains the buffer by
   re-ingesting each signal for real. **That placement is authoritative**, not
   the provisional ID.

**Crash semantics: at-most-once.** `ingestBuffer` is in-memory. A crash after
Apply commits but before the drain completes loses the buffered signals. Callers
needing more should keep a write-ahead log or rely on idempotent re-ingestion —
deterministic UUID v5 IDs make a repeat ingest a no-op, though
`EventDraftAssigned` is not re-emitted for an already-stored signal.

**Batch failures** leave the store untouched and return an empty summary; the
next tick retries. `Config.OnBatchError` is the only way to observe one.

---

## Public API

Full knob-by-knob configuration reference lives in
[`README.md`](README.md#configuration-reference).

```go
// Lifecycle.
func NewTracker[T any](cfg Config[T]) (*Tracker[T], error)
func (t *Tracker[T]) Close() error

// Ingest processes one signal and returns its provisional story ID.
// Returns ErrDimensionMismatch on an embedding length that differs from the
// first ingested signal. Goroutine-safe.
func (t *Tracker[T]) Ingest(ctx context.Context, sig Signal[T]) (uuid.UUID, error)

// SignalID derives the UUID v5 signal ID for a domain key under the
// configured namespace. Prefer it over calling uuid.NewSHA1 directly.
func (t *Tracker[T]) SignalID(domainKey string) uuid.UUID

// Events. Each Subscribe call returns an independent channel, closed on Close.
// A full channel receives EventBufferOverflow in place of dropped events.
func (t *Tracker[T]) Subscribe() <-chan StoryEvent[T]

// Reads.
func (t *Tracker[T]) Story(id uuid.UUID) (StoryMeta, error)          // ErrNotFound if absent
func (t *Tracker[T]) Stories(state StoryState) iter.Seq[StoryMeta]   // StoryStateAny for all
func (t *Tracker[T]) SignalsOf(id uuid.UUID) iter.Seq2[Signal[T], error]
func (t *Tracker[T]) Signal(id uuid.UUID) (Signal[T], error)         // story member or outlier

// Shipped helpers.
func NewMemStore() *MemStore   // in-memory Store
```

### Events

```go
const (
    EventDraftAssigned    EventKind = iota // real-time: signal provisionally assigned
    EventSignalReassigned                  // batch: signal moved into or between stories
    EventStoryCreated                      // new story persisted
    EventStorySplit                        // one story became two; StoryID2 is the child
    EventStoryMerged                       // two became one; StoryID2 is the retired ID
    EventStoryRetired                      // batch emptied the story; record deleted
    EventStoryDormant                      // crossed SilenceWindow
    EventStoryArchived                     // crossed ArchiveWindow
    EventBatchComplete                     // one per run; BatchSummary populated
    EventBufferOverflow                    // subscriber channel full; events dropped
)

type BatchSummary struct {
    StoriesCreated    int
    StoriesMerged     int
    StoriesSplit      int
    StoriesRetired    int
    SignalsReassigned int
    OutliersEvicted   int
    OutliersPromoted  int
    OutliersAdmitted  int
}
```

`EventSignalReassigned` accompanies outlier promotion, admission, split
migration, and merge migration. A run touching many signals emits many of them
before `EventBatchComplete`, so subscribers that cannot keep up should consume
`EventBatchComplete` for coarse progress and query the store for detail, or raise
`EventBufferSize`.

---

## Measured Behaviour

Reference corpus: 596 real news embeddings, dimension 3072, roughly 20–30 topics
(`testdata/corpus_embeddings.txt`, gated behind the `CORPUS` environment
variable).

| Property | Result |
|---|---|
| Stories found | 32, largest 28, 234 signals assigned |
| Repeat runs over unchanged data | Byte-identical membership across 5 runs |
| Streaming, 400 seed then 50 / 20 / 5 at a time | **0% churn** — not one of 645, 1690, or 6874 carried assignments moved |
| Streaming, 300 seed then 50 at a time | 0.4–1.0% churn — 3 to 8 of 791 carried assignments moved |
| Largest story during streaming | Pinned at 24–27 throughout |
| Story count during streaming | Grows 14 → 37 as new topics arrive |
| Passes to a fixpoint after an arrival | 1 extra pass, then nothing changes ever |

The remaining signals stay in the outlier bucket. On news data much of that is
genuine — isolated stories of one or two articles — and the rest is the coverage
side of the `MeanRemoval` / `AssignThreshold` trade-off documented in
[`README.md`](README.md#tuning).

---

## Non-Goals

- Distributed operation (single-node, embedded).
- Approximate nearest-neighbour indexing — brute force over centroids is
  O(stories) and fast enough for the expected story count.
- Built-in embedding generation (caller-provided).
- Correcting an individual misassigned signal, by design; see Membership
  stability.
