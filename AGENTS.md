# AGENTS.md

This document provides consolidated guidance for AI coding agents (Gemini Coder, Antigravity, Claude Code, etc.) working in this repository.

## Repository State

This is a **Go library** for real-time streaming story clustering.
Module: `go.kvsh.ch/streaming-story`  
Go version: 1.26.5 (uses `iter.Seq` / `iter.Seq2` standard library range-over-func patterns).

Refer to `DESIGN.md` for full architectural specifications before implementing core algorithms.

## UNDERCOVER MODE — CRITICAL

You are operating UNDERCOVER in a PUBLIC/OPEN-SOURCE repository.
Your commit messages, PR titles, and PR bodies MUST NOT contain ANY Google or Anthropic information.
Do not blow your cover. Do not reveal yourself. Do not add AI signatures to commit messages.

## Package Layout

Public API and everything that touches the store live in the root `story`
package. Pure logic lives in `internal/`, which nothing outside the module can
import, so those packages can be reshaped freely.

| Package | Holds |
|---|---|
| `story` (root) | Public types, `Config`, `Store`/`Tx`, `MemStore`, `CBORCodec`, and the `Tracker`: ingest, batch orchestration, persistence, events. |
| `internal/geom` | Vector geometry: the corpus mean, the projector that centres against it, group statistics, the quadratic angular bound. |
| `internal/cluster` | Grouping decisions over an index-based `Point`: growth, cliques, split, merge planning. Pure — no store, no clock, no `Config`. |
| `internal/keys` | The KV key schema and its parsers. Nothing else assembles a key. |
| `internal/dist` | Cosine distance over BLAS. |

Root files are grouped by function, not by type: `types.go` (public types and
events), `config.go` (knobs and validation), `store.go` (the `Store`/`Tx`/`Codec`
contracts plus `MemStore` and `JSONCodec`), `tracker.go` (lifecycle, batch loop,
subscriber fan-out), `ingest.go` (the Draft path), `threshold.go` (admission
radius policy, shared by Draft and by outlier admission), `batch.go` (collection,
apply window, snapshot, buffer drain), `maintain.go` (the pass itself),
`points.go` (the collected-signal form and every conversion into the `geom` /
`cluster` views), `record.go` (persisted shapes, their store access, and
calibration state), and `query.go` (the read API).

Two rules worth keeping: a decision belongs in `internal/cluster` if it can be
expressed over points and thresholds alone, and any conversion between a
`batchFacet` and an algorithm's view belongs in `points.go` rather than at the
call site.

## Build and Test Commands

```bash
go build ./...
go test ./...
go test -run TestName ./pkg/...   # single test
go vet ./...
```

No Makefile or custom tooling exists yet.

## Architecture & System Design

The library is a hybrid clustering system: a **Draft phase** (real-time, per-signal nearest-centroid assignment) and a **Maintenance phase** (periodic batch: promote, split, merge, recentre).

### Signal Flow

1. `Ingest` → cosine-similarity nearest-`RecentCentroid` lookup → assign or outlier-bucket.
2. Background goroutine fires every `BatchInterval` → maintenance pass → KV apply → emit events.

### Merge Survivor Rule

For merges, the **oldest StoryID survives** (earliest `CreatedAt`). Signals migrate by key-space scan under the survivor's prefix, including signals older than `BatchWindow`. A split migrates the same way. Sampling and the `BatchWindow`-scoped re-assignment rule are gone: membership is read in full because the lifetime centroid is the mean of every member.

### Dormant & Archived Story Lifecycle

- Active → Dormant: crossed `SilenceWindow`.
- Dormant → Archived: crossed `ArchiveWindow`.
- Dormant stories have no live signals in the window, so `mean_distance` and `σ` are undefined. They are **frozen in metadata** on the Dormant transition and used for Draft-phase threshold calculation.
- On reactivation, frozen stats are **cleared** and the story re-enters cold-start (falls back to `σ_global`).

### Outlier TTL Reference Point

Outliers are evicted when `At < lastBatchTimestamp − OutlierTTL`. The reference is `lastBatchTimestamp`, not wall-clock `now`, so maintenance pauses do not cause mass eviction.

### Concurrency & KV Store Constraints

The underlying KV store is assumed to be single-writer/multi-reader (like `bbolt` or `LevelDB`).
During the Apply phase an `applyInProgress` flag redirects `Ingest` calls into an in-memory `ingestBuffer` (bounded channel). The batch goroutine drains the buffer in a follow-up transaction. This is **at-most-once**: a crash between Apply commit and drain loses buffered signals.

The flag covers **only the write transaction** — collection is read-only and clustering touches no store, so writers are not stalled for those phases.

A buffered `Ingest` still returns a provisional story ID. It is computed from `draftSnapshot`, an immutable copy of the story metadata the batch already collected, published for the Apply window. The lookup **must not touch the store**: the `Store` contract does not promise `View` may run concurrently with `Update`, and single-lock backends (`MemStore` included) would block the caller for the whole Apply — the exact stall the buffer exists to prevent. The drain re-ingests each buffered signal for real; that placement is authoritative.

### Key Space

| Key | Value |
|---|---|
| `c:state` | Global calibration state (`calibState`): σ_global, dim, lastBatch, mean |
| `s:{storyID}` | Story metadata (`storyRecord`): centroid, recent centroid, radius, timestamps, stats |
| `s:{storyID}:f:{signalID}:{facet}` | Facet membership marker, payload-free |
| `g:{signalID}` | Canonical signal record (`Signal[T]`): ID, timestamp, embeddings, caller payload |
| `o:{signalID}:{facet}` | Unplaced facet marker, payload-free |
| `o:{signalID}` | Outlier signal |
| `l:{signalID}` | Signal location index: CBOR-encoded `[]FacetLoc` with one entry per facet |
| `t:{unix_sec}:{storyID}` | Time index for efficient Tier 3 range scans |

### Library Conveniences

- `story.CBORCodec[T]` is the shipped default `Codec`; supply a custom one only for a non-CBOR format.
- `Tracker.SignalID(domainKey)` derives the UUID v5 signal ID under `Config.Namespace`. Prefer it over calling `uuid.NewSHA1(story.TrackerNamespace, ...)` directly, which ignores a configured namespace.
- `Config.OnBatchError` is the only way to observe a failed batch run: a failure leaves the store untouched, returns an empty summary, and the next tick retries.
- `Config.InitialSigmaGlobal` (default 0.25) is the σ_global stand-in before the first batch measures one. Until then every story is in cold-start, so this single value decides the Draft assignment radius.

### Maintenance Pass (spec 006)

HDBSCAN is **gone**. `internal/hdbscan` and `internal/hungarian` were deleted, along with the two-phase Jaccard mapping engine. The batch phase now maintains existing stories rather than re-deriving them: evict, promote, split, merge, recentre, lifecycle.

Why: HDBSCAN's MST over mutual-reachability distances is single linkage with a density correction, and news embeddings chain through it. Measured on 596 real signals, transitive grouping at cosine distance 0.25 produced a 324-signal component; nearest-centroid grouping at the same threshold produced a largest group of 22.

Key rules when touching this code:

- **Every distance is measured in centred space.** The corpus mean is subtracted (`Config.MeanRemoval`, default 0.9) before any comparison; see `internal/geom`. Raw cosine is anisotropic: the mean of a large group converges on the shared direction every embedding carries, so two unrelated halves of the corpus had centroids 0.06 apart while their nearest members sat 0.84 apart. Split could therefore never fire, and centroid-based growth snowballed — end to end, `MeanRemoval = 0` yields 2 stories with a 581-signal blob where 0.9 yields 32 with a largest of 28.
- **`MeanRemoval` is 0.9, not 1.0.** Full removal is degenerate when the corpus is itself one tight group: the mean lands on top of every signal and the residual noise reads as opposition, shattering a coherent story into antipodal halves.
- **Thresholds are centred-space distances**, roughly twice the raw-cosine scale. Defaults: `AssignThreshold` 0.50, `MergeThreshold` 0.40, `SplitThreshold` 0.55. A value carried over from raw cosine is far too tight.
- **Promotion uses nearest-centroid growth with a closing compaction, not cliques.** Growth admits whatever is nearest the running centroid; the compaction then drops any member the final centroid left outside the threshold, which bounds the group whatever path the centre took. Cliques were replaced because a real news cluster does not satisfy all-pairs adjacency: at one threshold it grouped 98 of 596 signals (16%) where growth grouped 182 (31%); the shipped pipeline reaches 234 (39%). Connected components remain forbidden — they are the transitive linkage that chains.
- **Outliers are admitted to covering stories during maintenance.** A signal that arrives before the batch creating its story lands in the outlier bucket, and Draft never runs again for it; without admission, 342 of 596 signals stayed stranded until TTL. This is not the forbidden signal reassignment — an outlier has no membership to disturb.
- **Merge is gated by the split decision itself, not the radius gate.** The gate is only a *necessary* condition for a split, so using it to approve a merge refuses unions no split would ever touch: at `SplitThreshold` 0.55 it demands a union radius under 0.15, and merges never fire.
- **`MergeThreshold` < `SplitThreshold` is mandatory** and enforced by `validate()`. Merge tests a given partition; split searches for the best one, so a shared value lets them undo each other along different seams.
- **The split radius gate is `4r − 2r²`, not `2r`.** Cosine distance is not a metric; `1 − cos` is quadratic in the angle. A Euclidean bound silently skips stories that should split.
- **Centroids are recomputed from members every run, never accumulated.** An incremental mean depends on arrival order, which breaks reproducibility.
- **Two centroids:** `Centroid` (lifetime) for identity geometry, `RecentCentroid` (ActiveContextWindow) for admission.
- **No individual signal reassignment.** Story membership changes only via split or merge; outlier admission is the one documented exception, and it moves signals into stories, never between them.

### Resolved Design Decisions

- **Facets.** A signal carries one `Embedding` per facet (`Signal.Embeddings`). A facet is the unit of assignment and geometry and belongs to at most one story; a signal belongs to the union of its facets' stories, so membership is many-to-many. The library never creates, merges, reorders, or drops a facet — decomposition is the caller's judgment. Facet order is its persistent identity. See [spec 007](spec/007_multi_facet_signals/spec.md).
- **Sizes count distinct signals, never facets.** `cluster.Params.MinSize` counts distinct `Point.ID`s, so one signal split into `MinStorySize` facets cannot found a story alone. Do not "simplify" it back to `len(group)`.
- `MinStorySize` is the minimum distinct signals for a group to be a story; it gates promotion and both sides of a split. It replaced `MinClusterSize`, which has since been removed along with every other dead knob — `Config` has no no-op fields, and none should be reintroduced. See [`HISTORY.md`](HISTORY.md#removed-config-fields).
- **`BatchWindow` only sets the `OutlierTTL` default.** It does not bound clustering input: membership is read in full every pass. Do not reach for it to limit work.
- `StabilityWindow` is **removed** — `BatchWindow` is the sole re-assignment scope.
- Signal UUID namespace is a **fixed compile-time constant** (`TrackerNamespace`) — not derived from store path.
- **Story IDs are derived, not random.** A new story's ID is UUID v5 under `Config.Namespace` over its founding signal IDs (sorted) plus a seed naming the birth route: `promote` for an outlier group, `split:{parentID}` for a split child. Replaying a signal stream against a fresh store therefore reproduces the same story IDs, which is what makes a recorded run diffable against a replay. `deriveStoryID` rejects an ID already held — by a live story in the same run or by any record in the store, archived included — and rederives with the next salt, because a split can spawn exactly the member set an existing story was founded on and reusing that ID would silently fold the two together. Salting is part of the derivation, so a replay takes the same rejections in the same order. Do not reintroduce `uuid.New()` on the story path.
- Default windows are calibrated for **news-frequency ingestion** (1–10 signals/day per topic).
