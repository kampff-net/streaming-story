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

### KV Key Schema

| Prefix | Content |
|---|---|
| `c:state` | `σ_global`, dimensionality, last batch timestamp |
| `s:{storyID}:m` | Story metadata (centroid, radius, state, timestamps, frozen stats) |
| `s:{storyID}:s:{signalID}` | Signal data |
| `o:{signalID}` | Outlier signal |
| `l:{signalID}` | Signal location index: `s:{storyID}` for story membership, `o` for outlier bucket. Lets `Ingest` find where a copy lives so re-ingestion after a batch move never duplicates it |
| `t:{unix_sec}:{storyID}` | Time index for efficient Tier 3 range scans |

### Library Conveniences

- `story.JSONCodec[T]` is the shipped default `Codec`; supply a custom one only for binary encodings.
- `Tracker.SignalID(domainKey)` derives the UUID v5 signal ID under `Config.Namespace`. Prefer it over calling `uuid.NewSHA1(story.TrackerNamespace, ...)` directly, which ignores a configured namespace.
- `Config.OnBatchError` is the only way to observe a failed batch run: a failure leaves the store untouched, returns an empty summary, and the next tick retries.
- `Config.InitialSigmaGlobal` (default 0.25) is the σ_global stand-in before the first batch measures one. Until then every story is in cold-start, so this single value decides the Draft assignment radius.

### Maintenance Pass (spec 006)

HDBSCAN is **gone**. `internal/hdbscan` and `internal/hungarian` were deleted, along with the two-phase Jaccard mapping engine. The batch phase now maintains existing stories rather than re-deriving them: evict, promote, split, merge, recentre, lifecycle.

Why: HDBSCAN's MST over mutual-reachability distances is single linkage with a density correction, and news embeddings chain through it. Measured on 596 real signals, transitive grouping at cosine distance 0.25 produced a 324-signal component; nearest-centroid grouping at the same threshold produced a largest group of 22.

Key rules when touching this code:

- **Promotion uses mutual-neighbour cliques, never connected components.** Components are the transitive linkage that chains.
- **`MergeThreshold` < `SplitThreshold` is mandatory** and enforced by `validate()`. Merge tests a given partition; split searches for the best one, so a shared value lets them undo each other along different seams.
- **The split radius gate is `4r − 2r²`, not `2r`.** Cosine distance is not a metric; `1 − cos` is quadratic in the angle. A Euclidean bound silently skips stories that should split.
- **Centroids are recomputed from members every run, never accumulated.** An incremental mean depends on arrival order, which breaks reproducibility.
- **Two centroids:** `Centroid` (lifetime) for identity geometry, `RecentCentroid` (ActiveContextWindow) for admission.
- **No individual signal reassignment.** Membership changes only via split or merge.

### Resolved Design Decisions

- `MinStorySize` is the minimum signals for a group to be a story; it gates promotion and both sides of a split. It replaced `MinClusterSize`, which is retained as a deprecated no-op field.
- `StabilityWindow` is **removed** — `BatchWindow` is the sole re-assignment scope.
- Signal UUID namespace is a **fixed compile-time constant** (`TrackerNamespace`) — not derived from store path.
- Default windows are calibrated for **news-frequency ingestion** (1–10 signals/day per topic).
