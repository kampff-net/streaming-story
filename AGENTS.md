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

The library is a hybrid clustering system: a **Draft phase** (real-time, per-signal) and a **Refinement phase** (periodic batch, HDBSCAN).

### Signal Flow

1. `Ingest` → cosine-similarity nearest-centroid lookup → assign or outlier-bucket.
2. Background goroutine fires every `BatchInterval` → HDBSCAN → cluster mapping → KV apply → emit events.

### Cluster Mapping (Two-Phase)

- **Phase 1**: Uses the Hungarian algorithm for optimal 1-to-1 continuity (cost = 1 − Jaccard).
- **Phase 2**: Scans full unmatched set for splits and merges.
- Both phases use Jaccard over **BatchWindow-scoped signals only** — not lifetime signals — to avoid denominator blow-up on mature stories.
- For N-way merges, the **oldest StoryID survives** (earliest creation time). If secondary story is older than primary, survivor/retired labels flip.

### Sampling (Two-Pass)

When `len(signals) > BatchSampleCap`, sampling is two-pass:
1. **Guaranteed pass**: `MinClusterSize` signals per Active story, capped at `SampleGuaranteeMaxFraction` (0.5) × `BatchSampleCap` total. If budget is exceeded, per-story reservations scale down proportionally (floor 1).
2. **Proportional pass**: remaining capacity distributed by signal count.

### Stability Rule

Re-assignment is scoped to `BatchWindow`. Signals older than `BatchWindow` are never moved by a batch run.
**Exception**: a merge is a key-space migration (all signal keys move under the surviving story's prefix, including historical ones) — exempt from the stability rule.

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

### Cluster Extraction

`Config.ClusterSelection` picks how clusters are read out of the condensed tree: `ClusterSelectionEOM` (excess of mass, the default) or `ClusterSelectionLeaf`.

EOM can select a broad parent over the tighter clusters nested inside it, collapsing several distinct stories into one. The apply phase then moves those stories' window signals into the winner and retires the emptied stories. Raising `MinClusterSize` does not help — that acts during condensation, and a broad region that is genuinely dense only gets more stable. Use leaf extraction when the corpus has that shape.

`Config.MaxClusterSize` (0 = unlimited) caps candidate cluster size under EOM, forcing a descent into children. Not applicable to leaf extraction.

### Resolved Design Decisions

- `MinClusterSize` is a **fixed config constant** — not derived from window population.
- `StabilityWindow` is **removed** — `BatchWindow` is the sole re-assignment scope.
- Signal UUID namespace is a **fixed compile-time constant** (`TrackerNamespace`) — not derived from store path.
- Default windows are calibrated for **news-frequency ingestion** (1–10 signals/day per topic).
