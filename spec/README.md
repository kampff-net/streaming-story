# Specifications Index

> Spec-Driven Development (SDD) specs for `go.kvsh.ch/streaming-story`.
> See [TEMPLATE.md](TEMPLATE.md) for the spec template and guidelines.

## In Progress

| # | Feature | Status | Created | Updated | Spec |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 001 | Real-Time Ingestion & Draft Engine | 🔷 `DESIGN` | 2026-08-11 | 2026-08-11 | [spec.md](001_draft_ingestion_engine/spec.md) |
| 002 | Periodic Batch Re-clustering & HDBSCAN | ⛔ `SUPERSEDED` by 006 | 2026-08-11 | 2026-08-11 | [spec.md](002_batch_refinement_hdbscan/spec.md) |
| 003 | Two-Phase Cluster Mapping & Lifecycle | ⛔ `SUPERSEDED` by 006 | 2026-08-11 | 2026-08-11 | [spec.md](003_cluster_mapping_lifecycle/spec.md) |
| 004 | KV Storage Schema & Persistence Layer | 🔷 `DESIGN` | 2026-08-11 | 2026-08-11 | [spec.md](004_storage_persistence_layer/spec.md) |
| 005 | Event Streaming & Iterators API | 🔷 `DESIGN` | 2026-08-11 | 2026-08-11 | [spec.md](005_event_streaming_iterators/spec.md) |
| 009 | Story Suppression Lifecycle & Tracker State | 🔷 `APPROVED` | 2026-08-19 | 2026-08-19 | [spec.md](009_story_suppression_lifecycle/spec.md) |

> Spec 006 supersedes the batch re-clustering pipeline in 002 and the cluster
> mapping engine in 003. Both remain listed for the history.

> Spec 007 widens 006 rather than replacing it: every assignment rule 006
> established is kept verbatim and re-applied at facet granularity. It changes
> the store schema and breaks `Signal`, so existing stores are rebuilt by replay.
> Two items remain open and are recorded in the spec: the change is benchmarked
> only against `MemStore`, and no corpus decomposed by a real extractor exists
> yet, so the effect on the orphan rate is unmeasured.

## Completed

| # | Feature | Status | Created | Updated | Spec |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 006 | Centroid-Based Incremental Clustering | ✅ `COMPLETED` | 2026-08-16 | 2026-08-17 | [spec.md](006_centroid_incremental_clustering/spec.md) |
| 007 | Multi-Facet Signals & Many-to-Many Membership | ✅ `COMPLETED` | 2026-08-18 | 2026-08-18 | [spec.md](007_multi_facet_signals/spec.md) |
| 008 | High-Throughput Performance & Latency Optimizations | ✅ `COMPLETED` | 2026-08-18 | 2026-08-20 | [spec.md](008_performance_optimizations/spec.md) |

> Implemented, then revised in five places once measured against the reference
> corpus: centred geometry, promotion by centroid growth, outlier admission, the
> merge admission test, and story IDs derived from their founding signals rather
> than drawn at random. §2.10 of the spec summarises them; superseded
> approaches are in [HISTORY.md](../HISTORY.md). One item remains open: no
> pre-change benchmark baseline was captured, so batch-run performance is
> unverified rather than known-good.

> Spec 008: CBOR replaces JSON across every record and the default codec, the story record
> splits into batch-owned and ingest-owned halves, and an in-memory
> `activeStoryIndex` takes `findNearestStories` off the store entirely.
> Steady-state ingest is 213x faster, the store footprint halved, batch collect
> allocations fell 60%. Batch throughput reached 1.3-2.8x against a 5x target:
> the batch is dominated by clustering, not serialization, and this spec never
> touched clustering. `BenchmarkIngestDuringApply` regressed 32%, accepted as an
> edge case with the remedy named in `comparison.txt`.
>
> Task 8 was added late. A differential test against the pre-change tree found
> three logic changes inside a spec whose premise is that only the storage
> format changes; two were reverted at no measurable cost, the third was cleared
> on inspection. §2.5 keeps the record, because the change that did the most
> damage was the one nobody had declared.

## Proposed

_None._
