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

> Spec 006 supersedes the batch re-clustering pipeline in 002 and the cluster
> mapping engine in 003. Both remain listed for the history.

## Completed

| # | Feature | Status | Created | Updated | Spec |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 006 | Centroid-Based Incremental Clustering | ✅ `COMPLETED` | 2026-08-16 | 2026-08-17 | [spec.md](006_centroid_incremental_clustering/spec.md) |

> Implemented, then revised in five places once measured against the reference
> corpus: centred geometry, promotion by centroid growth, outlier admission, the
> merge admission test, and story IDs derived from their founding signals rather
> than drawn at random. §2.10 of the spec summarises them; superseded
> approaches are in [HISTORY.md](../HISTORY.md). One item remains open: no
> pre-change benchmark baseline was captured, so batch-run performance is
> unverified rather than known-good.

## Proposed

| # | Feature | Status | Created | Updated | Spec |
| :--- | :--- | :--- | :--- | :--- | :--- |
