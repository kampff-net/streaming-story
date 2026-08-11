# Streaming Story Tracker (`go.kvsh.ch/streaming-story`)

`go.kvsh.ch/streaming-story` is a Go library for ingesting a continuous stream of semantic vector signals and grouping them into evolving, real-time stories.

It implements a **Hybrid Clustering** approach:
1. **Real-time Ingestion (Draft Phase)**: Immediate low-latency signal assignment to the nearest active story centroid based on cosine similarity and dynamic adaptive thresholding.
2. **Periodic Batch Re-clustering (Refinement Phase)**: Asynchronous background HDBSCAN density clustering to discover true story structures, resolving splits, merges, and initial draft misassignments over sliding temporal windows.

---

## Installation

Requires **Go 1.26.5+** (uses standard library range-over-func iterators `iter.Seq` / `iter.Seq2`).

```bash
go get go.kvsh.ch/streaming-story
```

---

## Architecture & Concepts

* **Signal**: Atomic input element containing a UUID, timestamp, `float32` vector embedding, and opaque payload `T`.
* **Story**: Persistent semantic cluster with a calculated centroid, radius, creation timestamp, and state (`Active`, `Dormant`, `Archived`).
* **Tiered Temporal Windows**:
  - **Tier 1 (Ingestion)**: 1 signal at a time for immediate provisional draft assignment.
  - **Tier 2 (Batch Window)**: Recent signals (default: 24h) fed to HDBSCAN for batch re-clustering.
  - **Tier 3 (Active Context)**: Recent stories (default: 30d) used as centroid anchors.

---

## Quickstart & Usage

### 1. Implement a `Codec[T]` for Payload Persistence

Define a codec to serialize and deserialize your custom payload type `T`:

```go
package main

import (
	"encoding/json"
	"go.kvsh.ch/streaming-story"
)

type ArticlePayload struct {
	Title   string `json:"title"`
	Source  string `json:"source"`
	Content string `json:"content"`
}

type JSONCodec struct{}

func (c JSONCodec) Encode(sig story.Signal[ArticlePayload]) ([]byte, error) {
	return json.Marshal(sig)
}

func (c JSONCodec) Decode(b []byte) (story.Signal[ArticlePayload], error) {
	var sig story.Signal[ArticlePayload]
	err := json.Unmarshal(b, &sig)
	return sig, err
}
```

### 2. Initialize the Tracker

Create a `Tracker[T]` instance with your store backend and codec:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story"
)

func main() {
	// Initialize your Store implementation (e.g. in-memory or KV backend)
	store := NewMyStoreBackend()

	cfg := story.Config[ArticlePayload]{
		Store:         store,
		Codec:         JSONCodec{},
		BatchWindow:   24 * time.Hour,
		BatchInterval: 30 * time.Minute,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()

	// Derive deterministic UUID v5 Signal ID using TrackerNamespace
	domainKey := "article-12345"
	sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(domainKey))

	sig := story.Signal[ArticlePayload]{
		ID:        sigID,
		At:        time.Now(),
		Embedding: []float32{0.12, -0.43, 0.88 /* ... */},
		Data: ArticlePayload{
			Title:  "Breaking News Event",
			Source: "Wire",
		},
	}

	// Ingest signal real-time
	storyID, err := tracker.Ingest(context.Background(), sig)
	if err != nil {
		log.Fatalf("ingest error: %v", err)
	}

	fmt.Printf("Signal ingested into provisional StoryID: %s\n", storyID)
}
```

### 3. Subscribe to Real-Time & Refinement Events

`Subscribe()` returns a caller-independent channel delivering real-time draft assignments and batch structural updates:

```go
events := tracker.Subscribe()

go func() {
	for ev := range events {
		switch ev.Kind {
		case story.EventDraftAssigned:
			fmt.Printf("[Real-time] Signal %s provisionally assigned to Story %s\n", ev.SignalID, ev.StoryID)
		case story.EventStoryMerged:
			fmt.Printf("[Batch] Story %s merged into surviving Story %s\n", ev.StoryID2, ev.StoryID)
		case story.EventStorySplit:
			fmt.Printf("[Batch] Story %s split into child Story %s\n", ev.StoryID, ev.StoryID2)
		case story.EventBatchComplete:
			fmt.Printf("[Batch] Re-clustering complete: %d created, %d merged, %d split\n",
				ev.BatchSummary.StoriesCreated, ev.BatchSummary.StoriesMerged, ev.BatchSummary.StoriesSplit)
		}
	}
}()
```

### 4. Traverse Stories and Signals (Go 1.22 Iterators)

Use Go 1.22 range-over-func iterators for zero-allocation traversal:

```go
// Iterate all Active stories
for meta := range tracker.Stories(story.StoryStateActive) {
	fmt.Printf("Active Story %s (Created: %s, Radius: %.4f)\n",
		meta.ID, meta.CreatedAt.Format(time.RFC3339), meta.Radius)

	// Traversal over signals belonging to story
	for sig, err := range tracker.SignalsOf(meta.ID) {
		if err != nil {
			log.Printf("error reading signal: %v", err)
			continue
		}
		fmt.Printf("  └─ Signal %s: %s\n", sig.ID, sig.Data.Title)
	}
}
```

---

## Configuration Reference

| Parameter | Default | Description |
|---|---|---|
| `BatchWindow` | `24h` | Sliding temporal window of signals fed into each HDBSCAN batch run. |
| `BatchInterval` | `30m` | Interval between background HDBSCAN re-clustering runs. |
| `SilenceWindow` | `7d` | Inactivity threshold before an Active story transitions to `Dormant`. |
| `ArchiveWindow` | `30d` | Inactivity threshold before a Dormant story transitions to `Archived`. |
| `ActiveContextWindow` | `30d` | How far back the `t:` time index anchors Draft-phase story lookup. |
| `OutlierTTL` | `2×BatchWindow` | Max outlier age relative to the last batch timestamp. |
| `MinClusterSize` | `3` | Fixed HDBSCAN minimum cluster size constraint. |
| `MinSamples` | `MinClusterSize` | HDBSCAN core-point density. |
| `BatchSampleCap` | `50,000` | Maximum signals processed per batch run; excess is sampled. |
| `SampleGuaranteeMaxFraction` | `0.5` | Max fraction of the sample cap reserved for per-story minimums. |
| `AssignmentK` | `2.0` | $\sigma$-multiplier for draft distance threshold $T_{\text{assign}}(\text{story})$. |
| `ColdStartMinSignals` | `5` | Signal count before a story's own $\sigma$ is trusted. |
| `SigmaFloor` | `0.1` | Per-story $\sigma$ floor as a fraction of $\sigma_{global}$. |
| `EMAAlpha` | `0.1` | EMA decay for $\sigma_{global}$ updates. |
| `MappingMinJaccard` | `0.6` | Jaccard threshold for primary cluster continuation. |
| `SplitMinJaccard` | `0.3` | Jaccard threshold for split/merge detection. |
| `IngestBufferCap` | `10,000` | In-memory staging channel capacity during active batch persistence transactions. |
| `EventBufferSize` | `512` | Per-subscriber event channel buffer depth. |

---

## Specifications & Development

For deep architectural design and component specifications:
- [Design Architecture (`DESIGN.md`)](DESIGN.md)
- [Spec-Driven Development Specs (`spec/README.md`)](spec/README.md)
