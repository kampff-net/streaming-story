# Streaming Story Tracker (`go.kvsh.ch/streaming-story`)

A Go library for ingesting a continuous stream of embedding vectors ("signals")
and grouping them into evolving clusters ("stories") — incremental topic
clustering for news-shaped data, with stable cluster identity over time.

Two phases:

1. **Draft phase** — real time. Each arriving signal is assigned to the nearest
   story centroid if it falls inside that story's adaptive radius, and held as an
   outlier otherwise. Low latency, provisional.
2. **Maintenance phase** — periodic, background. Existing stories are
   *maintained*, not re-derived: evict, promote, admit, split, merge, recentre,
   lifecycle. Story IDs therefore survive across runs, and a re-run over
   unchanged data changes nothing.

Distances are measured in **centred space**: the corpus mean is subtracted before
any comparison, which is what keeps embeddings from collapsing into a single
blob. Every threshold below is a centred-space distance —
see [Geometry](DESIGN.md#geometry-centred-space).

---

## Installation

Requires **Go 1.26.5+** (uses `iter.Seq` / `iter.Seq2` range-over-func
iterators).

```bash
go get go.kvsh.ch/streaming-story
```

---

## Concepts

- **Signal** — one input: UUID v5 ID, timestamp, one or more **facet**
  embeddings, and an opaque payload `T`. Dimensionality is fixed by the first
  ingest and shared by every facet.
- **Facet** — one `Embedding` of a signal, and the unit of assignment and
  geometry. A signal that means two things carries two facets rather than one
  averaged vector that sits between both and matches neither. A facet belongs to
  at most one story; a signal belongs to the union of its facets' stories, so
  membership is many-to-many. How an item is decomposed is the caller's
  judgment — the library never creates, merges, or drops a facet.
- **Story** — a persistent cluster. Carries `Centroid` (mean of all members, the
  identity geometry), `RecentCentroid` (mean of recent members, what admission
  compares against), radius, per-story statistics, and a lifecycle state.
- **Outlier** — a signal no story covers yet. Held in its own bucket for the next
  maintenance pass to promote, admit, or evict.
- **Lifecycle** — `Active → Dormant → Archived`, on `SilenceWindow` and
  `ArchiveWindow`. Dormant can reactivate; Archived is terminal. Signals are
  retained in every state.
- **Store** — any prefix-scannable KV store with lexicographic byte ordering
  (bbolt, LevelDB, …). `MemStore` ships for tests.

---

## Quickstart

### 1. Create a tracker

`MemStore` ships with the library for tests, and signals are persisted using canonical CBOR:

```go
package main

import (
	"context"
	"log"
	"time"

	story "go.kvsh.ch/streaming-story"
)

type Article struct {
	Title  string `cbor:"0,keyasint"`
	Source string `cbor:"1,keyasint"`
}

func main() {
	tracker, err := story.NewTracker(story.Config[Article]{
		Store:         story.NewMemStore(), // required; swap for bbolt/LevelDB in production
		BatchSchedule: "*/30 * * * *",
		OnBatchError:  func(err error) { log.Printf("batch: %v", err) },
	})
	if err != nil {
		log.Fatal(err)
	}
	defer tracker.Close()

	sig := story.Signal[Article]{
		ID: tracker.SignalID("article-12345"), // deterministic UUID v5
		At: time.Now(),
		// One vector per facet. A single-facet signal behaves exactly as a
		// signal did before facets existed.
		Embeddings: []story.Embedding{
			{0.12, -0.43, 0.88 /* ... the article text      */},
			{0.31, 0.07, -0.22 /* ... an image insight      */},
		},
		Data: Article{Title: "Breaking News Event", Source: "Wire"},
	}

	storyIDs, err := tracker.Ingest(context.Background(), sig)
	if err != nil {
		log.Fatal(err)
	}
	// storyIDs is empty when no story claimed any facet, and holds one entry
	// per story the signal's facets reached. Placements are provisional: the
	// next maintenance pass may move them.
	log.Printf("provisional stories: %v", storyIDs)
}
```

Use `tracker.SignalID(domainKey)` rather than `uuid.NewSHA1` directly — it
honours a configured `Namespace`. Deterministic IDs make re-ingesting the same
signal a no-op.

### 2. Subscribe to events

`Subscribe()` returns an independent, buffered channel per caller, closed when
the tracker closes:

```go
events := tracker.Subscribe()

go func() {
	for ev := range events {
		switch ev.Kind {
		case story.EventDraftAssigned:
			log.Printf("signal %s provisionally in story %s", ev.SignalID, ev.StoryID)
		case story.EventStoryCreated:
			log.Printf("story %s created", ev.StoryID)
		case story.EventStorySplit:
			log.Printf("story %s split off child %s", ev.StoryID, ev.StoryID2)
		case story.EventStoryMerged:
			log.Printf("story %s absorbed retired story %s", ev.StoryID, ev.StoryID2)
		case story.EventStoryRetired:
			log.Printf("story %s retired (emptied)", ev.StoryID)
		case story.EventBatchComplete:
			s := ev.BatchSummary
			log.Printf("batch: +%d stories, %d merged, %d split, %d promoted, %d admitted",
				s.StoriesCreated, s.StoriesMerged, s.StoriesSplit,
				s.OutliersPromoted, s.OutliersAdmitted)
		case story.EventBufferOverflow:
			log.Print("subscriber fell behind; events were dropped")
		}
	}
}()
```

| Event | Meaning |
|---|---|
| `EventDraftAssigned` | Real-time assignment; provisional. |
| `EventSignalReassigned` | A maintenance pass moved a signal (promotion, admission, split, or merge). |
| `EventStoryCreated` | New story persisted. |
| `EventStorySplit` | One story became two; `StoryID2` is the new child. |
| `EventStoryMerged` | Two became one; `StoryID2` is the retired ID. |
| `EventStoryRetired` | A pass emptied the story and deleted its record. |
| `EventStoryDormant` / `EventStoryArchived` | Lifecycle transitions. |
| `EventBatchComplete` | One per run; `BatchSummary` is populated. |
| `EventBufferOverflow` | Channel was full; events were dropped. |

A run touching many signals emits many `EventSignalReassigned` before
`EventBatchComplete`. Slow subscribers should consume `EventBatchComplete` for
coarse progress and query the store for detail, or raise `EventBufferSize`.

### 3. Read stories and signals

```go
for meta, err := range tracker.Stories(story.StoryStateActive) { // StoryStateAny for all
	if err != nil {
		log.Printf("read error: %v", err)
		continue
	}
	log.Printf("story %s: %d signals, radius %.3f, created %s",
		meta.ID, meta.SignalCount, meta.Radius, meta.CreatedAt.Format(time.RFC3339))

	for sig, err := range tracker.SignalsOf(meta.ID) {
		if err != nil {
			log.Printf("read error: %v", err)
			continue
		}
		log.Printf("  └─ %s: %s", sig.ID, sig.Data.Title)
	}
}
```

`Story(id)` fetches one story (`ErrNotFound` if absent). `Signal(id)` fetches one
signal from its canonical record, regardless of where — or whether — its facets
are placed.

Membership is traversable from either end, at two levels of detail:

| | identity level | facet level |
| :--- | :--- | :--- |
| signal → stories | `StoriesOf(signalID)` | `FacetsOfSignal(signalID)` |
| story → signals | `SignalsOf(storyID)` | `FacetsOfStory(storyID)` |

`SignalsOf` yields a member once however many facets it contributed;
`FacetsOfStory` yields every facet, which is the multiset the centroid and
radius are computed over. `FacetsOfSignal` reports unplaced facets with a nil
story, so a partially placed signal is legible.

`Signals()` iterates every signal in the store, placed or not, in signal-ID
order. It is a lossless dump: replaying it through `Ingest` against a fresh
store is a full rebuild that needs no access to the original source and no
re-embedding.

---

## Configuration Reference

`Config` has no dead fields: every parameter below is read by the running
tracker. Only `Store` is required, and every other field defaults —
a zero value always means "use the default", so no parameter can be *set* to
zero. `MeanRemoval: 0` yields 0.9; pass a small epsilon if you genuinely want
raw geometry (you do not — see [Tuning](#tuning)).

`validate()` runs at `NewTracker` and rejects incoherent combinations rather than
silently correcting them: `AssignThreshold` outside `(0, 1]`, `MergeThreshold` at
or above `AssignThreshold`, `SplitThreshold` at or below `MergeThreshold` or above
1, `MeanRemoval` outside `[0, 1]`, and `MinStorySize` below 2.

### Required

| Parameter | Effect |
|---|---|
| `Store` | The persistence backend. Must give lexicographic byte ordering — the range scans over the time index and story prefixes depend on it. `NewMemStore()` for tests. |

### Identity

| Parameter | Default | Effect |
|---|---|---|
| `Namespace` | `TrackerNamespace` | UUID v5 namespace root that `SignalID(domainKey)` derives from. Set it per tenant to isolate multi-tenant deployments. Changing it changes every derived ID, so the same domain key becomes a different signal. |

### Cadence and lifecycle

| Parameter | Default | Effect |
|---|---|---|
| `BatchSchedule` | `*/30 * * * *` | Cron schedule for when the maintenance pass runs (e.g. `*/30 * * * *`, `@hourly`, `@every 30m`). **This is the only thing that promotes, admits, splits, or merges** — the Draft phase alone never restructures anything. Run it more frequently for faster structural correction, at proportional CPU cost. |
| `BatchWindow` | `24h` | Reference span for outlier retention, and nothing else: it sets `OutlierTTL`'s default to `2×BatchWindow`. It does **not** bound clustering input — story membership is read in full on every pass, because the lifetime centroid is the mean of every member. Set `OutlierTTL` directly and this parameter stops mattering. |
| `OutlierTTL` | `2×BatchWindow` | How long an unmatched signal is kept before eviction, measured against the **last batch timestamp** rather than wall clock, so a long maintenance pause cannot trigger mass eviction. Raise it to give sparse topics more time to accumulate `MinStorySize` corroborating signals. |
| `SilenceWindow` | `7d` | Inactivity before Active → Dormant. A Dormant story keeps its centroid, can still be a merge target, and can reactivate through Draft assignment. |
| `ArchiveWindow` | `30d` | Inactivity before Dormant → Archived, which is terminal: never collected, never an anchor, never reactivated. Signals are retained regardless and stay iterable. |
| `ActiveContextWindow` | `ArchiveWindow` | Two effects. It bounds how far back the time index is scanned for Draft anchors, so stories quieter than this stop admitting new signals; and it defines which members make up `RecentCentroid`. Shorten it to make stories track current coverage more tightly and go quiet sooner. |

### Geometry and thresholds

Every distance here is a **centred-space** cosine distance — the corpus mean is
subtracted before anything is measured — which puts the scale at roughly twice
raw cosine. A threshold copied from a raw-cosine configuration will be far too
tight.

| Parameter | Default | Effect |
|---|---|---|
| `MeanRemoval` | `0.9` | Fraction of the corpus mean subtracted before any distance. The most consequential parameter in the file: at `0` the geometry is raw, unrelated stories look identical, and the reference corpus collapses to 2 stories with a 581-signal blob; at `1.0` a corpus that is itself one tight group shatters into antipodal halves. Raise toward 0.95 to sharpen separation on a diverse corpus, lower toward 0.8 for stability on a narrow one. |
| `AssignThreshold` | `0.50` | Three roles: the maximum distance for a signal to join a story, the radius a promoted group must fit inside, and the hard ceiling on each story's adaptive radius — so a story that has drifted wide cannot keep widening its own catchment. Raising it increases coverage and lowers story count. |
| `MergeThreshold` | `0.40` | At or below this centroid distance two stories are treated as one, provided the union is not something the split step would immediately cut. Raise it to reunite a fragmented topic; must stay below `AssignThreshold`, since stories may not merge at a distance wider than a signal may sit from a centroid. |
| `SplitThreshold` | `0.55` | Above this best-partition distance a story divides in two. Must exceed `MergeThreshold`: the gap between them is the hysteresis band, and narrowing it lets a merge and a split undo each other along different seams, churning story IDs while the data sits still. |
| `MinStorySize` | `3` | How much corroboration makes a story: it gates outlier promotion and both sides of every split. Raise it for high-frequency sources; below 2 is rejected, since a one-signal story is an outlier by another name. |

### Draft-phase admission

A story's admission radius is `MeanDistance + AssignmentK × σ`, clamped to
`AssignThreshold`. These four parameters shape that expression, and the same
expression governs outlier admission during maintenance.

| Parameter | Default | Effect |
|---|---|---|
| `AssignmentK` | `2.0` | σ multiplier for the radius. Higher admits more freely; this is the knob for "how far outside its usual spread may a story reach". |
| `ColdStartMinSignals` | `5` | Members a story needs before its own σ is trusted. Below it the radius is `AssignmentK × σ_global`, so young stories borrow the corpus-wide spread instead of their own unreliable one. |
| `SigmaFloor` | `0.1` | Floors a story's σ at this fraction of σ_global, so a story whose first signals are near-identical cannot collapse its radius to zero and stop admitting its own coverage. Applies after cold-start too. |
| `InitialSigmaGlobal` | `0.25` | Stand-in for σ_global until a pass measures one. Narrow reach: a story can only exist after a pass, and a pass seeds σ_global, so this only sets the admission radius during the very first pass, for the stories that pass just created. |
| `EMAAlpha` | `0.1` | Weight given to the **previous** σ_global when a pass updates it: `σ_global ← EMAAlpha×σ_global + (1−EMAAlpha)×mean_this_pass`. The default therefore tracks the newest measurement at 90% rather than smoothing. Raise toward 1 to make σ_global sluggish. |

### Concurrency and observability

| Parameter | Default | Effect |
|---|---|---|
| `IngestBufferCap` | `10,000` | Signals held in memory while a pass owns the write transaction. A full buffer applies back-pressure — `Ingest` blocks until space frees or `ctx` is cancelled — and buffered signals are lost if the process dies before the drain, which is the library's one **at-most-once** window. |
| `EventBufferSize` | `512` | Per-subscriber channel depth. On overflow the subscriber receives `EventBufferOverflow` in place of the dropped events, so a slow consumer degrades visibly rather than silently. |
| `OnBatchError` | `nil` | Called with any error that aborts a pass. A failed pass leaves the store untouched and the next tick retries, so **this is the only way to observe batch failures**. Runs on the batch goroutine; must not block. |

---

## Tuning

Defaults are calibrated for low-to-medium frequency news (1–10 signals/day per
topic) against a 596-signal reference corpus of roughly 20–30 topics, where they
produce 32 stories with a largest of 28 and no drift across repeated runs.

| Symptom | What to change |
|---|---|
| Too many signals stuck as outliers | Raise `AssignThreshold`, or lower `MeanRemoval` toward 0.8. Both trade cluster count for coverage: on the reference corpus 0.8 gives 34 stories over 260 signals against 32 over 234 at 0.9. |
| Clusters too coarse — unrelated topics together | Raise `MeanRemoval` toward 0.95, or lower `AssignThreshold` and `MergeThreshold`. |
| Clusters too fragmented — one topic in several stories | Raise `MergeThreshold` (keeping it below `AssignThreshold`), or lower `MinStorySize`. |
| One story swallowing everything | `MeanRemoval` is too low. This is the anisotropy collapse; at `0` the reference corpus becomes 2 stories with a 581-signal blob. |
| Story IDs churn between runs | `SplitThreshold` and `MergeThreshold` are too close. Widen the hysteresis band. |
| High-frequency source (social, metrics) | Reduce `BatchWindow` (e.g. 30m), adjust `BatchSchedule` (e.g. `@every 5m`), `SilenceWindow` (6h), `ArchiveWindow` (7d); raise `MinStorySize` (5–10). |
| Structure corrects too slowly | Run `BatchSchedule` more frequently. Maintenance is the only thing that splits, merges, promotes, or admits. |
| Assignments shuffling between stories | Give the first pass more to work with. Measured on the reference corpus, a 400-signal seed followed by increments of 50, 20, or 5 produces **zero** churn, while a 300-signal seed produces 0.4–1.0%: a seed too small for the topic count leaves the early stories unrepresentative. |

Recalibrating for a different embedding model or source mix means re-measuring:
the corpus probe (`CORPUS=path go test -run TestCorpusProbe -v .`) reports story
count, size distribution, and outlier fraction per pass, and
`TestStreaming_IncrementalArrivalsAreStable` reports churn under incremental
arrival.

---

## Further Reading

- [`DESIGN.md`](DESIGN.md) — architecture, algorithms, key schema, concurrency
  model, and the reasoning behind each rule.
- [`HISTORY.md`](HISTORY.md) — approaches that were implemented, measured, and
  removed, and why. Read before reintroducing one.
- [`spec/`](spec/README.md) — spec-driven development specifications.
