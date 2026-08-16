# SDD Spec: Centroid-Based Incremental Clustering

## Metadata
* **Status:** `DESIGN`
* **Author:** Consigliere
* **Created:** 2026-08-16
* **Last Updated:** 2026-08-16
* **Approver:** Codefather

---

## Phase 1: Proposal (Rough Idea)

### 1.1 Problem Statement

The batch phase re-clusters the whole window with HDBSCAN on every run and then
tries to recover story identity from anonymous cluster labels. Two properties of
that design make story tracking unstable in production.

**Chaining.** HDBSCAN builds a minimum spanning tree over mutual-reachability
distances — single linkage with a density correction. News embeddings chain: A
relates to B, B relates to C, A does not relate to C. Measured on a live corpus
of 596 signals (Gemini 3072-dim embeddings), grouping by transitive closure at
cosine distance 0.25 produces one component holding **324 of 596 signals**; at
0.30 it holds 469. Nearest-centroid grouping over the identical vectors at the
identical thresholds produces a largest group of 22 and 39 respectively. The
observed symptom — one huge low-density story absorbing unrelated coverage, with
the stories it absorbed retiring empty — is a chaining artifact.

It is not a dimensionality artifact. The same corpus measures an intrinsic
dimension of 4.6 (TwoNN), pairwise distance spread σ/μ of 0.21, and relative
contrast of 1.77. Distances are informative; the linkage is what fails. No
parameter setting removes a structural property of the linkage: raising
`MinClusterSize` from 3 to 8 cut the corpus to 2 clusters and 88% noise while
leaving the largest cluster at 35 points. `ClusterSelectionLeaf` (spec-less,
added 2026-08-16) cuts chains lower in the tree and helps materially at
`MinClusterSize` 3 — largest cluster 40 → 14 — but it does not stop chains
forming.

**Non-monotone identity.** Each run discovers clusters from scratch, so cluster
labels carry no continuity. The entire two-phase mapping engine (spec 003) —
Hungarian assignment over a Jaccard cost matrix, N-way merge and split
detection, survivor rules, key-space migration — exists to reconstruct identity
the clusterer discarded. Because the density landscape is global, signals
arriving anywhere can change any story's membership, and because the window is
sampled (`BatchSampleCap`), two runs over the same store can disagree. A story
can therefore split, merge, or retire on a run in which it received no new
signals at all.

The cost of doing nothing: story identity is the product. A tracker whose
stories dissolve and reappear under new IDs cannot support summarisation,
alerting, or any downstream consumer that holds a story ID.

### 1.2 Proposed Solution

Stop re-deriving stories. Make the batch phase **maintenance** rather than
re-clustering.

Story identity becomes primary and permanent rather than a per-run inference.
Assignment continues to work as the Draft phase already does — nearest story
centroid within a threshold — which never chains, because a signal is compared
to a story's centroid and never transitively to its neighbour's neighbour.

The batch run performs a small set of bounded operations: promote groups of
outliers into new stories, split stories that have grown into two, merge stories
that have converged into one, recentre statistics, and advance lifecycle states.
It does not reassign settled signals except through a split or a merge.

Merge and split are the two directions of one question — are these one story or
two? — separated by a hysteresis band. Two stories merge when their centroids
fall to `MergeThreshold`; one story splits when its best internal partition
exceeds the strictly larger `SplitThreshold`. Between the two, nothing happens.
The band is what stops a merge and a split from undoing each other run after
run (§2.2).

### 1.3 Scope & Requirements

* **In Scope:**
  * Replace `runBatchCore`'s cluster/map/apply pipeline with a maintenance pass.
  * Threshold-based merge of converged stories.
  * Threshold-based split of diverged stories, gated by a sound radius test.
  * Outlier promotion into new stories via mutual-neighbour groups.
  * Centroid, radius, and σ recomputation from current membership, with a
    separate recency centroid for admission (§2.7).
  * Retire `internal/hdbscan` and `internal/hungarian` and the spec 003 mapping
    engine.
  * Config: `AssignThreshold`, `MergeThreshold`, `SplitThreshold`,
    `MinStorySize`; deprecate the HDBSCAN and Jaccard knobs.
  * Preserve the KV key schema, event model, dormancy/archival, outlier TTL,
    and every public accessor.
* **Out of Scope:**
  * Reassigning individual signals between existing stories. Membership changes
    only through split or merge, which move groups (§2.6).
  * Changing the embedding pipeline, the distance metric, or the KV schema.
  * Dimensionality reduction. The measurements show it is unnecessary.
  * N-way splits in a single pass. A story splits in two per run; a story
    containing three groups resolves over successive runs (§2.4).

---

## Phase 2: System Design (SDD)

### 2.1 Architecture & Components

#### The Stability Invariants

The design is defined by four properties. Every mechanism below exists to hold
one of them, and a future change that breaks one is a regression regardless of
what it improves.

> 1. **Bounded revision.** Membership changes only through split or merge, both
>    of which move whole groups on explicit threshold-crossing evidence and emit
>    an event. No signal is silently relocated, and no story is reconstructed
>    from scratch.
> 2. **Local.** A signal arriving in story X cannot alter story Y's membership.
>    There is no global density landscape.
> 3. **Deterministic.** The same store state yields the same outcome,
>    independent of sampling.
> 4. **Non-chaining.** Membership is decided against a story centroid, never
>    transitively through another signal.

Today's design holds none of them.

Invariant 1 is deliberately weaker than strict monotonicity. An earlier draft of
this spec forbade splits outright to keep membership append-only, which is the
stronger guarantee — but it leaves a story that has genuinely diverged with no
remedy, and pushes the correction onto whoever consumes the story. Allowing
split and merge, and *only* those, keeps the guarantee that matters: a story's
membership changes only for a stated, observable, event-emitting reason.

```mermaid
graph TD
    subgraph Draft["Draft phase — unchanged"]
        ING["Ingest(signal)"] --> NEAR["nearest story centroid<br/>(Tier 1/2/3 lookup)"]
        NEAR -->|"d <= T_assign(story)"| ASSIGN["assign to story"]
        NEAR -->|"otherwise"| OUT["outlier bucket"]
    end

    subgraph Maintenance["Batch phase — replaces re-clustering"]
        TICK["BatchInterval tick"] --> EVICT["1. evict stale outliers"]
        EVICT --> PROMOTE["2. promote outlier groups<br/>(mutual neighbours, size >= MinStorySize)"]
        PROMOTE --> SPLIT["3. split diverged stories<br/>(best partition > T_split)"]
        SPLIT --> MERGE["4. merge converged stories<br/>(centroid distance <= T_merge)"]
        MERGE --> RECENTRE["5. recentre: centroids, radius, sigma, sigma_global"]
        RECENTRE --> LIFECYCLE["6. lifecycle: Active → Dormant → Archived"]
        LIFECYCLE --> EMIT["emit events + BatchSummary"]
    end
```

#### What Is Removed

| Component | Fate |
|---|---|
| `internal/hdbscan` | Deleted. No caller remains. |
| `internal/hungarian` | Deleted. No caller remains. |
| `mapClusters`, `clusterMapping`, `jaccardIndex`, `coverageIndex` | Deleted with the mapping engine (spec 003 superseded). |
| `clusterSignals`, `sampleSignals`, `sampleGroups` | Deleted. Maintenance is O(stories²) on centroids, so windowed sampling is unnecessary. |
| `MinClusterSize`, `MinSamples`, `ClusterSelection`, `MaxClusterSize`, `MappingMinJaccard`, `SplitMinJaccard`, `BatchSampleCap`, `SampleGuaranteeMaxFraction` | Deprecated (§2.6). |

#### What Is Preserved

The KV key schema in full (`c:`, `s:`, `o:`, `l:`, `t:`), the `Store`/`Tx`/
`Codec` contracts, `Ingest` and its buffered-apply behaviour, `Subscribe`,
`Story`, `Stories`, `SignalsOf`, `Signal`, `Outliers`, `SignalID`, the event
enum, dormancy and archival windows, outlier TTL semantics, and the σ_global EMA
calibration. This is an algorithm replacement inside `runBatchCore`, not an API
rewrite.

### 2.2 Thresholds and Their Calibration

Two thresholds in cosine distance, both in `[0, 2]` but practically `[0, 1]` for
normalised text embeddings, plus one size floor.

| Name | Default | Meaning |
|---|---|---|
| `AssignThreshold` | 0.28 | Maximum centroid distance for a *signal* to join a story. |
| `MergeThreshold` | 0.22 | At or below this centroid distance, two stories are one story. |
| `SplitThreshold` | 0.30 | Above this centroid distance, one story's two parts are two stories. |
| `MinStorySize` | 3 | Signals a group must hold to exist as a story. |

Measured on the reference corpus: median 1-NN distance 0.20, median pairwise
distance 0.45. The valley between them is where a threshold belongs, and 0.28
sits in it. Nearest-centroid grouping at that threshold yields 335 groups with a
largest of 22 — no blob, no cliff.

#### Two Thresholds, One Hysteresis Band

Merge and split are the two directions of the same question — "are these one
story or two?" — but they are **not** exact complements, and giving them a
single shared threshold makes the system churn.

The asymmetry is in what each one measures:

- **Merge** evaluates one specific, historically-given partition: the two
  stories that happen to exist.
- **Split** *searches* for the best 2-way partition of a story's members.

A best-of-all-partitions search can beat a historically-given partition on the
same signals. Two stories whose centroids sit 0.21 apart merge; the merged
story's best internal cut may run along a different seam at 0.23 and split
again — into two stories that are not the two that merged. Nothing in the data
converged or diverged; the story IDs churned because a search was compared
against a fixed arrangement using one number.

Hence a band, with the split edge strictly above the merge edge:

| | Condition | Default |
|---|---|---|
| **Merge** | story centroid distance **≤ `MergeThreshold`** | 0.22 |
| *(neither)* | between the two — leave as-is | — |
| **Split** | best-partition centroid distance **> `SplitThreshold`** | 0.30 |

The band between 0.22 and 0.30 is the stable region: a configuration of stories
sitting inside it is left alone, whichever way it got there. After a split at
separation *s* > 0.30, merge needs ≤ 0.22 and declines. After a merge at
*d* ≤ 0.22, a split needs a cut better than 0.30 — a margin of 0.08 better than
the arrangement just merged, which a mere re-seam cannot supply.

`validate()` enforces `MergeThreshold < SplitThreshold`. Setting them equal is
the single-threshold design above and is rejected at startup rather than
silently permitted, because the resulting churn is subtle and shows up only as
unexplained story-ID turnover in production.

**Sizing the gap.** The band must be wider than the typical gain a partition
search extracts over an arbitrary cut. 0.08 in cosine distance is roughly one
standard deviation of the corpus's pairwise distribution (σ = 0.096), which is a
defensible starting point but is the parameter most worth revisiting once
`EventStorySplit` and `EventStoryMerged` rates are observable. Repeated
split-then-merge on the same signals means the gap is too narrow.

#### Why Assignment Keeps Its Own Threshold

Assignment is not folded into the merge/split band because it compares
different objects. Assignment measures **one point** against a centroid; merge
and split measure **two centroids** against each other. A single signal is a
noisy sample and needs a wider tolerance than an averaged centroid, so the
natural scales differ — hence 0.28 for admitting a signal against 0.22 for
declaring two stories the same.

The ordering that must hold is `MergeThreshold < AssignThreshold`. If stories
merged at a distance wider than a signal may sit from a centroid, two stories
could unify while their own members were too far apart to have joined either —
incoherent. `SplitThreshold` is deliberately *not* bounded by `AssignThreshold`:
it measures two centroids, not a point against one, and the default 0.30 sits
above `AssignThreshold` 0.28 on purpose, so that a story is cut only when its
halves are further apart than any single signal was ever permitted to stray.
`validate()` enforces `MergeThreshold < SplitThreshold` and
`MergeThreshold < AssignThreshold`.

The existing adaptive per-story rule — `T_assign(story) = mean_distance +
AssignmentK × σ(story)`, with cold-start fallback to σ_global — is retained and
takes precedence once a story has at least `ColdStartMinSignals` members.
`AssignThreshold` is the cold-start floor and the absolute ceiling: a story's
adaptive threshold is clamped to it, so an unusually diffuse story cannot widen
its own catchment without bound. Clamping is what stops the adaptive rule from
recreating the blob by a different route.

### 2.3 Outlier Promotion

Outliers accumulate when no story is near enough. Promotion turns a group of
mutually-near outliers into a new story.

1. Collect outliers within `OutlierTTL` of `lastBatchTimestamp` (unchanged).
2. Build the mutual-neighbour graph over them: an edge where cosine distance
   ≤ `AssignThreshold`.
3. Take **maximal cliques** rather than connected components, capped by a
   greedy approximation: repeatedly pick the outlier with the most neighbours,
   form a group from it and its neighbours that are also mutually within
   threshold, remove them, repeat.
4. A group of at least `MinStorySize` becomes a new story via the existing
   `createStory` path; `EventStoryCreated` is emitted.
5. Ungrouped outliers stay outliers.

**Why cliques and not components.** Connected components over outliers is
exactly the transitive linkage that produced the 324-signal blob. Requiring
mutual proximity keeps a new story compact from birth. The greedy approximation
is used because maximal-clique enumeration is exponential; determinism is
preserved by breaking ties on signal ID.

Promotion runs **before** split and merge, so a promoted story that turns out
to sit on top of an existing one is merged in the same run rather than
lingering as a near-duplicate until the next.

### 2.4 Split

A story splits when it has come to hold two groups that `SplitThreshold` says
are different stories.

#### The Radius Gate

Evaluating a partition for every story on every run is wasteful, so a cheap
necessary condition runs first:

> Attempt a split only when `radius > SplitThreshold / 2`.

This gate is **sound**, not heuristic. Every member lies within `radius` of the
story centroid, so any two subsets of members have centroids inside that same
ball, and two points in a ball of radius *r* are at most *2r* apart. If
`2 × radius ≤ SplitThreshold`, no partition can produce parts separated by more
than `SplitThreshold`, and the split test would necessarily fail. Skipping those
stories cannot miss a split.

`radius` is already maintained on `StoryMeta` and recomputed during recentre, so
the gate costs one comparison per story.

#### The Partition Test

For each story past the gate:

1. Partition its members in two by **2-medoid** (k-medoids, k=2): seed with the
   two mutually most-distant members, assign every member to the nearer seed,
   then re-select each part's medoid and reassign until stable or 10 iterations
   pass. Medoids rather than means because a medoid is an actual signal, making
   the partition reproducible and immune to the centroid of an elongated group
   landing in empty space between its ends.
2. Compute each part's **centroid** — the same quantity merge compares, so the
   two operations speak in one measure.
3. Accept the split only if **both** hold:
   - each part has at least `MinStorySize` members, and
   - the distance between the two part centroids is **> `SplitThreshold`**.
4. Otherwise leave the story intact.

Both conditions matter. Without the size floor, a split peels a single outlying
signal off as its own "story", which is what `MinStorySize` exists to prevent
(§2.3) and would be inconsistent to allow here. Without the distance condition,
a merely *diffuse* story — one broad topic with no internal gap — gets cut in
half arbitrarily, and the halves sit inside the hysteresis band where nothing
reunites them.
Diffuse is not the same as bimodal, and only bimodal justifies a split.

A story failing on the size floor but passing on distance is worth noticing: it
holds a small distinct pocket that is not yet big enough to stand alone. It stays
put and is re-tested each run as the pocket grows. No state is needed for this —
the test is recomputed from membership every time.

#### Identity and Events

**The larger part keeps the story ID**; the smaller becomes a new story with a
fresh ID and `CreatedAt = now`. Ties break toward the part holding the older
signal, so the outcome does not depend on iteration order. Keeping the ID with
the majority preserves continuity for most downstream holders of that ID.

`EventStorySplit` is emitted with `StoryID` = the retained parent, `StoryID2` =
the new child — matching the existing enum documentation, which is now reachable
rather than dead.

Signals moved to the child migrate by key-space rewrite, exactly as merge
migrates in the other direction, and **the `BatchWindow` stability rule does not
apply**: a split moves every member including signals older than the window,
because leaving historical signals behind would put material in a story whose
centroid no longer represents it. This mirrors the merge exception already
documented in spec 003 and `AGENTS.md`.

#### One Split Per Story Per Run

A story is split at most once per batch run, into exactly two parts. A story
holding three distinct groups therefore takes two runs to resolve fully: the
first run separates it into two, the second splits whichever part is still
bimodal.

The alternative — recursing until no part splits — is rejected. Recursion is
re-clustering by another name: it derives an unbounded number of stories from
one run's view of the data, which is the behaviour this spec exists to remove.
Converging over successive runs keeps each run's change small, observable, and
bounded by one `EventStorySplit` per story.

### 2.5 Merge

For every ordered pair of Active stories whose centroids are within
`MergeThreshold`, merge. Iteration is over story IDs sorted lexicographically,
so the result does not depend on map iteration order.

Merging is transitive within a run: A–B and B–C merge into one story. This is
the one place chaining is permitted, and it is bounded — the merge graph is
built over story centroids, of which there are orders of magnitude fewer than
signals, and `MergeThreshold` is the strict threshold. A run that merges A, B,
and C into one story cannot cascade further, because the merged story's centroid
is recomputed only after the merge set is closed.

Survivor rule is unchanged from spec 003: **the oldest `CreatedAt` survives**.
Signals migrate by key-space scan, including signals older than `BatchWindow`;
this remains the documented exception to the stability rule. `EventStoryMerged`
is emitted with `StoryID` = survivor, `StoryID2` = retired.

**Merge is the only operation that destroys a story ID.** Split creates one;
nothing else removes one. `EventStoryRetired`
therefore fires only when a merge empties a story, never as a side effect of
re-clustering. In practice, since the merge migrates the full key space, the
retired story's record is deleted as part of the merge and reported as
`EventStoryMerged`; `EventStoryRetired` becomes reachable only through the
degenerate case of a story whose signals were all evicted.

### 2.6 Membership Stability

No signal is ever moved between stories individually. Membership changes only
by split or merge, both of which relocate a whole group and emit an event.

This is the sharpest departure from the current design. Today `applyBatch` moves
every sampled window signal to whatever story its cluster label mapped to, which
is how a weak peripheral member of a broad cluster is absorbed into a story it
never belonged to, and how the story it came from ends up empty and retired.

The cost is honest and worth stating: an individual misassignment is not
individually correctable. A signal assigned when a story was young and its
centroid unrepresentative stays there unless the story later splits or merges.
Three things bound the damage:

- Assignment is against a centroid with a strict threshold, so a misassignment
  requires genuine proximity at assignment time.
- Merge fixes the case where the signal really belonged to a neighbouring story
  that has since converged with this one.
- Split (§2.4) fixes the case where enough such signals accumulate to form a
  distinct group — which is precisely when the error becomes worth correcting.
  A single misplaced signal never crosses `MinStorySize` and is left alone,
  which is the right outcome: churning one signal between stories every run is
  the instability this design removes.

`EventSignalReassigned` remains in the enum and is emitted only for outlier
promotion (`StoryID` = the new story), split migration (`StoryID` = the child),
and merge migration (`StoryID` = the survivor).

### 2.7 Centroid Maintenance

A story's centroid is recomputed on every run, but *which* centroid and *over
what* are separate questions, and the answers differ by purpose.

#### Two Centroids

`StoryMeta` carries two vectors:

| Field | Computed over | Used for |
|---|---|---|
| `Centroid` | **all** members, unweighted mean | merge and split geometry, radius, σ |
| `RecentCentroid` | members with `At >= now - ActiveContextWindow`, unweighted mean | Draft-phase admission |

Admission compares an arriving signal against `RecentCentroid`, so a developing
story keeps admitting current coverage as it moves from the event to its
aftermath. Merge and split compare `Centroid`, so the identity decisions rest on
a story's whole history and do not swing with the last few days of traffic.

Using one vector for both is the tempting simplification and it fails in both
directions. A lifetime-only centroid anchors admission on the story's opening
coverage, so late developments drift out of catchment and get filed as new
stories — over-fragmentation. A recency-only centroid lets a story walk: each
run admits what is near the current centre, which moves toward what was just
admitted, and over months the story becomes a different story under the same ID.
That is concept drift, and it is worse than fragmentation because it is silent.

A story with no members inside `ActiveContextWindow` — every Dormant story, by
definition — has `RecentCentroid = Centroid`. That replaces the existing frozen
`FrozenMeanDistance` / `FrozenSigma` mechanism's role for the centroid
specifically; the frozen dispersion statistics are retained unchanged.

#### Recompute, Never Accumulate

Both centroids are **recomputed from current membership each run**, as a pure
function of stored state. Neither is updated incrementally.

This is a hard requirement of invariant 3, not a style preference. An
incrementally-accumulated centroid — the natural `c += (x - c) / n` running mean
or an EMA over arrivals — depends on the order signals arrived and on how many
runs have elapsed, so two stores holding identical signals can carry different
centroids. Every threshold comparison downstream then differs too, and the same
data yields different stories. Recomputation costs `O(members × D)` per story
per run, which the resource table (§2.9) already accounts for, and buys
reproducibility outright.

The same rule governs `radius`, `mean_distance`, and per-story σ: all are
recomputed from members against the freshly computed `Centroid`. σ_global keeps
its existing EMA — it is a global calibration constant, not a per-story identity
input, and its smoothing is deliberate.

#### Interaction With Split

Because `Centroid` is the unweighted lifetime mean, a story that genuinely
develops in a new direction grows its radius rather than sliding its centre.
That is the intended coupling: growth past the radius gate puts the story in
front of the split test (§2.4), which either finds a real bimodal seam and cuts
it, or finds none and leaves a legitimately broad story alone.

The alternative — letting the centroid slide so radius stays small — would hide
exactly the signal the split test needs. Drift is not something to absorb
quietly; it is evidence, and the design routes it to the operation that acts on
it.

#### Rejected: Age-Weighted Centroid

A single centroid with exponential age weighting (`w = exp(-Δt / halfLife)`)
would interpolate between the two behaviours with one vector instead of two. It
is rejected for now on two grounds: it adds a half-life constant that has no
measured basis in the corpus, and it makes merge and split comparisons
time-dependent, so two stories can merge or split because time passed rather
than because signals arrived. Recomputed-from-members weighting would at least
stay deterministic, so this remains a viable refinement if the two-centroid
split proves clumsy in practice — but it should be driven by an observed
problem, not adopted upfront.

### 2.8 Configuration

```go
// Clustering thresholds, in cosine distance.

// AssignThreshold is the maximum centroid distance for a signal to join a
// story. It is also the cold-start value and the ceiling for the adaptive
// per-story threshold (default: 0.28).
AssignThreshold float64

// MergeThreshold is the centroid distance at or below which two stories are
// the same story and are merged (default: 0.22).
MergeThreshold float64

// SplitThreshold is the centroid distance above which a story's best two-way
// partition is two stories and is split (default: 0.30).
//
// It must exceed MergeThreshold. The band between them is hysteresis: merge
// tests one historically-given partition while split searches for the best
// one, so equal thresholds let a merge and a split undo each other along
// different seams, churning story IDs without the data having changed.
SplitThreshold float64

// MinStorySize is the number of signals a group must hold to exist as a
// story (default: 3). It gates outlier promotion and both sides of a split;
// a smaller group stays in the outlier bucket or stays put.
MinStorySize int
```

`validate()` enforces:

- `0 < AssignThreshold <= 1`
- `0 < MergeThreshold < AssignThreshold`
- `MergeThreshold < SplitThreshold <= 1`
- `MinStorySize >= 2` — a one-signal "story" is an outlier by another name.

`MinStorySize` is deliberately retained from the HDBSCAN design, where it was
`MinClusterSize`. It is the answer to "how much corroboration before this is a
story?", which is a product question independent of the clustering algorithm,
and it applies uniformly: a group of outliers is not promoted below it, and a
split is not accepted if either side would fall below it.

**Deprecation.** `MinClusterSize`, `MinSamples`, `ClusterSelection`,
`MaxClusterSize`, `MappingMinJaccard`, `SplitMinJaccard`, `BatchSampleCap`, and
`SampleGuaranteeMaxFraction` become unused. They are **retained as fields**,
marked `Deprecated:` in godoc, and ignored — removing them would break every
caller's compilation for no benefit, and `magic-giant` sets most of them. A
future major version removes them.

`ClusterSelection` and `MaxClusterSize` were added on 2026-08-16 to mitigate
chaining under EOM extraction. This spec supersedes them within one day of
their introduction; that is the correct outcome of the measurement that
followed, and the leaf-extraction work remains valid for any caller still using
`internal/hdbscan` directly — which, after this spec, is nobody.

### 2.9 Resource Impact

Per batch run, with `S` stories, `O` in-window outliers, `D` embedding
dimension:

| Phase | Current | Proposed |
|---|---|---|
| Clustering | O(N² D) MST over sampled window | none |
| Mapping | O(C² S) Hungarian + Jaccard | none |
| Merge | — | O(S² D) over centroids |
| Promotion | — | O(O² D) over outliers |
| Split | — | O(M² D) per gated story, M = its member count |
| Recentre | O(N D) | O(N D) — two centroids, same pass |

Only stories past the radius gate (§2.4) pay the split cost, and the gate is
sound, so a compact story costs one comparison. A pathological story holding
most of the corpus would cost a pairwise pass over its members — but such a
story is exactly what this design prevents, and it is split on the run it
appears.

`S` and `O` are one to three orders of magnitude below `N` in the reference
corpus (596 signals, ~30 stories, ~170 outliers), so the batch run gets
materially cheaper and `BatchSampleCap` stops being necessary. No allocation
budget applies — the batch goroutine is off the ingestion path, and `Ingest`
continues to be buffered during the apply window.

Latency budget is unchanged: a run must finish well inside `BatchInterval`.
Verified by the existing `bench_test.go`, extended with a merge/promotion
benchmark at 10k signals.

---

## Phase 3: Implementation Plan (IP)

### 3.1 Task Breakdown

- [ ] **Task 1:** Add `AssignThreshold`, `MergeThreshold`, `SplitThreshold`,
      `MinStorySize` with defaults and validation (including
      `MergeThreshold < SplitThreshold`); mark superseded fields `Deprecated:`.
  - **Files:** `config.go`, `config_test.go`
  - **Verification:** `go test -run TestConfig ./...`

- [ ] **Task 2:** Clamp the adaptive per-story threshold to `AssignThreshold`
      and use it as the cold-start floor.
  - **Files:** `tracker.go` (`calcThreshold`), `tracker_test.go`
  - **Verification:** `go test -run TestCalcThreshold ./...`

- [ ] **Task 3:** Add `RecentCentroid` to `StoryMeta` and its persistence;
      recompute both centroids from members during recentre; point Draft
      admission at `RecentCentroid`.
  - **Files:** `types.go`, `tracker.go`, `batch.go`, `persist.go`,
    `snapshot.go`, `tracker_test.go`, `snapshot_test.go`
  - **Verification:** `go test -run 'TestCentroid|TestSnapshot' ./...` (covers:
    recompute is order-independent over shuffled insertion; a story with no
    recent members has `RecentCentroid == Centroid`; a developing story keeps
    admitting current coverage that a lifetime centroid would reject)

- [ ] **Task 4:** Implement outlier promotion by greedy mutual-neighbour
      cliques.
  - **Files:** `batch.go`, `batch_test.go`
  - **Verification:** `go test -run TestPromote ./...` (covers: clique beats
    component on a chained fixture, `MinStorySize` boundary, tie-break
    determinism)

- [ ] **Task 5:** Implement the split: radius gate, 2-medoid partition,
      `MinStorySize` and `SplitThreshold` acceptance, larger-part-keeps-ID.
  - **Files:** `batch.go`, `batch_test.go`
  - **Verification:** `go test -run TestSplit ./...` (covers: bimodal story
    splits; diffuse story of equal radius does not; either part below
    `MinStorySize` blocks it; radius gate never skips a story that would have
    split; larger part keeps the ID with deterministic tie-break; historical
    signals migrate; one split per story per run)

- [ ] **Task 6:** Implement threshold merge with the oldest-survives rule,
      reusing the spec 003 key-space migration.
  - **Files:** `batch.go`, `batch_test.go`
  - **Verification:** `go test -run TestMerge ./...` (covers: transitive A–B–C,
    survivor selection, historical signal migration, determinism under shuffled
    story order)

- [ ] **Task 7:** Rewrite `runBatchCore` as the six-step maintenance pass;
      delete `clusterSignals`, `mapClusters`, `sampleSignals`, and the Jaccard
      helpers.
  - **Files:** `batch.go`, `batch_test.go`, `tracker_behavior_test.go`
  - **Verification:** `go test ./...`

- [ ] **Task 8:** Delete `internal/hdbscan` and `internal/hungarian`.
  - **Files:** `internal/hdbscan/`, `internal/hungarian/`
  - **Verification:** `go build ./... && go vet ./...`

- [ ] **Task 9:** Stability regression suite — the four invariants of §2.1 as
      executable tests.
  - **Files:** `stability_test.go`
  - **Verification:** `go test -run TestStability ./...` (idempotent re-run
    changes nothing; signals in story X do not move story Y; a chained fixture
    produces no blob; two runs over identical state agree; **no split-merge
    cycle** — a corpus run for 50 consecutive batches emits no repeated
    split-then-merge on the same signal set)

- [ ] **Task 10:** Update `DESIGN.md`, `AGENTS.md`, `README.md`; mark spec 002
      and 003 `SUPERSEDED`; update `spec/README.md`.
  - **Files:** docs and spec index
  - **Verification:** manual review

- [ ] **Task 11:** Update `magic-giant` config plumbing for the new knobs.
  - **Files:** `../magic-giant/internal/config/config.go`,
    `../magic-giant/cmd/magic-giant/main.go`, both YAML files, tests
  - **Verification:** `cd ../magic-giant && go test ./...`

### 3.2 Risks & Mitigation

| Risk | Detection | Mitigation |
|---|---|---|
| **Threshold miscalibration.** 0.28/0.22 come from one 596-signal corpus; a different source mix shifts the valley. | Story count per batch, outlier fraction, radius distribution. | Both are config. The measurement script that produced them is reproducible against any store. Worth re-running before deploying to a new corpus. |
| **Permanent misassignment** (§2.6). Individual signals are never relocated. | Story radius growth. | Strict centroid threshold at assignment; merge repairs the converged case; split (§2.4) repairs it once enough misplaced signals form a group clearing `MinStorySize`. A single stray signal is deliberately left alone — churning one signal per run is the instability being removed. |
| **Split/merge oscillation.** Merge tests a given partition, split searches for the best one, so a shared threshold lets them undo each other along different seams. | Repeated `EventStorySplit` / `EventStoryMerged` on the same signal set; the 50-batch stability test. | Hysteresis band, `MergeThreshold < SplitThreshold` enforced by `validate()`. The 0.08 default gap is ~1σ of the corpus pairwise distribution and is the first parameter to widen if churn appears. |
| **Split fragments a diffuse story.** A broad topic with no internal gap gets cut arbitrarily. | Split events on stories whose parts land inside the hysteresis band. | Acceptance needs part-centroid separation `> SplitThreshold`, not merely a partition existing. Diffuse is not bimodal. Both parts must also clear `MinStorySize`. |
| **Slow convergence on multi-modal stories.** One split per story per run means a three-group story takes two runs. | Story radius staying high across runs. | Accepted: bounded, observable change per run beats unbounded recursion, which is re-clustering by another name. |
| **Lost discovery.** A fixed threshold cannot find cluster shapes density clustering would. | Stories that should be one remaining several. | Accepted and explicit. Merge reunites over-fragmentation across runs; for developing news, identity continuity outranks shape discovery. |
| **Centroid drift** on long-running stories. A recency-tracking centre lets a story silently become a different story under the same ID. | Radius growth; `Centroid` vs `RecentCentroid` divergence, which is directly measurable. | Identity geometry uses the lifetime `Centroid`, which cannot slide (§2.7); only admission follows recency. Adaptive threshold clamped to `AssignThreshold`. Drift surfaces as radius growth and is routed to the split test rather than absorbed. |
| **Stale catchment.** The mirror risk: a story stops admitting its own developments because its centre reflects opening coverage. | Rising outlier rate alongside topically-related stories being created. | `RecentCentroid` over `ActiveContextWindow` governs admission for exactly this reason. |
| **Deprecated fields mislead callers** into thinking clustering is still tunable. | Code review. | `Deprecated:` godoc on every superseded field, plus a `DESIGN.md` note. |
| **Over-fragmentation at startup.** Cold start with few signals promotes little and creates many singleton outliers. | Outlier fraction after first runs. | Expected and benign: outliers are retained until `OutlierTTL` and promoted once coverage arrives. `MinStorySize` 3 matches the previous `MinClusterSize` default. |

---

## Phase 4: Execution & Verification
- [ ] All per-task verification steps pass.
- [ ] `go vet ./...` clean.
- [ ] Unit tests pass, including the four stability invariants and the
      50-batch no-oscillation run.
- [ ] `go build ./...` succeeds with `internal/hdbscan` and
      `internal/hungarian` deleted.
- [ ] `magic-giant` builds and its suite passes against the new library.
- [ ] Benchmarks show no batch-run regression at 10k signals.
- [ ] Reference corpus reproduces: no story exceeding a plausible size, no
      unexplained retirements across consecutive runs.
- [ ] Codefather approval.

---

## Phase 5: Completed
- [ ] All Phase 4 items `[x]`.
- [ ] No regressions.
- [ ] Spec document reflects the actual implementation.
- [ ] Specs 002 and 003 marked `SUPERSEDED` with a pointer here.
- [ ] `spec/README.md` updated to `COMPLETED`.
- [ ] Codefather approval.
