# Superseded Design Decisions

This file is the graveyard. Every approach here was implemented, measured, and
removed. It exists so the reasoning is not lost and so a future change does not
reintroduce a failure mode that was already paid for.

`DESIGN.md` and `README.md` describe only what the library does now. Nothing in
this file is live behaviour.

Measurements cite the reference corpus: `testdata/corpus_embeddings.txt`, 596
real news embeddings of dimension 3072, holding roughly 20–30 distinct topics.

---

## 1. HDBSCAN batch re-clustering

**What it was.** `internal/hdbscan` re-derived the whole story set on every
batch run: build a minimum spanning tree over mutual-reachability distances,
condense it, and select clusters by excess-of-mass or by leaf. `MinClusterSize`,
`MinSamples`, `MaxClusterSize`, and `ClusterSelection` (with its `EOM` and `Leaf`
modes) configured it.

**Why it was removed.** An MST over mutual-reachability distances is single
linkage with a density correction, and news embeddings chain through it. On the
reference corpus, transitive grouping at cosine distance 0.25 put **324 of 596
signals into one component**; nearest-centroid grouping at the same threshold
produced a largest group of **22**.

`ClusterSelectionEOM` made this worse rather than better: a large, diffuse,
long-lived region accumulates stability across the whole lambda range it
survives, which can exceed the summed stability of the tighter clusters nested
inside it, so the broad region wins and every story inside it is reported as one.
Raising `MinClusterSize` does not counteract it — density pruning happens
earlier, and a broad region that is genuinely dense only becomes more stable.

**What replaced it.** Incremental maintenance of existing stories: evict,
promote, admit, split, merge, recentre, lifecycle. See DESIGN.md.

**Do not reintroduce.** Any grouping rule based on transitive linkage — single
linkage, connected components over a distance graph, DBSCAN-family reachability —
chains on this data.

---

## 2. Jaccard / Hungarian cluster mapping

**What it was.** Because re-clustering discarded story identity every run, a
two-phase engine reconstructed it. Phase 1 built a cost matrix of
`1 − Jaccard(batch cluster, persistent story)` restricted to pairs above
`MappingMinJaccard` (0.6) and solved it with the Hungarian algorithm in
`internal/hungarian` for optimal 1-to-1 continuity. Phase 2 scanned the full
Jaccard matrix for the secondary overlaps the 1-to-1 constraint suppressed,
detecting N-way splits and merges above `SplitMinJaccard` (0.3) with a combined
coverage test of 0.7.

**Why it was removed.** It solved a problem that only existed because clustering
threw identity away each run. Once stories are maintained rather than
re-derived, identity is never lost, so there is nothing to reconstruct. The
engine was pure cost: O(n³) assignment plus a full overlap matrix, to recover
labels the new design never drops.

**What replaced it.** Story IDs persist by construction. A split keeps the ID on
the larger part; a merge keeps the oldest `CreatedAt`.

---

## 3. Stratified sampling and `BatchSampleCap`

**What it was.** Batch runs sampled down to `BatchSampleCap` (50,000) signals in
two passes, reserving up to `SampleGuaranteeMaxFraction` (0.5) of the cap for
per-story minimums and distributing the remainder proportionally. Clustering
cost grew with signal count, so the input had to be bounded.

**Why it was removed.** Maintenance is O(stories²) over centroids plus one pass
over each story's members, not O(signals²) over a distance matrix. There is
nothing left whose cost a sample would bound. Sampling also made the run
non-reproducible for a fixed store, which the determinism rule forbids.

**Note.** Both fields were retained briefly as documented no-ops, then removed;
see [Removed `Config` fields](#removed-config-fields).

---

## 4. `StabilityWindow` and per-signal re-assignment

**What it was.** Signals inside a `StabilityWindow` could be re-assigned
individually to whichever story the current batch found a better fit for, while
older signals were frozen.

**Why it was removed.** Two scopes for the same question — which signals may
move — disagreed, and a signal near a boundary between two stories flipped
between them on every run. That churn was the instability the current design
exists to remove. The field is gone from `Config`; `BatchWindow` was already the
authoritative scope.

**What replaced it.** Membership changes only through a split or a merge, each
moving a whole group. An individual misassignment is repaired only when enough
similar signals accumulate to clear `MinStorySize` — which is exactly when the
error is worth acting on. Outlier admission is the one exception, and it moves
signals *into* stories, never between them.

---

## 5. Store-path-derived UUID namespace

**What it was.** Signal ID namespaces were derived from the tracker's directory
(`trackerDir`).

**Why it was removed.** A path that changes — relative against absolute, a
symlink, a container mount point — silently produced different IDs for the same
domain key, so the same signal ingested from two deployments became two signals.

**What replaced it.** `TrackerNamespace`, a fixed compile-time constant, with
`Config.Namespace` for deliberate per-tenant isolation.

---

## 6. Noise label and noise retention

**What it was.** HDBSCAN labels points as noise, so the design carried a noise
state alongside stories, with rules for how long a noise-labelled signal was
retained before eviction.

**Why it was removed.** There is no clustering step to emit a noise label. A
signal is either a member of a story or sits in the outlier bucket, and the
bucket has one rule: `OutlierTTL` against `lastBatch`.

---

## 7. Mutual-neighbour cliques for outlier promotion

**What it was.** Promotion built a graph over outliers with an edge wherever
cosine distance was within `AssignThreshold`, then took greedy maximal cliques of
at least `MinStorySize`. Requiring every pair to be mutually adjacent was the
defence against the chaining that HDBSCAN had shown.

**Why it was removed.** The defence was sound; the criterion was too strict for
real data. A news cluster of radius 0.16 has extremes twice that apart, so
all-pairs adjacency shattered single topics and left most of their signals
ungrouped. At one threshold on the reference corpus, cliques grouped **98 of 596
signals (16%)** where nearest-centroid growth grouped **182 (31%)**; the shipped
pipeline reaches **234 (39%)**.

**What replaced it.** Nearest-centroid growth with a closing compaction, which
bounds a group inside one ball of radius `AssignThreshold` regardless of the path
the centroid took while growing. Non-chaining survives as a guarantee rather than
as all-pairs adjacency. Cliques are still used for merge candidate grouping,
where the population is stories rather than signals.

---

## 8. Raw cosine geometry

**What it was.** Every distance was raw cosine on the caller's embeddings, with
thresholds `AssignThreshold` 0.28, `MergeThreshold` 0.22, `SplitThreshold` 0.30.

**Why it was removed.** Text embeddings are anisotropic: every vector carries a
large component along one shared direction, so the corpus occupies a narrow cone
rather than the sphere. Two consequences, both fatal to centroid-based
clustering:

1. The mean of a large group converges on that shared direction, so two groups
   sharing nothing still have near-identical centroids. Measured: two halves of
   the corpus whose closest members sat **0.84** apart had centroids **0.06**
   apart. Every centroid-distance test — split above all — read "identical" and
   never fired.
2. Because every centroid resembles every signal, a group that grows by admitting
   whatever is nearest its centroid snowballs. At a radius of 0.50 of the median
   pairwise distance, raw geometry produced a **229**-signal group where centred
   geometry produced **25**.

End to end, with everything else fixed, `MeanRemoval = 0` yields **2 stories with
a 581-signal blob**; 0.9 yields **32 stories with a largest of 28**.

**What replaced it.** Centred space: subtract `MeanRemoval` × corpus mean before
measuring anything, and rebase every threshold onto that scale (0.50 / 0.40 /
0.55). The scale is roughly twice raw cosine — median pairwise distance 1.02
centred against 0.45 raw.

**Rejected alternatives, measured.**

| Approach | Verdict |
|---|---|
| Remove top-k principal components ("all-but-the-top") beyond the mean | Separation contrast peaks at mean-centring alone (0.275) and degrades monotonically: 0.231 at k=1, 0.199 at k=3, 0.119 at k=20. Those components carry topic signal, not anisotropy. |
| UMAP / t-SNE | Stochastic whole-dataset fit with no exact out-of-sample map. Every refit reshuffles the geometry, so story identity churns and the stability invariants break. Distances also lose absolute meaning, so thresholds stop being interpretable across fits. |
| Random projection (Johnson–Lindenstrauss) | Deterministic and cheap, but it preserves cosine — it buys speed, not separation. Still available as a future performance option. |
| ZCA / full whitening on a running covariance | Same drift exposure as a fitted projection, more parameters, and the k-sweep above shows over-whitening degrades separation. |
| `MeanRemoval = 1.0` (full centring) | Degenerate when the corpus is itself one tight group: the mean lands on top of every signal, the residuals are whatever noise remains, and a coherent story shatters into antipodal halves. This broke four behavioural tests that 0.9 passes. |
| EMA-updated mean | Makes the geometry depend on how many batches have run, so a re-run over unchanged data shifts every distance slightly and flips membership on borderline stories. The mean is recomputed from full membership instead. |

---

## 9. Radius gate as the merge admission test

**What it was.** A merge candidate group was accepted only when
`maxAngularSeparation(radiusOf(union)) ≤ SplitThreshold` — the same radius gate
the split step uses as a pre-filter.

**Why it was removed.** The gate is a *necessary* condition for a split, not a
sufficient one, so using it to approve a merge rejects unions no split would ever
touch. At `SplitThreshold` 0.55 it demands a union radius under **0.15**, tighter
than a real story ever is, and **no merge ever fired**. Fragmentation therefore
persisted: 52 stories for roughly 25 topics, with no mechanism to reunite the
pieces.

**What replaced it.** The union is put through `splitStory` itself. A merge is
allowed exactly when the next run would leave the result whole, which makes merge
and split true inverses and keeps the fix correct if either rule changes.

---

## 10. Outliers as a terminal bucket

**What it was.** The outlier bucket had exactly two exits: promotion into a new
story with other outliers, or eviction at `OutlierTTL`.

**Why it was removed.** The Draft phase runs once, at ingest. A signal arriving
before the batch that creates its story is bucketed, and nothing ever re-tests
it — so it expired even when an established story covered it perfectly. On the
reference corpus **342 of 596 signals** were stranded this way.

**What replaced it.** An admission step in the maintenance pass, re-applying the
Draft test against stories that now exist.

---

## 11. Random story IDs

**What it was.** A story born in the maintenance pass took its ID from
`uuid.New()` — random, at both mint sites: promotion out of the outlier bucket
and the child of a split.

**Why it was removed.** Every other output of a run is a function of the input
stream: grouping is sorted by signal ID, centroids are recomputed from members,
and the pass is idempotent over unchanged data. The IDs were the one exception,
so replaying a recorded stream against a fresh store produced the same stories
under different names and could not be diffed against the original run. The
randomness also leaked into behaviour — story IDs order the split and merge
decisions, and the residual churn measured by the streaming suite (3–8 of 791
carried assignments at a 300-signal seed) moved run to run for that reason
alone.

**What replaced it.** `deriveStoryID`: UUID v5 under `Config.Namespace` over the
sorted founding signal IDs, seeded with the birth route (`promote`, or
`split:{parentID}`). An ID already held — by a live story of the same run or by
any record in the store, archived included — is rederived with the next salt,
because a split can spawn exactly the member set an existing story was founded
on and reusing that ID would silently fold the two together. Salting is inside
the derivation, so a replay meets the same occupied IDs in the same order.

---

## Removed `Config` fields

These were kept as documented no-ops for one cycle so callers would still
compile, then removed once nothing in the module referenced them. A caller
setting any of them now fails to build, which is the intended signal: the
parameter had no effect long before it disappeared.

| Field | Belonged to |
|---|---|
| `BatchSampleCap` | §3 stratified sampling |
| `SampleGuaranteeMaxFraction` | §3 stratified sampling |
| `MinClusterSize` | §1 HDBSCAN — replaced by `MinStorySize` |
| `MinSamples` | §1 HDBSCAN |
| `MaxClusterSize` | §1 HDBSCAN |
| `ClusterSelection`, with the `ClusterSelectionEOM` / `ClusterSelectionLeaf` constants and the `ClusterSelection` type | §1 HDBSCAN |
| `MappingMinJaccard` | §2 Jaccard/Hungarian mapping |
| `SplitMinJaccard` | §2 Jaccard/Hungarian mapping |
| `StabilityWindow` | §4 — removed outright, never deprecated |
| `Dir` | §5 store-path-derived namespace |

**`BatchWindow` survives but does almost nothing.** It sets `OutlierTTL`'s
default to `2×BatchWindow` and has no other effect: membership is read in full on
every pass, so it no longer bounds the clustering input the way it did under
windowed re-clustering. Set `OutlierTTL` directly and it stops mattering.

---

## 7. JSON serialization and `JSONCodec[T]`

**What it was.** All internal records (`storyRecord`, `calibState`, `Signal[T]`, location indexes) were serialized with `encoding/json`. `JSONCodec[T]` was the default codec.

**Why it was removed.** JSON encoding of floating-point vectors is slow and bloated (~10-15 bytes per float ASCII vs 4 bytes binary). Deserialization during ingest and batch collection caused high CPU overhead and GC churn.

**What replaced it.** Canonical CBOR (`github.com/fxamacker/cbor/v2`) with integer keys (`keyasint`). `CBORCodec[T]` is the default codec. `JSONCodec[T]` was removed. Existing stores must be rebuilt via replay.

---

## 8. Preserving producer embedding magnitudes

**What it was.** Ingest preserved producer vector magnitudes in `Signal.Embeddings`, and distance computations normalized on every pairwise comparison.

**Why it was removed.** Magnitude is unused by cosine similarity, centroids, and clustering. Recomputing norms on every distance comparison was the single most expensive linear algebra operation.

**What replaced it.** Normalization to unit vectors on ingest. The store holds unit vectors; `Signals()` returns normalized unit vectors; zero-magnitude embeddings are rejected at `Ingest` with `ErrZeroEmbedding`.

