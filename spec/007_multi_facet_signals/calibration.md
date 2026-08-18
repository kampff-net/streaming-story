# Spec 007 — Calibration Measurements

Reference corpus: `testdata/corpus_embeddings.txt`, 596 signals, 3072-dim.
Harness: ingest all signals with recent timestamps, run three batches, read
`sigmaGlobal` and the settled structure. Config is stock defaults.

| | σ_global | stories | placed signals | placed facets | unplaced |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `F = 1` | 0.255905 | 32 | 234 | 234 | 362 (60.7%) |
| `F = 2` (duplicated vector) | 0.252008 | 36 | 246 | 492 | 350 (58.7%) |

## Conclusion: no threshold change is warranted

**σ_global moves by 1.5%.** `InitialSigmaGlobal` defaults to 0.25 and the
measured value at `F = 1` is 0.2559 — the existing default is already the right
number, and the facet change does not move it enough to matter.

`AssignThreshold`, `MergeThreshold`, `SplitThreshold`, `InitialSigmaGlobal`, and
`SigmaFloor` are therefore **left unchanged**. Re-deriving them from these
measurements would reproduce the values already in `config.go`.

This contradicts the expectation set in §2.5, which argued a recalibration was
mandatory because `corpusMeanOf` now averages facets and so moves the corpus
mean. That reasoning is sound but its premise does not hold at `F = 1`, where
facet space and signal space are the same space, and the `F = 1` clustering is
identical to spec 006 (`TestStability_SingleFacetMatchesSpec006`). §2.5 has been
corrected to say so.

## What these numbers do not establish

**The `F = 2` row is plumbing, not semantics.** No genuinely decomposed corpus
exists yet, so that row duplicates each signal's single vector into two
identical facets. It exercises the facet machinery end to end, but both facets
point in the same direction, so it cannot show what real decomposition does to
σ_global or to the orphan rate. The 60.7% → 58.7% improvement is an artifact of
a shifted corpus mean and more admission attempts, **not** evidence that facets
reduce orphaning.

Settling that needs a corpus decomposed by a real extractor — `magic-giant`'s
facet extraction, which is a separate spec in that repository. Until then the
honest statement is: this change makes multi-facet clustering *possible* and
provably does not regress single-facet clustering. Whether it reduces the orphan
rate on production data is unmeasured.

**The 60.7% unplaced rate is pre-existing and unchanged.** It reproduces exactly
on the pre-facet code. It reflects a corpus where most signals genuinely have no
near neighbours at `MinStorySize` 3, and eviction at `OutlierTTL` then removes
them. It is not caused by this change and is not addressed by it.

## Two pre-existing findings, both out of scope

Neither is caused by this change; both reproduce on the pre-facet tree.

**σ_global reads zero under aged fixtures.** It accumulates only over stories
that end a batch run `Active` (`maintain.go`, step 6). A fixture whose signal
timestamps are older than `SilenceWindow` sends every story Dormant before the
first batch, so nothing accumulates and `sigmaGlobal` stays 0 — leaving every
adaptive radius on the `InitialSigmaGlobal` fallback, silently. Production
traffic is recent, so this does not bite there, but it makes any calibration
harness using historical timestamps measure nothing. The measurements above use
recent timestamps for exactly this reason.

**`BenchmarkIngestDuringApply` could deadlock.** It set `applyInProgress` and
pushed into `ingestBuffer` with no drainer, so past `IngestBufferCap` sends it
blocked forever. It had never been run at `-count 10`, so it had never reached
the cap. Fixed as part of Task 0 by draining the channel for the benchmark's
duration, which is what the batch goroutine does in production.
