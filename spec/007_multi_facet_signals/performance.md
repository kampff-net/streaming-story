# Spec 007 — Performance

`baseline.txt` is the pre-change capture (Task 0), `after.txt` the post-change
one, both `-count 10` on the same machine. **Every benchmark runs at `F = 1`**,
so these numbers are the cost of the new *schema*, not the cost of facets.

Compare with:

```sh
go run golang.org/x/perf/cmd/benchstat@latest \
  spec/007_multi_facet_signals/baseline.txt spec/007_multi_facet_signals/after.txt
```

## Result

| benchmark | sec/op vs base | allocs/op vs base |
| :--- | ---: | ---: |
| `IngestSteadyState` | +57.7% | +14.2% |
| `SignalsOf` | +28.7% | +63.2% |
| `Batch` | +28.3% | +47.5% |
| `IngestDuringApply` | +22.2% | 2 → 6 |

The §2.4 budget — ingest within 1.5× the `F = 1` baseline at `F = 3` — is
**missed at `F = 1` already**, before facets enter. That is a real result and
is not explained away below; it is explained.

## The cause is `MemStore`, not the design

`MemStore.ScanRange` and `ScanPrefix` call `sortedKeys()`, which collects **every
key in the store** into a slice and sorts it — on every scan (`store.go`). The
Draft path performs one `ScanRange` per ingest, so ingest cost is `O(N log N)`
in the *total number of keys in the store*, whatever those keys are.

The new schema holds one extra key per signal. Measured on the
`BenchmarkIngestSteadyState` fixture (200 signals, one batch):

| schema | keys | breakdown |
| :--- | ---: | :--- |
| spec 006 | 409 | 204 `s:` + 200 `l:` + 4 `t:` + 1 `c:` |
| spec 007 | 609 | 200 `g:` + 204 `s:` + 200 `l:` + 4 `t:` + 1 `c:` |

That is **+48.9% keys**. Sorting cost predicts a slowdown between 1.49×
(linear) and 1.59× (`N log N`); the measured ingest slowdown is 1.58×. The
agreement is close enough to identify the mechanism rather than merely be
consistent with it.

**Production does not pay this.** `MemStore` is the test store — bbolt, which
`magic-giant` uses, iterates with a B+tree cursor and seeks to the range start,
costing `O(log N + k)` in the keys actually scanned. It does not care how many
unrelated keys the store holds, so the extra `g:` record does not enter the
Draft path's scan cost at all.

The honest statement: **this change has not been benchmarked on a production
store.** The numbers above measure a test harness whose scan is quadratic-ish by
construction, and they should not be quoted as the cost of facets. A bbolt-backed
benchmark is the missing measurement, and it is the one that would settle the
§2.4 budget.

## What was optimised

`Ingest` originally re-read and re-decoded each touched story's metadata with
`readStoryMeta`, although `findNearestStories` had already decoded it during the
scan. The matched `StoryMeta` is now carried forward. Allocations per ingest
fell 167 → 153; wall time moved ~1.4%, which is what identified the scan rather
than the decode as the dominant term.

## Allocation

Allocations rise across the board (`IngestSteadyState` +14% after the fix above,
`SignalsOf` +63%). Two genuine sources, both structural rather than incidental:

- The location index is now a JSON array with one entry per facet, where it was
  a short fixed string. Marshalling and unmarshalling it allocates.
- `SignalsOf` does a random `Get` per distinct member instead of reading the
  payload inline from the scan, which is the trade §2.4 predicted and accepted:
  the payload is stored once rather than once per membership, and that is what
  makes `Signals()` and the §2.5 dump path possible.
