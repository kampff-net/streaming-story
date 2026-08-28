# SDD Spec: 010 Cron-Like Batch Scheduling

## Metadata
* **Status:** `COMPLETED`
* **Author:** Antigravity (on behalf of Codefather)
* **Created:** 2026-08-28
* **Last Updated:** 2026-08-28
* **Approver:** Codefather

---

## Phase 1: Proposal

### 1.1 Problem Statement
`streaming-story` currently uses `BatchInterval time.Duration` with `time.NewTicker(cfg.BatchInterval)`.
This has two limitations:
1. **Clock alignment**: Tickers fire relative to process start time rather than clean wall-clock boundaries (e.g., top of the hour `0 * * * *`, every half hour `*/30 * * * *`, or daily schedules).
2. **Synchronization across instances/restarts**: Service restarts shift batch execution unpredictably.

Maintaining both `BatchInterval` and `BatchSchedule` introduces configuration ambiguity. Because cron expressions support standard 5-part schedules, descriptors (`@hourly`), and duration intervals (`@every 30m`), `BatchSchedule` completely replaces `BatchInterval`.

### 1.2 Proposed Solution
1. Use `github.com/robfig/cron/v3` as the cron scheduling and execution engine.
2. Remove `BatchInterval` from `Config[T]` and replace with `BatchSchedule string`.
3. Default `BatchSchedule` to `"*/30 * * * *"`.
4. Configure `cron.Cron` with standard 5-field parser (+ descriptors) and `cron.SkipIfStillRunning` wrapper to guarantee sequential batch execution.
5. In `Tracker.Close()`, call `ctx := t.cron.Stop()` and await `<-ctx.Done()` to drain in-flight batches before closing the underlying `Store`.

### 1.3 Scope & Requirements
* **In Scope:**
  * Add `github.com/robfig/cron/v3` dependency.
  * Replace `BatchInterval` with `BatchSchedule string` in `Config[T]`.
  * Validate `BatchSchedule` in `Config.Validate()` and during `NewTracker()`.
  * Embed `cron.Cron` instance inside `Tracker[T]`.
  * Wrap batch jobs with `cron.SkipIfStillRunning`.
  * Gracefully drain running batch jobs on `Tracker.Close()` using `cron.Stop()`.
  * Update all existing `streaming-story` tests and examples.
* **Out of Scope:**
  * Multi-job scheduler interfaces exposed to consumers (cron engine is internal to `Tracker`).
  * Modifying clustering algorithms, store schema, or event subscription channels.

---

## Phase 2: System Design

### 2.1 Architecture & Components

```mermaid
graph TD
    A[Config[T].BatchSchedule] -->|Validate & Parse| B[cron.Cron Engine]
    B -->|Schedule Trigger| C{SkipIfStillRunning}
    C -->|Idle| D[t.runBatch Pass]
    C -->|Running| E[Skip / Log Drop]
    D --> F[Clustering / State Updates]
    G[Tracker.Close] -->|t.cron.Stop| H[Wait <-ctx.Done]
    H --> I[Close Store & Subs]
```

### 2.2 Data Structures & Interfaces

In `streaming-story/config.go`:
```go
type Config[T any] struct {
    // BatchSchedule defines the cron expression for batch clustering runs
    // (e.g., "*/30 * * * *", "0 */2 * * *", "@hourly", "@every 30m").
    // Default: "*/30 * * * *".
    BatchSchedule string

    // ... other existing fields
}
```

In `streaming-story/tracker.go`:
```go
type Tracker[T any] struct {
    cfg  Config[T]
    cron *cron.Cron
    // ...
}
```

### 2.3 Initialization & Lifecycle

In `NewTracker()`:
```go
parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
// Validate expression
if _, err := parser.Parse(cfg.BatchSchedule); err != nil {
    return nil, fmt.Errorf("invalid BatchSchedule %q: %w", cfg.BatchSchedule, err)
}

c := cron.New(
    cron.WithParser(parser),
    cron.WithChain(cron.SkipIfStillRunning(cron.DiscardLogger)),
)
_, err = c.AddFunc(cfg.BatchSchedule, t.runBatch)
if err != nil {
    return nil, fmt.Errorf("failed to schedule batch: %w", err)
}

c.Start()
t.cron = c
```

In `Tracker.Close()`:
```go
func (t *Tracker[T]) Close() error {
    var closeErr error
    t.closeOnce.Do(func() {
        // Stop cron runner and wait for any active batch to finish.
        ctx := t.cron.Stop()
        <-ctx.Done()

        t.closeMu.Lock()
        defer t.closeMu.Unlock()

        t.subMu.Lock()
        t.closed.Store(true)
        subs := t.subs
        t.subs = nil
        for _, ch := range subs {
            close(ch)
        }
        t.subMu.Unlock()

        closeErr = t.cfg.Store.Close()
    })
    return closeErr
}
```

### 2.4 Testing Considerations
For unit tests that require running without background interference (or manually triggering batches), tests can supply a schedule that won't fire during test runs (e.g. `BatchSchedule: "0 0 1 1 *"`) and invoke `t.runBatch()` or `t.RunBatch()` directly.

---

## Phase 3: Implementation Plan

### 3.1 Task Breakdown
- [x] **Task 1:** Add dependency `github.com/robfig/cron/v3` to `streaming-story/go.mod`.
  - **Files:** `streaming-story/go.mod`
  - **Verification:** `go mod tidy`
- [x] **Task 2:** Replace `BatchInterval` with `BatchSchedule` in `streaming-story/config.go` & `config_test.go`.
  - **Files:** `streaming-story/config.go`, `streaming-story/config_test.go`
  - **Verification:** `go test -run TestConfig ./...`
- [x] **Task 3:** Update `Tracker[T]` engine lifecycle (`cron.New`, `cron.Stop()`) in `streaming-story/tracker.go`.
  - **Files:** `streaming-story/tracker.go`, `streaming-story/tracker_test.go`
  - **Verification:** `go test -run TestTracker ./...`
- [x] **Task 4:** Update all test files and examples across `streaming-story` (`BatchInterval` -> `BatchSchedule`).
  - **Files:** `streaming-story/*_test.go`, `streaming-story/examples/*/*.go`
  - **Verification:** `go test ./...`
- [x] **Task 5:** Add unit tests for cron scheduling, invalid schedules, and graceful draining.
  - **Files:** `streaming-story/schedule_test.go`
  - **Verification:** `go test -race ./...`

### 3.2 Risks & Mitigation
* **Risk:** Concurrent batch runs when clustering takes longer than schedule window.
  * **Mitigation:** `cron.SkipIfStillRunning` middleware drops overlapping triggers.
* **Risk:** Active batch writing to store after `Store.Close()` during shutdown.
  * **Mitigation:** `<-ctx.Done()` on `t.cron.Stop()` guarantees complete execution of running batch before closing store.

---

## Phase 4: Execution & Verification
- [x] All per-task verification steps pass.
- [x] Linter / vet clean (`go vet ./...`).
- [x] Unit tests pass (`go test ./...` and `go test -race ./...`).
- [x] Build targets compile (`go build ./...`).
- [x] Approved by Codefather.

---

## Phase 5: Completed
- [x] All Phase 4 items checked.
- [x] `spec/README.md` updated.
- [x] Approved by Codefather.
