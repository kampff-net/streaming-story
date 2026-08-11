# SDD Spec: [Feature Name]

## Metadata
* **Status:** `PROPOSAL`
* **Author:** [Author Name]
* **Created:** [YYYY-MM-DD]
* **Last Updated:** [YYYY-MM-DD]
* **Approver:** Codefather

---

## Phase 1: Proposal (Rough Idea)

### 1.1 Problem Statement
*Describe the pain point, gap, or feature requirement.*

### 1.2 Proposed Solution
*Briefly outline the conceptual approach — no implementation detail.*

### 1.3 Scope & Requirements
* **In Scope:**
  * Requirement 1
  * Requirement 2
* **Out of Scope:**
  * Explicitly excluded adjacent work.

---

## Phase 2: System Design (SDD)

### 2.1 Architecture & Components
*How changes fit into the existing project. Include Mermaid diagrams where relevant.*

```mermaid
graph TD
    %% Diagram goes here
```

### 2.2 Data Structures & Interfaces
*New types, interfaces, schemas, config keys. Value ranges and units.*

### 2.3 Protocol / API Changes
*Wire formats, schemas, CLI flags, environment variables, Makefile targets.*

### 2.4 Real-Time & Resource Impacts
*Allocation budget, latency budget, performance verification strategy.*

---

## Phase 3: Implementation Plan (IP)

### 3.1 Task Breakdown
- [ ] **Task 1:** [Title]
  - **Files:** `path/to/file`
  - **Verification:** `<command>`
- [ ] **Task 2:** [Title]
  - **Files:** `path/to/file`
  - **Verification:** `<command>`

### 3.2 Risks & Mitigation
*What could go wrong. How to detect and recover.*

---

## Phase 4: Execution & Verification
- [ ] All per-task verification steps pass.
- [ ] Linter / vet clean.
- [ ] Unit tests pass.
- [ ] Build targets compile.
- [ ] Neighbor packages unaffected.
- [ ] Approved by Codefather.

---

## Phase 5: Completed
- [ ] All Phase 4 items `[x]`.
- [ ] No regressions.
- [ ] Spec document reflects actual implementation.
- [ ] `spec/README.md` updated to `COMPLETED`.
- [ ] Approved by Codefather.
