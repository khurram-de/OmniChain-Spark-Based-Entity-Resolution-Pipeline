# OmniChain — Explainable Entity Resolution (Scala + Spark, Datasets-first)

OmniChain is a **high-performance, policy-driven Entity Resolution (ER) pipeline** built using **Scala and Apache Spark (Datasets-first)** to support **AML / Financial Crime** use cases such as uncovering structuring and layering behavior by linking fragmented identifiers across transaction data.

The project runs locally on realistic scale using the **PaySim dataset (~6.3M transactions)** and demonstrates a production-style ER architecture:

- Multi-pass blocking (scale control)
- Candidate pair generation
- Similarity scoring as facts
- Policy-driven decisioning
- Entity graph resolution via connected components

---

## Why OmniChain (AML Context)

In financial crime investigations, a single real-world actor often appears under multiple identifiers, reused accounts, or automated transaction patterns.  
Entity Resolution unifies these fragments into **resolved entities**, enabling investigators to understand true aggregate behavior such as total transaction volume and frequency.

OmniChain emphasizes:

- **Explainability** – every match is traceable to evidence
- **Policy control** – behavior changes via configuration, not code
- **Scale realism** – PaySim-scale data, not toy examples

---

## Project Status

**Current State:** COMPLETED

### Completed
- PaySim ingestion into `Dataset[Transaction]`
- Multi-pass blocking (exact identifier + behavioral)
- Candidate pair generation with cross-pass de-duplication
- Similarity scoring (Levenshtein + behavioral signals)
- Policy-driven decisioning (IDENTIFIER / BEHAVIORAL modes)
- Entity resolution via iterative connected components
- Entity-level intelligence reporting

---

## Tech Stack

- Scala **2.13.16**
- Apache Spark **4.0.0**
- Typesafe Config **1.4.3**
- sbt (Spark dependencies marked as `provided`)

---

## Repository Structure (Key Files)

```
src/main/scala/omnichain/
  app/Main.scala
  ingress/
    DataLoader.scala
    PaySimRaw.scala
  model/
    Transaction.scala
    CandidatePairs.scala
    SimilarityScore.scala
    DecisionPolicy.scala
    EvidenceMode.scala
    MatchDecision.scala
    PairDecision.scala
  transformations/
    blocking.scala
    generateCandidatePairs.scala
    SimilarityScoring.scala
    Decisioning.scala
    EntityResolution.scala
  metrics/
    PipelineMetrics.scala

src/main/resources/
  application.conf

run_local.sh
build.sbt
```
---

## Domain Model (Core Types)

### Transaction
Normalized domain representation of a transaction:

- txId: String
- name: String          (PaySim: nameOrig)
- wallet: String        (proxy: nameOrig)
- amount: Double
- txType: String        (PaySim: type)
- eventTime: Long       (PaySim: step)

### CandidatePairs
Represents a pair of transactions generated within the same blocking key.

### SimilarityScore
Pure facts only (no decisions):
- name similarity (Levenshtein)
- wallet exact match
- relative amount difference
- transaction type match
- temporal distance

### MatchDecision / PairDecision
Explainable match outcome derived from policy (decision + reasons + policy version).

---

## Pipeline Overview

PaySim CSV
↓
DataLoader (Dataset[Transaction])
↓
Multi-Pass Blocking (Scale Control)
↓
Candidate Pair Generation
↓
Similarity Scoring (Facts)
↓
Policy-Driven Decisioning
↓
Matched Pairs
↓
Entity Resolution (Connected Components)

---

## Ingress (PaySim → Transaction)

- Explicit schema via `PaySimRaw.getSchema()`
- Synthetic `txId` generation using:
  - `monotonically_increasing_id()` (Main)
  - or deterministic hash via `sha2(...)` (DataLoader)
- Mapping:
  - nameOrig → name
  - nameOrig → wallet (documented proxy)
  - amount → amount
  - type → txType
  - step → eventTime

---

## Blocking (Scale Control)

Two independent blocking passes:

### Pass A — Exact Name Blocking
- blockKey = normalized name (lower + trim)
- High precision, small blocks

### Pass B — Behavioral Blocking
- blockKey = amount (cents) + transaction type
- Surfaces automated structuring behavior
- Composite key reduces candidate explosion

Optional block capping is available to drop heavy-hitter blocks above a configured threshold.

---

## Candidate Pair Generation

- Implemented via Spark self-join on blockKey
- Pair ordering enforced (`txId < txId2`)
- No cartesian products
- No driver-side collections
- Cross-pass union with order-invariant de-duplication

---

## Similarity Scoring (Facts Only)

Implemented in `SimilarityScoring.toSimilarityScore`:

- Levenshtein-based normalized name similarity
- Wallet exact match (case-insensitive)
- Relative amount difference
- Transaction type exact match
- Temporal distance (absolute step delta)

Scoring produces **facts only** — no thresholds, no decisions.

---

## Policy-Driven Decisioning

Decisioning is fully policy-controlled and explainable.

- Evidence extracted as `DecisionReason`
- Decisions derived via `DecisionPolicy`
- Supported Evidence Modes:
  - IDENTIFIER (wallet or name)
  - BEHAVIORAL (amount + time + optional type)

Decisions include:
- MATCHED / NOT_MATCHED
- Sorted evidence reasons
- Policy version for auditability

---

## Entity Resolution

Implemented using iterative label propagation (connected components):

- Builds undirected edges from matched pairs
- Propagates minimum label per component
- Uses `localCheckpoint(eager = true)` to control lineage growth
- Produces mapping: txId → resolvedEntityId

---

## Configuration (application.conf)
```
omnichain.policy {
version = "1.0.0"
nameSimilarityThreshold = 0.85
amountRelativeTolerance = 0.10
walletMatchEnabled = true
requireTypeMatch = true
maxStepDistance = 5
evidenceMode = "BEHAVIORAL" # IDENTIFIER | BEHAVIORAL
}
```
---

## How to Run (Local)

### Prerequisites
- Java 17+
- sbt
- Apache Spark installed locally

### Recommended
Use the provided script:

bash run_local.sh

This will:
1. Build the project with sbt
2. Run via `spark-submit --master local[*]`

### Dataset Path
PaySim CSV path is currently hardcoded in `Main.scala`.  
Update the path or refactor to accept arguments as needed.

---

## Runtime Output

The pipeline logs:

- Total transactions loaded
- Top block sizes per blocking pass
- Candidate counts (per pass and deduped)
- Loaded policy configuration
- Matched pair counts
- Connected components convergence metrics
- Sample resolved entities

---

## Performance Notes (Local Mode)

- local[*] means driver == executor
- Memory pressure is expected at PaySim scale
- Practical tuning levers:
  - composite behavioral blocking keys
  - block size caps
  - selective persistence
  - driver memory via spark-submit

OOMs are treated as **design signals**, not configuration failures.

---

## Design Principles Demonstrated

- Blocking controls scale, not correctness
- Similarity signals are facts, not decisions
- Decisions are policy-driven and explainable
- Pure Scala business logic; Spark handles distribution
- Avoid UDFs and driver-side collections
- Dataset-first design for refactor confidence

---
## Dataset

This project uses the PaySim synthetic financial transaction dataset
(~6.3 million transactions), which simulates mobile money behavior for
fraud and AML research.

The dataset is publicly available on Kaggle:
https://www.kaggle.com/datasets/ealaxi/paysim1

Due to its size, the raw PaySim CSV is not stored in this repository.
To run the pipeline locally, download the dataset from Kaggle and place
the CSV at:

src/main/scala/omnichain/data/PaySim.csv

---
## Results

The full pipeline was executed locally on the PaySim synthetic transaction dataset
(~6.3M transactions). Due to dataset size, the raw PaySim CSV is not stored in the
repository; however, the execution outputs and metrics are versioned for
reproducibility and review.

```text
+----------------+--------------+------------+-----------------+
|resolvedEntityId|totalAmount   |numberOfTxns|distinctIdsMerged|
+----------------+--------------+------------+-----------------+
|60129841133     |140,000,000   |14          |14               |
|60130221781     |100,000,000   |10          |10               |
|60130288633     |100,000,000   |10          |10               |
|51540355144     |80,000,000    |8           |8                |
|60130207639     |60,000,000    |6           |6                |
|60130270887     |60,000,000    |6           |6                |
|60129935774     |60,000,000    |6           |6                |
|25770584120     |50,000,000    |5           |5                |
|60129981844     |50,000,000    |5           |5                |
+----------------+--------------+------------+-----------------+
```

A sample of the final entity-level output is shown above.  
Full execution logs and metrics are available under  
`results/omnichainCompleteResult.txt`.

---

## Final Note

OmniChain is intentionally **not over-optimized**.

Design trade-offs are documented to prioritize correctness, explainability, and extensibility over premature optimization.

---

## Author

Maintained by: **Khurram K.**  
🔗 [LinkedIn](https://www.linkedin.com/in/mkhurramhk/)

---