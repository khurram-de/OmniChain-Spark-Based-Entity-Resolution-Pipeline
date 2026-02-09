# OmniChain — Lessons Learned & Performance Diagnostics

Dataset: **PaySim (~6.36M transactions)**  
Environment: **Scala + Spark (local[*])**, Datasets-first, policy-driven decisioning  

This document captures the key performance and scale issues encountered while running OmniChain on PaySim-scale data, how they were diagnosed (primarily via Spark physical plans), and the design corrections applied.

---

## 1) Core mental model

### Entity Resolution scale reality

- **Blocking diagnostics scale linearly** (e.g., `groupBy(blockKey).count()` is usually fine).
- **Candidate generation scales quadratically within a block**: for block size `n`, candidate pairs are `n * (n - 1) / 2`.

Example: a block of **3,065 rows** implies ~**4.7M** candidate pairs.

**Takeaway:** the “danger zone” is almost always **candidate generation**, not ingestion or simple aggregations.

---

## 2) Blocking & capping lessons

### 2.1 `repartition(N)` is not a free memory fix

**What happened:** early `.repartition(400)` introduced a `RoundRobinPartitioning` shuffle before meaningful work, increasing memory/disk pressure in `local[*]`.

**Lesson:**

- `repartition(N)` **forces a shuffle**.
- It does **not** solve skew.
- It can increase GC and spill pressure.

**Preferred lever:** set shuffle partitions where it matters (joins / aggregations):

```scala
spark.conf.set("spark.sql.shuffle.partitions", "64")
```

### 2.2 Join-based block capping is a Spark anti-pattern

**Anti-pattern:** `count → filter keys → join back`.

**Symptom in plan:** `SortMergeJoin` on `blockKey`, two shuffles, sorts on both sides, OOM in local mode.

**Lesson:** if you can decide something **within a block**, do it **inside the group**, not via a dataset join.

### 2.3 Correct pattern: cap inside groups

**Correct mental model:**

1. Shuffle once by `blockKey`
2. Inspect rows inside the group
3. Decide: drop / truncate / sub-block

Implementation options:

- Dataset approach: `groupByKey(...).flatMapGroups(...)`
- DataFrame approach: aggregate to arrays + slice (careful with memory)

---

## 3) Spark physical plan debugging checklist

### 3.1 Red flags to scan for

| Red flag | Typical meaning | Why it matters |
|---|---|---|
| `RoundRobinPartitioning` | forced shuffle (often from `repartition`) | adds IO + memory pressure |
| `SortMergeJoin` | large join, sort on both sides | expensive in local mode |
| `InMemoryRelation (deserialized)` | caching JVM objects | high heap usage / GC |
| multiple `FileScan csv` | recomputation / double scan | repeated IO + lineage |
| `DeserializeToObject` / `SerializeFromObject` | Dataset object churn | CPU + memory overhead |
| `AdaptiveSparkPlan isFinalPlan=false` | not executed plan yet | don’t trust pre-action `explain()` |

### 3.2 AQE workflow

If you see `isFinalPlan=false`, treat the plan as provisional.

**Correct workflow:**

1. Trigger an action (`count`, `show`, write)
2. Inspect the **SQL** tab in Spark UI
3. Use the **Executed Plan** as source of truth

---

## 4) Dataset vs DataFrame: when typing costs you

Typed Datasets are great for explainability and refactor safety, but they can introduce:

- `MapElements`
- `DeserializeToObject`
- `SerializeFromObject`

These show up when chaining `.map` across typed stages.

**Practical guideline used in OmniChain:**

- Stay **columnar** (DataFrame operations) for heavy-lift transforms
- Convert to `Dataset[T]` **once** at a boundary where it adds value

Example pattern:

```scala
// Prefer columnar transforms
val df2 = df1.select(...).filter(...)

// Convert once at the edge
val ds: Dataset[Transaction] = df2.as[Transaction]
```

---

## 5) Common performance issues and the fixes applied

### 5.1 Excessive object (de)serialization

**Symptom:** `SerializeFromObject` / `DeserializeToObject` around simple keying steps.

**Bad pattern:**

```scala
ds.map(txn => (txn.name.trim.toLowerCase, txn))
```

**Better pattern:** keep columnar as long as possible:

```scala
ds.toDF
  .select(
    lower(trim($"name")).as("blockKey"),
    struct($"*").as("tx")
  )
```

### 5.2 Multiple `map()` steps create `MapElements` chains

**Symptom:** repeated `MapElements` stages.

**Fix:** do multiple field projections in a single `select`, then `as[T]` once.

### 5.3 Caching strategy: serialized vs deserialized

**Symptom:** `StorageLevel(..., deserialized, ...)`.

**Fix:** prefer serialized caching for large data:

```scala
val SL = org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
```

### 5.4 Caching too much / caching too early

**Symptom:** multiple `InMemoryRelation` entries.

**Fix:** cache only datasets that feed **multiple actions**; unpersist aggressively once safe.

### 5.5 Shuffle memory exhaustion

**Symptom:** full data OOM but `.limit()` works.

**Explanation:** `.limit()` often avoids wide shuffles; the full plan introduces `Exchange hashpartitioning(...)`.

**Fixes used:**

- increase `spark.sql.shuffle.partitions`
- prefer serialized caching
- avoid join-based capping

### 5.6 Self-join causes double scan if input not materialized

**Symptom:** `FileScan csv` appears twice.

**Fix:** persist + materialize the blocked dataset before generating pairs.

---

## 6) Stabilized local-mode settings

What consistently worked best in local mode:

- Increase driver memory (driver == executor in `local[*]`)
- Use serialized persistence (`MEMORY_AND_DISK_SER`)
- Avoid early repartition shuffles

Example baseline:

```scala
SparkSession.builder()
  .master("local[*]")
  .config("spark.driver.memory", "8g") // Shown here as a reference baseline; final runs relied on default system settings.
  .config("spark.sql.shuffle.partitions", "64") // Shown here as a reference baseline; final runs relied on default system settings.
  .getOrCreate()
```

---

## 7) Architectural takeaway

In ER pipelines:

- **Blocking controls scale, not correctness**.
- Capping and heavy-hitter handling must be **policy-driven** and **explainable**.
- When scale explodes, it’s usually due to **quadratic pair growth** inside a handful of heavy blocks.
- Spark plan literacy is a core skill: diagnose first, then tune intentionally.
