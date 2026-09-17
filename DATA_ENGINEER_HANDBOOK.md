# Data Engineer Handbook — Daily Reference

A single lookup document for daily data engineering work: concepts, code patterns, and a
troubleshooting runbook. Use Ctrl+F to jump to a topic instead of reading top to bottom.

---

## Table of Contents

1. [Big Picture Concepts](#1-big-picture-concepts)
2. [Data Modeling (Star Schema & Friends)](#2-data-modeling-star-schema--friends)
3. [SQL Deep Dive](#3-sql-deep-dive)
4. [Python / Pandas Gotchas](#4-python--pandas-gotchas)
5. [PySpark / Spark Deep Dive](#5-pyspark--spark-deep-dive)
6. [Delta Lake / Lakehouse](#6-delta-lake--lakehouse)
7. [Streaming (Kafka + Structured Streaming)](#7-streaming-kafka--structured-streaming)
8. [Orchestration (Airflow / ADF / Databricks Workflows)](#8-orchestration-airflow--adf--databricks-workflows)
9. [Cloud Storage (ADLS / S3 / GCS)](#9-cloud-storage-adls--s3--gcs)
10. [Data Quality & Governance](#10-data-quality--governance)
11. [CI/CD & Version Control for Data Pipelines](#11-cicd--version-control-for-data-pipelines)
12. [Performance & Cost Optimization Playbook](#12-performance--cost-optimization-playbook)
13. [Troubleshooting Runbook (Symptom → Cause → Fix)](#13-troubleshooting-runbook-symptom--cause--fix)
14. [Interview Rapid-Fire Cheat Sheet](#14-interview-rapid-fire-cheat-sheet)

---

## 1. Big Picture Concepts

**OLTP vs OLAP**
- OLTP (Postgres, MySQL): row-based, many small fast transactions, normalized (3NF), powers apps.
- OLAP (warehouse/lakehouse): read-heavy, analytical queries over large volumes, often denormalized (star schema), columnar storage.

**ETL vs ELT**
- ETL: transform before loading into warehouse (traditional, transform on a separate compute engine).
- ELT: load raw data first, transform inside the warehouse/lakehouse using its own compute (Snowflake, BigQuery, Databricks, dbt). Dominant pattern today because cloud compute is cheap and elastic.

**Data Warehouse vs Data Lake vs Lakehouse**
- Warehouse: structured, schema-on-write, fast SQL, expensive storage (Snowflake, Redshift, Synapse).
- Lake: any file format, schema-on-read, cheap storage (S3/ADLS/GCS + Parquet), historically weak on ACID/updates.
- Lakehouse: lake storage + a transaction layer (Delta Lake / Iceberg / Hudi) giving ACID, schema enforcement, time travel on top of cheap object storage. This is where the industry has converged.

**Batch vs Streaming**
- Batch: process data in chunks on a schedule (hourly/daily). Simpler, cheaper, higher latency.
- Micro-batch: streaming engine processing small batches every few seconds (Structured Streaming default).
- Streaming/real-time: continuous processing, sub-second to few-second latency (Kafka + Flink/Structured Streaming continuous mode).
- Rule of thumb: don't reach for streaming unless there's an actual latency requirement — it costs more in complexity and money.

**Medallion Architecture (Bronze / Silver / Gold)**
- Bronze: raw data as-is from source, append-only, minimal transformation (maybe type casting + lineage columns like `ingestion_timestamp`, `source_file`).
- Silver: cleaned, deduplicated, conformed — joins across sources, business-rule filters, one row per business key.
- Gold: aggregated, business-level tables ready for BI/ML — usually star-schema shaped (facts + dimensions) or pre-aggregated marts.

---

## 2. Data Modeling (Star Schema & Friends)

**Star Schema**
- One **fact table** (events/transactions: sales, clicks, crashes) surrounded by **dimension tables** (who/what/where/when: customer, product, store, date).
- Fact table holds foreign keys to dimensions + numeric **measures**.
- Denormalized dimensions (no snowflaking) → fewer joins → faster BI queries. Trade-off: some redundancy in dimension tables.

**Snowflake Schema**
- Dimensions normalized into sub-dimensions (e.g., `product → category → department`). Saves storage, costs more joins. Rarely worth it on modern columnar storage — prefer star schema unless a dimension is huge and changes independently.

**Fact table types**
- Transaction fact: one row per event (a sale, a click).
- Periodic snapshot: one row per entity per time period (daily account balance).
- Accumulating snapshot: one row per process, updated as it moves through stages (order → shipped → delivered).
- Measures: additive (sum across all dimensions, e.g. revenue), semi-additive (sum across some dims, not time, e.g. account balance), non-additive (ratios, percentages — never sum, recompute instead).

**Dimension concepts**
- Surrogate key: system-generated integer PK for a dimension row, independent of the source system's natural/business key. Always use these in a warehouse — natural keys change, get reused, or collide across source systems.
- Slowly Changing Dimensions (SCD):
  - **Type 0**: never changes (immutable attribute).
  - **Type 1**: overwrite old value, no history kept (e.g., fix a typo in a name).
  - **Type 2**: keep full history — new row per change, with `effective_start_date`, `effective_end_date`, `is_current` flag. Most common when you need "what did the customer's address look like on the date of this order."
  - **Type 3**: keep only previous value in an extra column (`current_value`, `previous_value`) — rarely used, limited history.
  - **Type 6**: hybrid of 1+2+3.
- Conformed dimension: a dimension (e.g., `dim_date`, `dim_customer`) shared across multiple fact tables/marts so metrics are comparable across subject areas.
- Junk dimension: combine several small low-cardinality flags/indicators into one dimension table to keep the fact table narrow.
- Degenerate dimension: a dimension attribute (like an order number) stored directly in the fact table with no separate dimension table, because it has no other attributes.

**Kimball vs Inmon**
- Kimball: bottom-up, build dimensional marts per business process first, denormalized, faster to deliver value.
- Inmon: top-down, build a normalized enterprise data warehouse (3NF) first, then derive marts from it. More upfront modeling effort, stronger single source of truth.
- Most real-world lakehouse projects today do a hybrid: normalized/conformed Silver layer, dimensional Gold layer.

**Normalization quick reference**
- 1NF: atomic values, no repeating groups.
- 2NF: 1NF + no partial dependency on a composite key.
- 3NF: 2NF + no transitive dependency (non-key attributes depend only on the key).
- OLTP systems target 3NF; OLAP/Gold layers intentionally denormalize for query speed.

---

## 3. SQL Deep Dive

**Joins**
- INNER, LEFT/RIGHT OUTER, FULL OUTER, CROSS, SELF join.
- Common bug: LEFT JOIN + filter on the right table in `WHERE` silently turns it into an INNER JOIN — put the filter in the `ON` clause instead if you need to keep unmatched left rows.

**Window functions** (use constantly for dedup, ranking, running totals)
```sql
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)   -- dedup: keep latest row
RANK() / DENSE_RANK() OVER (...)                                       -- ranking with/without gaps
LAG(col) / LEAD(col) OVER (...)                                        -- compare to previous/next row
SUM(amount) OVER (PARTITION BY customer_id ORDER BY event_date)        -- running total
```
Dedup pattern:
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
  FROM staging_table
) WHERE rn = 1
```

**CTE vs subquery vs temp view**: CTEs are for readability, not performance — most engines inline them. Materialize as a temp table if reused many times over large data.

**Indexing (OLTP mainly)**
- B-tree index for equality/range lookups; composite index column order matters — put the most selective / most-filtered column first.
- An index helps writes get slower (every insert/update maintains it) — don't over-index write-heavy tables.

**Reading query plans**
- `EXPLAIN` / `EXPLAIN ANALYZE` (Postgres), `EXPLAIN` (Spark SQL) — look for full table scans where an index/partition pruning should apply, nested loop joins on large tables (should be hash/merge join), and row-count estimate mismatches (stale statistics → run `ANALYZE`/`ANALYZE TABLE`).

**Common SQL errors**
| Error | Cause | Fix |
|---|---|---|
| Ambiguous column reference | Same column name in joined tables | Qualify with table alias |
| Cartesian product / row explosion | Missing or wrong join condition | Check join keys, add `DISTINCT`/proper `ON` clause |
| GROUP BY error ("column must appear in GROUP BY") | Selecting a non-aggregated column not in GROUP BY | Add to GROUP BY or wrap in an aggregate/window function |
| Duplicate rows after join | Join key isn't unique on one side | Dedup that side first, or confirm intended fan-out |
| Silently wrong NULL comparisons | `WHERE col = NULL` never matches | Use `IS NULL` / `IS NOT NULL` |

---

## 4. Python / Pandas Gotchas

- `SettingWithCopyWarning`: you modified a slice/view, not the original — use `.loc[row, col] = value` or `df = df.copy()` explicitly.
- Memory blowup: pandas loads everything into RAM. For files bigger than memory, use `chunksize=`, switch to PySpark/Polars, or downcast dtypes (`pd.to_numeric(..., downcast=...)`, `category` dtype for low-cardinality strings).
- `df.apply(row-wise)` is slow — prefer vectorized operations (`df['x'] * 2`) or `.map`/`.replace` over `.apply(lambda row: ...)`.
- Merge blowing up row count: same root cause as SQL — non-unique join key on one side. Check with `df[key].duplicated().sum()` before merging.
- Timezone bugs: naive vs aware datetimes don't compare — standardize with `.dt.tz_localize()` / `.dt.tz_convert()` at ingestion.
- Virtual env / dependency conflicts: pin versions in `requirements.txt`; a common failure mode is a newer pandas/numpy major version silently breaking Spark's Arrow-based pandas UDFs — pin known-good versions together (see project-specific pin: pandas < 3 to keep PySpark's Arrow conversion working).

---

## 5. PySpark / Spark Deep Dive

**Architecture**: Driver (plans the job, holds DAG, negotiates with cluster manager) → Cluster Manager (YARN/Kubernetes/Standalone) → Executors (run tasks, hold cached data/shuffle files). One Spark application = one driver + many executors.

**RDD vs DataFrame vs Dataset**: RDD is the low-level, untyped, no-optimizer API (rarely used directly now). DataFrame is the standard API — untyped rows, but goes through the Catalyst optimizer. Dataset (Scala/Java only) adds compile-time type safety on top of DataFrame.

**Lazy evaluation**: transformations (`select`, `filter`, `withColumn`, `join`, `groupBy`) just build a logical plan; nothing runs until an **action** (`count`, `show`, `write`, `collect`) triggers execution. This is why a `.count()` right before `.write()` in test scripts effectively computes the DataFrame twice unless you `.cache()` it first.

**Narrow vs wide transformations**: narrow (`map`, `filter`, `withColumn`) — no shuffle, each partition processed independently. Wide (`groupBy`, `join`, `distinct`, `orderBy`, `repartition`) — requires a shuffle across the network, the expensive part of almost every slow Spark job.

**Partitioning**
- `repartition(n)` — full shuffle, use to *increase* partitions or fix skew.
- `coalesce(n)` — no shuffle (merges existing partitions), only *decreases* partitions, much cheaper — use before writing output to avoid a million tiny files.
- `spark.sql.shuffle.partitions` (default 200) — the number of partitions after a shuffle; tune down for small data (avoids too many tiny tasks) and up for very large data.
- Rule of thumb for output file size: aim for ~128MB–1GB per file; too many small files kills read performance on the next job (the "small file problem").

**Joins**
- Broadcast join: small table (< ~10MB–a few hundred MB, controlled by `spark.sql.autoBroadcastJoinThreshold`) is sent whole to every executor — no shuffle on the big side. Force with `broadcast(df)` if the optimizer doesn't pick it automatically.
- Sort-merge join: default for two large tables — both sides shuffled and sorted on the join key.
- Shuffle hash join: alternative to sort-merge, used when one side is small enough to build a hash map per partition but too big to broadcast.
- **Data skew**: one join key has vastly more rows than others → one task takes forever while others finish instantly. Fix with **salting** (add a random suffix to the skewed key, explode the small side to match, join, then strip the suffix) or enable AQE skew join optimization.

**Caching/persistence**: `.cache()` (= `MEMORY_AND_DISK`) or `.persist(StorageLevel.X)` when a DataFrame is reused multiple times (e.g., used in two downstream branches). Always `.unpersist()` when done to free executor memory. Don't cache something used only once — pure overhead.

**Catalyst optimizer & AQE**
- Catalyst does predicate pushdown (filter as early/close to the source as possible, e.g. into the Parquet reader) and projection pushdown (only read columns actually needed).
- Adaptive Query Execution (AQE, on by default in Spark 3+): re-optimizes the plan at runtime using actual data statistics — coalesces small shuffle partitions automatically, converts sort-merge to broadcast join if a table turns out smaller than expected, and handles skewed joins automatically (`spark.sql.adaptive.skewJoin.enabled`).

**File formats**
- Parquet: columnar, compressed, splittable, predicate pushdown support — default choice for analytics.
- ORC: similar to Parquet, common in Hive-heavy stacks.
- Avro: row-based, schema embedded, good for streaming/Kafka payloads and schema evolution.
- CSV/JSON: human-readable, no compression/columnar benefits, no enforced schema — fine for landing zone, avoid for Silver/Gold.
- Compression: Snappy (fast, splittable, default for Parquet) vs Gzip (higher ratio, slower, not always splittable).

**UDFs**
- Prefer built-in `pyspark.sql.functions` over Python UDFs — built-ins run inside the JVM with Catalyst optimization; a plain Python UDF serializes data to a Python process row-by-row (slow, no optimizer visibility).
- If you must use a UDF, prefer a **pandas UDF** (`@pandas_udf`) — vectorized, uses Arrow to batch rows to Python, far faster than a row-at-a-time UDF.

**Common Spark errors**
| Error | Typical cause | Fix |
|---|---|---|
| `OutOfMemoryError` (executor) | Too much data per partition, or a big broadcast, or huge shuffle spill | Increase partitions (`repartition`), increase executor memory, reduce broadcast threshold, avoid `collect()` on large data |
| Driver OOM on `.collect()` / `.toPandas()` | Pulling entire distributed dataset into the single driver process | Aggregate/filter first, use `.take(n)`/`.limit(n)`, or write to storage instead of collecting |
| Job stuck on one task ("straggler") | Data skew on a join/groupBy key | Salting, enable AQE skew handling, check key distribution with `df.groupBy(key).count().orderBy(desc)` |
| `AnalysisException: cannot resolve column` | Column name typo, or schema drifted between reads | Print `df.printSchema()`, check upstream source, use `try_cast`/explicit schema on read |
| Excess shuffle spill to disk (seen in Spark UI) | Not enough executor memory relative to data / too few shuffle partitions | Increase `spark.sql.shuffle.partitions`, add memory, filter earlier |
| Task not serializable | Referencing a non-serializable object (e.g. a DB connection or Spark session) inside a UDF/lambda closure | Create the resource inside the executor code (e.g. `mapPartitions`), not on the driver before shipping the closure |
| Thousands of tiny output files | Too many partitions relative to data volume before write | `coalesce(n)` before `.write()`, or enable Delta's auto-optimize |
| `Py4JJavaError` on startup | JVM/Java version mismatch, or `JAVA_HOME` not pointing at a Spark-supported JDK | Pin to a supported JDK version (e.g. JDK 17 for recent Spark), check `JAVA_HOME` |

---

## 6. Delta Lake / Lakehouse

- **ACID transactions** on object storage via a transaction log (`_delta_log/` — JSON commit files + periodic Parquet checkpoints).
- **Time travel**: `SELECT * FROM table VERSION AS OF 12` or `TIMESTAMP AS OF '2026-09-01'` — query/restore a prior state.
- **Schema enforcement vs evolution**: writes reject mismatched schema by default (enforcement); opt in to `mergeSchema` option to allow evolution when the source legitimately adds columns.
- **MERGE INTO (upsert)** — the standard SCD Type 2 implementation pattern:
```sql
MERGE INTO dim_customer AS target
USING staging_customer AS source
ON target.customer_id = source.customer_id AND target.is_current = true
WHEN MATCHED AND target.address <> source.address THEN
  UPDATE SET target.is_current = false, target.effective_end_date = current_date()
WHEN NOT MATCHED THEN
  INSERT (customer_id, address, effective_start_date, is_current)
  VALUES (source.customer_id, source.address, current_date(), true)
```
- **OPTIMIZE**: compacts small files into larger ones (fixes the small-file problem after many appends/streaming micro-batches).
- **ZORDER BY (col)**: co-locates related data within files for a column commonly filtered on, improving data-skipping on reads.
- **VACUUM**: physically deletes files no longer referenced by the transaction log after a retention window (default 7 days) — needed to actually reclaim storage cost; also removes the ability to time-travel past that point.
- **Concurrency conflicts** (`ConcurrentAppendException`, `ConcurrentDeleteReadException`): two writers touched overlapping data/partitions at once. Fix by partitioning writes so concurrent jobs touch disjoint partitions, or serialize the conflicting jobs, or retry with backoff.
- **Auto-compaction / Optimize Write** (Databricks): can be turned on per-table to avoid manually running OPTIMIZE.

---

## 7. Streaming (Kafka + Structured Streaming)

**Kafka fundamentals**
- Topic → partitions (unit of parallelism and ordering — ordering guaranteed only within a partition, not across the topic).
- Producer writes to partitions (by key hash, or round-robin if no key).
- Consumer group: each partition consumed by exactly one consumer within a group → scale consumers up to (at most) the partition count for parallelism.
- Offsets: consumer's position per partition; committing offsets too early risks message loss on crash, too late risks reprocessing (duplicates) — design consumers to be idempotent downstream.
- Replication factor: copies of each partition across brokers for durability.

**Structured Streaming (Spark)**
- `spark.readStream.format("kafka"/"delta"/"cloudFiles"...)...load()` then `.writeStream.format(...).option("checkpointLocation", ...).start()`.
- **Checkpointing**: stores offsets + state so a restarted stream resumes exactly where it left off — never point two streaming queries at the same checkpoint directory.
- **Trigger modes**: default micro-batch (as fast as possible), `trigger(processingTime="1 minute")` (fixed interval), `trigger(availableNow=True)` (process everything currently available then stop — good for turning a "streaming" job into a scheduled batch-like run), `trigger(continuous=...)` (true low-latency, limited operation support).
- **Output modes**: `append` (only new rows, no updates to old output — most sinks), `update` (changed rows since last trigger), `complete` (whole result table every trigger — only for aggregations that fit in memory).
- **Watermarking**: `withWatermark("event_time", "10 minutes")` tells Spark how late data is allowed to arrive before a windowed aggregation is finalized and old state is dropped — without it, streaming aggregation state grows unbounded forever.
- **Auto Loader** (Databricks `cloudFiles` format): incrementally and efficiently discovers new files landing in cloud storage without listing the whole directory each run — the production version of the manual checkpoint-file pattern used for small batch jobs.

**Common streaming failures**
| Symptom | Cause | Fix |
|---|---|---|
| Stream won't restart / wrong data reprocessed | Corrupted or deleted checkpoint directory | Restore checkpoint from backup, or if acceptable, start a fresh checkpoint (accepting reprocessing/dedup downstream) |
| Ever-growing state / OOM in aggregation | No watermark set on a windowed aggregation | Add `withWatermark`, tune the threshold to match real event lateness |
| Duplicate records downstream | At-least-once delivery + no idempotency | Use a dedup key + `MERGE`, or Delta's `foreachBatch` with a dedup step |
| Growing consumer lag | Consumers slower than producer rate, or too few partitions/consumers | Scale out consumers (up to partition count), optimize per-record processing cost |
| Small file explosion in streaming sink | Every micro-batch writes new small files | Enable auto-compaction / periodic `OPTIMIZE`, or increase trigger interval |

---

## 8. Orchestration (Airflow / ADF / Databricks Workflows)

**Airflow**
- DAG = tasks + dependencies (`task_a >> task_b`), scheduled by a cron-like `schedule_interval`.
- Operators: `PythonOperator`, `BashOperator`, `SparkSubmitOperator`, `DatabricksSubmitRunOperator`, etc. Sensors wait for a condition (file arrival, external task completion) before proceeding.
- XComs: small metadata passed between tasks (not for large data — use storage paths instead).
- Retries/backoff: set `retries` + `retry_delay` per task; use exponential backoff for flaky external dependencies (APIs, locks).
- Backfill: rerun a DAG for historical dates (`airflow dags backfill -s <start> -e <end>`) — make tasks idempotent so backfills don't double-count.
- Idempotency is the golden rule: a task run twice for the same logical date should produce the same result (e.g., overwrite a partition rather than append).

**Common Airflow failure modes**
| Symptom | Cause | Fix |
|---|---|---|
| Task stuck in "scheduled"/never runs | Not enough worker slots, or a pool/queue misconfiguration | Check executor capacity, pool slots, worker logs |
| DAG doesn't trigger on schedule | `schedule_interval` semantics (runs *after* the interval ends), or `catchup=False` skipping backlog | Understand execution_date vs actual run time; set `catchup` intentionally |
| Task fails silently downstream | Upstream task "succeeded" but produced no/partial data | Add data-quality checks as an explicit task, not just a completion check |
| Duplicate data after retry | Task wasn't idempotent (e.g., pure `INSERT` instead of `MERGE`/overwrite) | Make writes idempotent — overwrite by partition or MERGE on key |

**ADF (Azure Data Factory)**: pipelines made of activities (Copy Activity, Data Flow, Databricks Notebook activity), linked services (connection info), datasets, triggers (schedule/tumbling window/event-based). Integration Runtime is the compute that actually moves data (Azure IR for cloud-to-cloud, Self-Hosted IR to reach on-prem sources).

**Databricks Workflows/Jobs**: multi-task jobs with dependencies, retries, and cluster reuse across tasks; can trigger on schedule, on file arrival, or via API — increasingly replaces external orchestrators for Databricks-only pipelines.

---

## 9. Cloud Storage (ADLS / S3 / GCS)

- ADLS Gen2 = Blob Storage + hierarchical namespace (real directories, atomic rename/move, POSIX-like ACLs) — accessed as `abfss://container@account.dfs.core.windows.net/path`.
- S3: no true hierarchical namespace (keys just contain `/`, prefixes simulate folders) — `s3a://bucket/path` in Spark.
- Auth: connection string / account key (simplest, least secure), SAS token (scoped, time-limited), service principal / managed identity / IAM role (preferred for production — no long-lived secrets in code).
- Partitioning on storage: `key=value` folder convention (`event_date=2026-09-17/`) lets engines prune partitions at read time instead of scanning everything — always partition by a column commonly filtered on (usually date), and avoid over-partitioning (too many tiny partitions = too many small files = slow listing).
- Lifecycle policies: auto-tier or delete old raw data (e.g., move Bronze older than 90 days to cool/archive storage) to control cost.

---

## 10. Data Quality & Governance

- **Validation layers**: schema validation (types/required fields) → business rule validation (ranges, referential integrity, uniqueness) → statistical/anomaly checks (row count vs historical average, null rate spike).
- **Tools**: Great Expectations, dbt tests (`unique`, `not_null`, `relationships`, `accepted_values`), Deequ (Spark-native), or hand-rolled checks like this project's `validate_event()` required-field check.
- **Batch metadata / audit pattern**: record `total_events`, `valid_events`, `invalid_events`, `batch_status` per run (as this project does) — the minimum viable observability for a batch pipeline; lets you alert on `PARTIAL_SUCCESS`/failure trends without reading logs.
- **Governance**: Unity Catalog / AWS Lake Formation / Purview for centralized access control, lineage, and audit logging across a lakehouse. RBAC at the catalog/schema/table/row/column level; PII columns tagged and masked/encrypted for non-privileged roles.
- **Lineage**: track `source_file`, `ingestion_timestamp`, and pipeline/job name on every Bronze row (as this project already does) — the cheapest form of lineage, and the first thing you need when debugging "why does this Gold number look wrong."

---

## 11. CI/CD & Version Control for Data Pipelines

- Git branching: feature branch → PR → code review → merge; never commit secrets (`.env`, connection strings) — use `.gitignore` + a secrets manager (Key Vault/Secrets Manager) in production.
- Testing PySpark code: `pytest` + a local `SparkSession` fixture; libraries like `chispa` (DataFrame equality assertions) or manual `.collect()` comparisons on small fixture data.
- Databricks-specific: Databricks Asset Bundles (DABs) or Repos + CI (GitHub Actions/Azure DevOps) to deploy notebooks/jobs across dev/staging/prod workspaces instead of manual notebook edits in production.
- Config/secrets: never hardcode connection strings — load from `.env`/environment variables locally (as this project does via `python-dotenv`) and from a managed secret store (Azure Key Vault, AWS Secrets Manager, Databricks secret scopes) in deployed environments.
- Environment parity: keep a local/dev path (local filesystem or a dev container) that mirrors the production cloud path so you can iterate cheaply before pointing code at real cloud storage — the same pattern as this project's local vs `abfss://` bronze scripts.

---

## 12. Performance & Cost Optimization Playbook

1. **Filter and select early** — push predicates/column pruning as close to the source read as possible.
2. **Right-size partitions** — target ~128MB–1GB per output file; too many small files or too few huge files both hurt.
3. **Broadcast small dimension tables** in joins instead of shuffling both sides.
4. **Cache only what's reused**, and unpersist when done.
5. **Avoid `collect()`/`toPandas()`** on anything that isn't guaranteed small.
6. **Use columnar formats (Parquet/Delta)** with compression (Snappy) instead of CSV/JSON for anything beyond the landing zone.
7. **Partition storage by a commonly-filtered column** (usually date) so downstream reads skip irrelevant data.
8. **Enable AQE** (on by default in modern Spark) to auto-tune shuffle partitions and skew handling.
9. **Compact small files periodically** (`OPTIMIZE` in Delta, or a manual `coalesce` + rewrite job).
10. **Autoscale clusters + use spot/preemptible instances** for non-critical batch workloads to cut compute cost.
11. **Turn off unused clusters** (auto-termination) — idle interactive clusters are one of the biggest silent cost leaks on Databricks.
12. **Measure before optimizing** — use the Spark UI (stages, tasks, shuffle read/write, GC time) to find the actual bottleneck rather than guessing.

---

## 13. Troubleshooting Runbook (Symptom → Cause → Fix)

| # | Symptom | Likely Cause | Resolution |
|---|---|---|---|
| 1 | Spark job fails with `OutOfMemoryError` | Skewed/oversized partitions, too-large broadcast, or driver collecting too much data | Repartition, increase executor memory, lower broadcast threshold, avoid `collect()`/`toPandas()` on large data |
| 2 | One task takes forever, rest finish fast | Data skew on join/groupBy key | Salt the key, enable AQE skew join, pre-aggregate before joining |
| 3 | Pipeline reprocesses the same data every run, counts double | No checkpoint/watermark tracking processed files | Implement a checkpoint/manifest (see this project's `bronze_checkpoint.py`) or use Auto Loader |
| 4 | Millions of tiny files in storage, reads are slow | Too many partitions before write, or frequent small streaming writes | `coalesce()` before write, run periodic compaction/`OPTIMIZE` |
| 5 | `AnalysisException`/schema mismatch reading a table | Upstream schema drifted (new/renamed/retyped column) | Inspect with `printSchema()`, use explicit schema on read, enable/guard schema evolution deliberately |
| 6 | Duplicate rows appear after a retry | Non-idempotent write (`append`/`INSERT` instead of overwrite/MERGE) | Make writes idempotent: overwrite by partition or `MERGE` on business key |
| 7 | Airflow/orchestrator task never starts | Worker/pool slots exhausted, or dependency not satisfied | Check scheduler/worker logs, pool capacity, upstream task status |
| 8 | Streaming job won't resume correctly after restart | Checkpoint directory corrupted, deleted, or shared between two queries | Restore from backup checkpoint, or start fresh with a documented reprocessing/dedup plan |
| 9 | Streaming aggregation state keeps growing / eventual OOM | No watermark on windowed aggregation | Add `withWatermark` sized to real-world event lateness |
| 10 | Delta write fails with `ConcurrentAppendException` | Two jobs wrote overlapping partitions at the same time | Partition writes to be disjoint across concurrent jobs, or serialize/retry with backoff |
| 11 | Query is slow, but data volume seems small | Missing partition pruning, stale table statistics, or unnecessary shuffle | Check the query plan (`EXPLAIN`), run `ANALYZE TABLE`, verify partition filter is actually being pushed down |
| 12 | Cloud storage auth failure (`403`/`AuthenticationFailed`) | Expired SAS token, wrong connection string, missing role assignment | Rotate/verify credentials, confirm the identity has the right RBAC role on the storage account/container |
| 13 | Job works locally, fails in cluster/cloud | Environment/dependency mismatch (Python, JDK, library versions), or local-only file paths | Pin dependency versions across environments, parameterize paths (no hardcoded local paths) |
| 14 | Numbers in Gold/BI don't match Silver | Double-counting from a fan-out join, or an aggregation done before dedup | Check join cardinality before aggregating; dedup on business key first |
| 15 | `Py4JJavaError` / Spark won't start | JVM/JDK version incompatible with the Spark version | Use a Spark-supported JDK (e.g. JDK 17 for recent Spark 3.x/4.x), fix `JAVA_HOME` |
| 16 | Kafka consumer lag growing steadily | Consumers slower than producers, or too few partitions for parallelism | Scale consumers (up to partition count), profile per-message processing cost, consider increasing partitions |
| 17 | pandas job crashes with memory error on a "normal-sized" file | Inefficient dtypes (object/string columns not downcast), or loading everything at once | Downcast dtypes, use `chunksize`, or move the job to Spark/Polars |
| 18 | `.env`/secrets not picked up | Wrong working directory, `.env` not loaded before use, or var name mismatch | Confirm `load_dotenv()` runs before the variable is read; print (don't log) the resolved config keys during debugging |

---

## 14. Interview Rapid-Fire Cheat Sheet

- **ETL vs ELT** → ELT loads raw first, transforms in-warehouse; standard with cloud compute.
- **Star vs snowflake schema** → star = denormalized dims, faster BI queries; snowflake = normalized dims, saves space, more joins.
- **SCD Type 2** → new row per change + effective dates + `is_current` flag, implemented via `MERGE INTO`.
- **Partition vs bucket** → partition = folder-level pruning (usually date); bucketing = fixed hash-based file split for join/aggregation performance on a high-cardinality key.
- **repartition vs coalesce** → repartition shuffles (can increase or decrease, balances data); coalesce merges partitions without a shuffle (decrease only, cheaper).
- **Broadcast join** → send the small table to every executor to avoid shuffling the large one.
- **Data skew fix** → salting the join key, or AQE automatic skew handling.
- **Idempotency** → a rerun/backfill must produce the same result — the core reliability principle in both orchestration and streaming.
- **Watermarking** → bounds how late streaming data can arrive before state is dropped, keeping aggregation state finite.
- **Delta Lake value-add over plain Parquet** → ACID transactions, time travel, schema enforcement/evolution, `MERGE` support.
- **Medallion architecture** → Bronze (raw + lineage), Silver (cleaned/conformed/deduped), Gold (business-level aggregates/star schema).
- **Small file problem** → too many tiny files from over-partitioning or frequent small writes; fix with compaction/`OPTIMIZE`/`coalesce`.

---

*This document is a living reference — add a new row to the troubleshooting table (Section 13) every time you hit and solve a new failure, so the doc grows with real experience instead of staying generic.*
