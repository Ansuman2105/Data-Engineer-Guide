# Azure Data Engineer — Coding Scenarios & Solutions

Companion to [DATA_ENGINEER_HANDBOOK.md](DATA_ENGINEER_HANDBOOK.md) (concepts + troubleshooting runbook).
This file is **code-first**: a scenario, a working solution, and the gotcha/error you'll hit if you get it
wrong. Organized so you can jump straight to the pattern you need.

---

## Table of Contents

1. [PySpark Coding Scenarios](#1-pyspark-coding-scenarios)
2. [SQL Coding Scenarios](#2-sql-coding-scenarios)
3. [Azure-Specific Coding Scenarios](#3-azure-specific-coding-scenarios)
4. [Delta Lake Coding Scenarios](#4-delta-lake-coding-scenarios)
5. [Production-Grade Patterns (retry, idempotency, DQ checks)](#5-production-grade-patterns-retry-idempotency-dq-checks)
6. [Code-Level Error → Fix Quick Table](#6-code-level-error--fix-quick-table)

---

## 1. PySpark Coding Scenarios

### 1.1 Remove duplicates, keep the latest record per key
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc

w = Window.partitionBy("customer_id").orderBy(desc("updated_at"))

deduped_df = (
    df.withColumn("rn", row_number().over(w))
      .filter("rn = 1")
      .drop("rn")
)
```
**Gotcha**: `dropDuplicates(["customer_id"])` keeps an *arbitrary* row, not the latest — only use it when you truly don't care which duplicate survives.

### 1.2 Top-N rows per group (e.g., top 3 products by sales per category)
```python
from pyspark.sql import Window
from pyspark.sql.functions import rank, col

w = Window.partitionBy("category").orderBy(col("sales").desc())

top_n_df = (
    df.withColumn("rnk", rank().over(w))
      .filter(col("rnk") <= 3)
      .drop("rnk")
)
```

### 1.3 Running total / cumulative sum
```python
from pyspark.sql import Window
from pyspark.sql.functions import sum as _sum, col

w = Window.partitionBy("account_id").orderBy("txn_date") \
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_with_running_total = df.withColumn("running_balance", _sum(col("amount")).over(w))
```

### 1.4 Flatten nested JSON (structs + arrays)
```python
from pyspark.sql.functions import col, explode

# struct access
flat_df = df.select(
    col("device.id").alias("device_id"),
    col("device.location").alias("device_location"),
    "event_type"
)

# explode an array column into one row per element
exploded_df = df.withColumn("item", explode(col("items")))
final_df = exploded_df.select("order_id", "item.sku", "item.qty")
```
**Gotcha**: `explode` on a `null` array drops the row entirely — use `explode_outer` to keep rows where the array is null/empty.

### 1.5 Pivot and unpivot
```python
# Pivot: rows -> columns
pivot_df = df.groupBy("customer_id").pivot("month").sum("sales")

# Unpivot: columns -> rows (Spark has no native unpivot pre-3.4; use stack)
unpivot_df = df.selectExpr(
    "customer_id",
    "stack(3, 'jan', jan, 'feb', feb, 'mar', mar) as (month, sales)"
)
```

### 1.6 Handle corrupt/malformed records while reading
```python
df = (
    spark.read
    .option("mode", "PERMISSIVE")            # default: bad records -> _corrupt_record column
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(explicit_schema)                  # always pass an explicit schema in production
    .json(path)
)

bad_records_df = df.filter(col("_corrupt_record").isNotNull())
good_records_df = df.filter(col("_corrupt_record").isNull())
```
Other modes: `DROPMALFORMED` (silently drops bad rows — dangerous, use only if you log the drop count), `FAILFAST` (job fails on first bad record — good for strict pipelines that must not silently lose data).

### 1.7 Schema merge across files with slightly different schemas
```python
df = (
    spark.read
    .option("mergeSchema", "true")   # Parquet/Delta: union of all schemas seen
    .parquet(path)
)
```
For JSON landing zones, prefer passing an explicit superset schema instead of relying on inference — inference over many files means a full extra read pass and non-deterministic column ordering.

### 1.8 Broadcast join (force it when the optimizer doesn't pick it)
```python
from pyspark.sql.functions import broadcast

result_df = fact_df.join(broadcast(small_dim_df), on="dim_id", how="left")
```

### 1.9 Fix a skewed join with salting
```python
from pyspark.sql.functions import col, concat, lit, floor, rand

SALT_BUCKETS = 10

# salt the skewed (large) side
salted_fact = fact_df.withColumn("salt", floor(rand() * SALT_BUCKETS)) \
                      .withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))

# explode the small side so every salt bucket has a match
from pyspark.sql.functions import explode, array
salt_range_df = spark.range(SALT_BUCKETS).withColumnRenamed("id", "salt")
salted_dim = dim_df.crossJoin(salt_range_df) \
                    .withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))

result_df = salted_fact.join(salted_dim, on="salted_key", how="left").drop("salt", "salted_key")
```

### 1.10 Pandas UDF (vectorized) instead of a slow row-at-a-time UDF
```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def normalize(col: pd.Series) -> pd.Series:
    return (col - col.mean()) / col.std()

df = df.withColumn("normalized_amount", normalize(col("amount")))
```

### 1.11 Incremental load using a watermark column
```python
from pyspark.sql.functions import max as _max

last_watermark = spark.read.table("control.watermarks") \
    .filter("table_name = 'orders'") \
    .select("last_loaded_ts").first()[0]

incremental_df = spark.read.jdbc(url, "orders", properties=conn_props) \
    .filter(col("updated_at") > last_watermark)

new_watermark = incremental_df.agg(_max("updated_at")).first()[0]

# ... write incremental_df, then update the watermark table with new_watermark
```

### 1.12 Repartition before writing to control output file count
```python
(
    df.repartition(8, "event_date")     # or .coalesce(8) if just reducing, no shuffle needed
      .write
      .mode("append")
      .partitionBy("event_date")
      .parquet(output_path)
)
```

### 1.13 Data quality check as a reusable function
```python
def run_dq_checks(df, key_cols, not_null_cols):
    issues = []

    dup_count = df.groupBy(*key_cols).count().filter("count > 1").count()
    if dup_count > 0:
        issues.append(f"{dup_count} duplicate keys found on {key_cols}")

    for c in not_null_cols:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            issues.append(f"{null_count} nulls found in required column '{c}'")

    if issues:
        raise ValueError("Data quality check failed: " + "; ".join(issues))

    return True
```

---

## 2. SQL Coding Scenarios

### 2.1 Nth highest value (classic "second highest salary")
```sql
SELECT DISTINCT salary
FROM employees
ORDER BY salary DESC
OFFSET 1 LIMIT 1;                 -- Postgres/Spark SQL

-- Portable version using DENSE_RANK (works even with ties)
SELECT salary FROM (
  SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
  FROM employees
) t
WHERE rnk = 2;
```

### 2.2 Remove duplicates, keep latest row
```sql
DELETE FROM staging_table
WHERE ctid NOT IN (                                  -- Postgres-specific row id; use a surrogate PK elsewhere
  SELECT MIN(ctid) FROM (
    SELECT ctid, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
    FROM staging_table
  ) t WHERE rn = 1
);

-- Engine-agnostic pattern: select-then-overwrite
CREATE OR REPLACE TABLE staging_table AS
SELECT * EXCEPT(rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
  FROM staging_table
) WHERE rn = 1;
```

### 2.3 Gaps and islands (find consecutive date ranges of activity)
```sql
WITH numbered AS (
  SELECT
    customer_id,
    activity_date,
    activity_date - (ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY activity_date))::int AS grp
  FROM daily_activity
)
SELECT customer_id, MIN(activity_date) AS streak_start, MAX(activity_date) AS streak_end, COUNT(*) AS streak_len
FROM numbered
GROUP BY customer_id, grp
ORDER BY customer_id, streak_start;
```

### 2.4 Find missing dates in a range (date spine join)
```sql
WITH date_spine AS (
  SELECT generate_series('2026-01-01'::date, '2026-01-31'::date, interval '1 day')::date AS d
)
SELECT d AS missing_date
FROM date_spine
LEFT JOIN daily_activity a ON a.activity_date = date_spine.d
WHERE a.activity_date IS NULL;
```

### 2.5 Running total / moving average
```sql
SELECT
  txn_date,
  amount,
  SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
  AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_date
                     ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS trailing_7day_avg
FROM transactions;
```

### 2.6 Pivot rows to columns
```sql
SELECT customer_id,
  SUM(CASE WHEN month = 'Jan' THEN sales END) AS jan_sales,
  SUM(CASE WHEN month = 'Feb' THEN sales END) AS feb_sales
FROM monthly_sales
GROUP BY customer_id;
```

### 2.7 Recursive CTE (org hierarchy / bill of materials)
```sql
WITH RECURSIVE org_chart AS (
  SELECT employee_id, manager_id, name, 1 AS level
  FROM employees WHERE manager_id IS NULL           -- anchor: top of hierarchy

  UNION ALL

  SELECT e.employee_id, e.manager_id, e.name, oc.level + 1
  FROM employees e
  JOIN org_chart oc ON e.manager_id = oc.employee_id
)
SELECT * FROM org_chart ORDER BY level;
```

### 2.8 SCD Type 2 upsert (MERGE)
```sql
MERGE INTO dim_customer AS tgt
USING staging_customer AS src
ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
WHEN MATCHED AND (tgt.address <> src.address OR tgt.email <> src.email) THEN
  UPDATE SET is_current = FALSE, effective_end_date = CURRENT_DATE
WHEN NOT MATCHED THEN
  INSERT (customer_id, address, email, effective_start_date, effective_end_date, is_current)
  VALUES (src.customer_id, src.address, src.email, CURRENT_DATE, NULL, TRUE);
```
**Gotcha**: this MERGE only closes the old row — you still need a second `INSERT` for the new-version row when a match closes an old one (many engines require two MERGE statements or a staged approach: close old rows, then insert all new-version rows in a follow-up `INSERT ... SELECT`).

---

## 3. Azure-Specific Coding Scenarios

### 3.1 Upload/download/list files in ADLS Gen2 (Python SDK)
```python
from azure.storage.filedatalake import DataLakeServiceClient

service_client = DataLakeServiceClient.from_connection_string(conn_str)
fs_client = service_client.get_file_system_client("stb-data-lake")

# upload
file_client = fs_client.get_file_client("bronze/stb_events/part-0001.parquet")
with open("local_file.parquet", "rb") as f:
    file_client.upload_data(f.read(), overwrite=True)

# list files under a path
paths = fs_client.get_paths(path="bronze/stb_events")
for p in paths:
    print(p.name, p.is_directory)

# download
downloaded = file_client.download_file()
with open("local_copy.parquet", "wb") as f:
    f.write(downloaded.readall())
```

### 3.2 Connect Spark to ADLS Gen2 with a service principal (production pattern — no account keys)
```python
spark.conf.set("fs.azure.account.auth.type.<account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<account>.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<account>.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.<account>.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<account>.dfs.core.windows.net",
                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

df = spark.read.parquet("abfss://container@account.dfs.core.windows.net/bronze/stb_events/")
```

### 3.3 Read a secret from Azure Key Vault
```python
# On Databricks (secret scope backed by Key Vault) — never hardcode secrets in notebooks
client_secret = dbutils.secrets.get(scope="kv-scope", key="sp-client-secret")

# Plain Python (outside Databricks) using azure-identity + azure-keyvault-secrets
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://<vault-name>.vault.azure.net/", credential=credential)
client_secret = client.get_secret("sp-client-secret").value
```

### 3.4 Read from Azure SQL Database via JDBC
```python
jdbc_url = "jdbc:sqlserver://<server>.database.windows.net:1433;database=<db>"
conn_props = {
    "user": user,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(url=jdbc_url, table="dbo.orders", properties=conn_props)

# Parallelize large table reads with a partition column
df = spark.read.jdbc(
    url=jdbc_url, table="dbo.orders", properties=conn_props,
    column="order_id", lowerBound=1, upperBound=1_000_000, numPartitions=20
)
```

### 3.5 Read streaming data from Event Hubs (Kafka-compatible endpoint) with Structured Streaming
```python
kafka_options = {
    "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
    "subscribe": topic_name,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{event_hub_connection_string}";'
    ),
    "startingOffsets": "earliest",
}

raw_stream = spark.readStream.format("kafka").options(**kafka_options).load()

parsed = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), event_schema).alias("data")).select("data.*")

query = (
    parsed.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/stb_events")
    .outputMode("append")
    .start("/mnt/bronze/stb_events")
)
```

### 3.6 ADF-style incremental load pattern (watermark table + Lookup + Copy Activity), expressed as pseudocode/expressions
```
# ADF pipeline logic (expressed as steps, since ADF is JSON-based, not code):
1. Lookup Activity  -> read last_watermark from a control table
2. Copy Activity    -> source query: "SELECT * FROM orders WHERE updated_at > '@{activity('Lookup').output.firstRow.last_watermark}'"
3. Lookup Activity  -> SELECT MAX(updated_at) FROM staging_orders  (the new watermark)
4. Stored Proc / Script Activity -> UPDATE control_table SET last_watermark = '@{activity('GetNewWatermark').output.firstRow.max_ts}'
```
**Key ADF expression patterns worth memorizing**: `@pipeline().parameters.paramName`, `@activity('ActivityName').output.firstRow.columnName`, `@utcnow()`, `@formatDateTime(utcnow(),'yyyy-MM-dd')` (used constantly for date-partitioned paths).

### 3.7 Parameterize a Databricks notebook (widgets) for reuse across environments
```python
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_date", "")

env = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date") or datetime.now().strftime("%Y-%m-%d")

bronze_path = f"abfss://bronze@{env}storageaccount.dfs.core.windows.net/stb_events/"
```

---

## 4. Delta Lake Coding Scenarios

### 4.1 Upsert (MERGE) in PySpark
```python
from delta.tables import DeltaTable

delta_tbl = DeltaTable.forPath(spark, "/mnt/silver/customers")

(
    delta_tbl.alias("tgt")
    .merge(staging_df.alias("src"), "tgt.customer_id = src.customer_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

### 4.2 SCD Type 2 with PySpark + Delta MERGE
```python
from pyspark.sql.functions import current_date, lit

updates_df = staging_df.withColumn("effective_start_date", current_date()) \
                        .withColumn("effective_end_date", lit(None).cast("date")) \
                        .withColumn("is_current", lit(True))

(
    delta_tbl.alias("tgt")
    .merge(updates_df.alias("src"), "tgt.customer_id = src.customer_id AND tgt.is_current = true")
    .whenMatchedUpdate(
        condition="tgt.address <> src.address",
        set={"is_current": "false", "effective_end_date": "current_date()"}
    )
    .whenNotMatchedInsertAll()
    .execute()
)
# Note: closed rows need a follow-up append of the new version row in a two-step MERGE,
# or use whenNotMatchedBySourceInsert patterns depending on Delta version.
```

### 4.3 Compaction, Z-order, and cleanup
```python
spark.sql("OPTIMIZE silver.customers ZORDER BY (customer_id)")
spark.sql("VACUUM silver.customers RETAIN 168 HOURS")   -- 7 days, default safe minimum
```

### 4.4 Time travel
```python
df_yesterday = spark.read.format("delta").option("versionAsOf", 12).load("/mnt/silver/customers")
df_at_time = spark.read.format("delta").option("timestampAsOf", "2026-09-01").load("/mnt/silver/customers")
```

### 4.5 Handle schema evolution safely
```python
(
    new_batch_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")     # allow new columns; existing columns still type-checked
    .save("/mnt/bronze/stb_events")
)
```

---

## 5. Production-Grade Patterns (retry, idempotency, DQ checks)

### 5.1 Retry with exponential backoff (for flaky API/DB calls)
```python
import time
import random

def retry_with_backoff(fn, max_retries=5, base_delay=1):
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            sleep_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"Attempt {attempt + 1} failed ({e}), retrying in {sleep_time:.1f}s")
            time.sleep(sleep_time)
```

### 5.2 Idempotent file-based checkpoint (avoid reprocessing — same pattern used in this project's bronze_checkpoint.py)
```python
def get_new_files(all_files, processed_files_path):
    processed = set(json.load(open(processed_files_path))) if os.path.exists(processed_files_path) else set()
    return [f for f in all_files if str(f.resolve()) not in processed]

def mark_processed(files, processed_files_path):
    processed = set(json.load(open(processed_files_path))) if os.path.exists(processed_files_path) else set()
    processed.update(str(f.resolve()) for f in files)
    json.dump(list(processed), open(processed_files_path, "w"), indent=4)
```

### 5.3 Exactly-once-ish streaming writes with foreachBatch + dedup
```python
def upsert_to_delta(microbatch_df, batch_id):
    microbatch_df.createOrReplaceTempView("updates")
    microbatch_df.sparkSession.sql("""
        MERGE INTO silver.stb_events tgt
        USING updates src
        ON tgt.event_id = src.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

query = (
    parsed_stream.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", checkpoint_path)
    .start()
)
```

### 5.4 Structured logging + failure alerting pattern
```python
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def run_job():
    try:
        logging.info("Job started")
        # ... pipeline steps ...
        logging.info("Job completed successfully")
    except Exception as e:
        logging.exception(f"Job failed: {e}")
        # send_alert_to_teams_or_email(str(e))   -- hook into your alerting channel
        raise
```

---

## 6. Code-Level Error → Fix Quick Table

| Error you'll see | Where | Fix |
|---|---|---|
| `AnalysisException: Path does not exist` | `spark.read` on ADLS/S3 path | Check container/path spelling, confirm auth config is set before the read call |
| `Py4JJavaError: ... 403 Forbidden` | Reading/writing ADLS/S3 | Service principal/role missing "Storage Blob Data Contributor" (or equivalent) on the container |
| `AnalysisException: cannot resolve '...' given input columns` | DataFrame `select`/`filter` | Column name typo, or upstream schema changed — `df.printSchema()` first |
| `Delta table doesn't exist` on MERGE | `DeltaTable.forPath` | Table not yet created — create it with an initial `write.format("delta").save(path)` before merging |
| `ConcurrentAppendException` | Delta MERGE/write | Two jobs wrote the same partition concurrently — partition writers to disjoint data, or retry with backoff (5.1) |
| `pyspark.sql.utils.IllegalArgumentException: requirement failed: Partition column ... not found` | `.partitionBy()` on write | Partition column doesn't exist yet in the DataFrame — add it with `withColumn` before writing |
| `TypeError: Column is not iterable` | Using Python `and`/`or`/`in` on Spark columns | Use `&`, `|`, `~` and `.isin()` instead of Python boolean operators on Column objects |
| `Kafka consumer group rebalancing constantly` | Structured Streaming from Event Hub/Kafka | Processing time per batch too high relative to trigger interval — increase trigger interval or scale out |
| `pyodbc.InterfaceError` / JDBC connection timeout | Azure SQL JDBC read | Firewall rule on SQL server doesn't allow the client/cluster IP — add firewall rule or use private endpoint |
| `Field required and value is not present` (dbutils.secrets) | Reading Key Vault secret in Databricks | Secret scope not linked to the right Key Vault, or key name mismatch — check `dbutils.secrets.list("scope")` |

---

*Pair this with Section 13 of [DATA_ENGINEER_HANDBOOK.md](DATA_ENGINEER_HANDBOOK.md) — that file has the conceptual "why," this one has the runnable "how." Add new scenarios here as you hit them in real work.*
