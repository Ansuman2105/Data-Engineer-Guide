# 🚀 Data Engineer Handbook

### A practical daily reference for Data Engineering concepts, coding patterns, troubleshooting & interview preparation.

<p align="center">

**📚 Learn → 💻 Code → 🛠️ Troubleshoot → 🎯 Prepare**

</p>

---

## 🌟 About This Repository

This repository is a **practical Data Engineering reference** created for everyday learning, problem-solving, and interview preparation.

Instead of going through hundreds of pages of theory, this handbook focuses on **quickly finding the concept, pattern, or solution you need**.

The main handbook covers concepts, architecture, best practices, troubleshooting, and interview topics, while the coding companion focuses on **real coding scenarios with working solutions and common gotchas**.

---

# 🧭 What's Inside?

| 📂 Section                      | 🔎 Topics                                                 |
| ------------------------------- | --------------------------------------------------------- |
| 🏗️ **Big Picture Concepts**    | OLTP, OLAP, ETL, ELT, Lake, Warehouse, Lakehouse          |
| 🧩 **Data Modeling**            | Star Schema, Snowflake, Facts, Dimensions, SCD            |
| 🗄️ **SQL**                     | Joins, Window Functions, CTEs, Query Plans, Common Errors |
| 🐍 **Python / Pandas**          | Common gotchas, memory, merging, datetime, dependencies   |
| ⚡ **PySpark / Spark**           | Architecture, partitions, joins, caching, AQE, UDFs       |
| 🧱 **Delta Lake**               | ACID, MERGE, Time Travel, OPTIMIZE, ZORDER, VACUUM        |
| 🌊 **Streaming**                | Kafka, Structured Streaming, Checkpointing, Watermarking  |
| 🔄 **Orchestration**            | Airflow, Azure Data Factory, Databricks Workflows         |
| ☁️ **Cloud Storage**            | ADLS, S3, GCS, authentication & partitioning              |
| ✅ **Data Quality & Governance** | Validation, audit, lineage, governance                    |
| 🔧 **CI/CD**                    | Git, testing, deployment, secrets & environments          |
| 🚀 **Performance**              | Spark & storage optimization, cost optimization           |
| 🛠️ **Troubleshooting**         | Symptom → Cause → Fix                                     |
| 🎯 **Interview Prep**           | Rapid-fire Data Engineering concepts                      |

These sections correspond to the structure of the handbook.

---

# 💻 Coding Scenarios

The companion coding guide is **code-first**:

> **Scenario → Working Solution → Gotcha**

So instead of only explaining *what* something is, it focuses on **how to solve common Data Engineering problems in code**.

### ⚡ PySpark

Examples include:

* Deduplication
* Top-N per group
* Running totals
* Nested JSON
* Pivot / Unpivot
* Corrupt records
* Schema merging
* Broadcast joins
* Data skew & salting
* Pandas UDFs
* Incremental loads
* Partition management
* Data quality checks

### 🗄️ SQL

Examples include:

* Nth highest value
* Deduplication
* Gaps & Islands
* Missing dates
* Running totals
* Moving averages
* Pivoting
* Recursive CTEs
* SCD Type 2
* MERGE patterns

### ☁️ Azure

Examples include:

* ADLS Gen2
* Azure Key Vault
* Azure SQL
* Event Hubs
* Azure Data Factory
* Databricks
* Service Principal authentication

### 🧱 Delta Lake

Examples include:

* MERGE / Upsert
* SCD Type 2
* OPTIMIZE
* ZORDER
* VACUUM
* Time Travel
* Schema Evolution

### 🏭 Production Patterns

Examples include:

* Retry with exponential backoff
* Idempotency
* Streaming deduplication
* Checkpointing
* Structured logging
* Failure handling

The coding guide organizes these into PySpark, SQL, Azure, Delta Lake, production patterns, and an error/fix table.

---

# 🗺️ Data Engineering Journey

```text
                    🚀 DATA ENGINEERING
                           │
          ┌────────────────┼────────────────┐
          │                │                │
        🐍 Python        🗄️ SQL          🧩 Modeling
          │                │                │
          └────────────────┼────────────────┘
                           │
                       ⚡ PySpark
                           │
              ┌────────────┼────────────┐
              │            │            │
          ☁️ Azure      🧱 Delta      🌊 Kafka
              │            │            │
              └────────────┼────────────┘
                           │
                    🔄 Pipelines
                           │
                    🛠️ Troubleshooting
                           │
                    🎯 Interview Prep
```

---

# 🔥 Quick Reference

### If you want to learn...

**SQL** → Start with `SQL Deep Dive`

**Spark** → Start with `PySpark / Spark Deep Dive`

**Lakehouse** → Start with `Delta Lake / Lakehouse`

**Real-time data** → Start with `Streaming`

**Azure Data Engineering** → Start with `Cloud Storage` + `Azure coding scenarios`

**Pipeline orchestration** → Start with `Airflow / ADF / Databricks Workflows`

**Production engineering** → Start with `Data Quality`, `CI/CD`, `Performance`, and `Troubleshooting`

**Interview preparation** → Jump directly to the `Interview Rapid-Fire Cheat Sheet`

---

# 🛠️ Technologies Covered

<p align="center">

`Python` · `Pandas` · `PySpark` · `SQL` · `PostgreSQL`

`Azure` · `ADLS Gen2` · `ADF` · `Databricks`

`Delta Lake` · `Kafka` · `Airflow` · `Git`

</p>

---

# 🎯 Who Is This For?

This repository can be useful for:

* 👨‍💻 Aspiring Data Engineers
* 🧑‍💻 Software Engineers moving into Data Engineering
* 📊 Data Engineers revising concepts
* 🎓 Students learning Data Engineering
* 💼 Candidates preparing for interviews
* 🔧 Engineers looking for quick troubleshooting references

---

# ⭐ Why This Handbook?

### 📖 Concept + Code

Understand the **why** and then look at the **how**.

### ⚡ Quick Lookup

Use `Ctrl + F` to jump directly to the topic you need instead of reading everything from beginning to end.

### 🛠️ Practical Gotchas

The coding guide highlights common mistakes and the problems you may encounter when implementing a pattern incorrectly.

### 🎯 Interview Friendly

Important concepts and coding patterns are organized so they can also be used for interview revision.

---

# 📚 Repository Structure

```text
📦 Data Engineering Repository
│
├── 📘 DATA_ENGINEER_HANDBOOK.md
│   └── Concepts + Architecture + Troubleshooting
│
├── 💻 AZURE_DATA_ENGINEER_CODING_SCENARIOS.md
│   └── Coding Patterns + Solutions + Gotchas
│
└── 📄 README.md
    └── Repository Guide
```

> Adjust the filenames above if your actual GitHub filenames are different.

---

# 🤝 Contributions

Found something incorrect or have a useful Data Engineering pattern to add?

You're welcome to:

```text
🍴 Fork
   ↓
🌿 Create a Branch
   ↓
💻 Make Changes
   ↓
📩 Pull Request
```

Useful additions include:

* New coding scenarios
* Troubleshooting solutions
* Interview questions
* Performance patterns
* Data Engineering best practices

---

# ⭐ Support

If you find this repository useful:

### ⭐ Star the repository

It helps the project reach more Data Engineering learners and motivates continued improvement.

---

<div align="center">

## 🚀 Learn. Code. Debug. Build.

### Made for Data Engineers who prefer practical knowledge over endless theory.

**🐍 Python | 🗄️ SQL | ⚡ Spark | ☁️ Azure | 🧱 Delta | 🌊 Streaming**

</div>
