Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
###################################################################################################################
# Resource-Constrained Shell Orchestration Engine & dbt Star Schema Platform

An enterprise-grade, lightweight data transformation and orchestration architecture engineered natively inside an isolated Linux environment (WSL2 Ubuntu) operating under tight local hardware constraints. 

## 🏗️ Architectural Topology Overview

This platform processes raw ingestion networks, builds verified dimensional analytical star schemas, and runs continuous data quality validations. To maximize available system memory, this project intentionally decouples from resource-heavy orchestration frameworks (like Apache Airflow) in favor of a low-overhead, zero-dependency POSIX shell execution wrapper.


┌──────────────────────────────────────┐
│      Native Linux Cron Daemon        │
│        (Interval: 30 Mins)           │
└──────────────────┬───────────────────┘
│
▼
┌──────────────────────────────────────┐
│      Linux flock Advisory Lock       │
│   (Prevents Overlapping Triggers)    │
└──────────────────┬───────────────────┘
│
▼
┌──────────────────────────────────────┐
│   POSIX Engine (orchestrate.sh)      │
│  - Environment Context Isolation    │
│  - Shell Error-Code Redirections     │
└──────────┬────────────────────┬──────┘
│                    │
▼                    ▼
┌───────────────────────────┐┌───────────────────────────┐
│    dbt Transformation     ││   dbt Quality Assertions  │
│ (Staging Views -> Marts)  ││ (Unique, Not-Null Guards) │
└─────────────┬─────────────┘└─────────────┬─────────────┘
│                            │
└─────────────┬──────────────┘
│
▼
┌──────────────────────────────────────┐
│      PostgreSQL Target Container     │
│   - Analytics Warehouse Tables       │
│   - audit.orchestration_logs Ledger  │
└──────────────────────────────────────┘┌──────────────────────────────────────┐
│      Native Linux Cron Daemon        │
│        (Interval: 30 Mins)           │
└──────────────────┬───────────────────┘
│
▼
┌──────────────────────────────────────┐
│      Linux flock Advisory Lock       │
│   (Prevents Overlapping Triggers)    │
└──────────────────┬───────────────────┘
│
▼
┌──────────────────────────────────────┐
│   POSIX Engine (orchestrate.sh)      │
│  - Environment Context Isolation    │
│  - Shell Error-Code Redirections     │
└──────────┬────────────────────┬──────┘
│                    │
▼                    ▼
┌───────────────────────────┐┌───────────────────────────┐
│    dbt Transformation     ││   dbt Quality Assertions  │
│ (Staging Views -> Marts)  ││ (Unique, Not-Null Guards) │
└─────────────┬─────────────┘└─────────────┬─────────────┘
│                            │
└─────────────┬──────────────┘
│
▼
┌──────────────────────────────────────┐
│      PostgreSQL Target Container     │
│   - Analytics Warehouse Tables       │
│   - audit.orchestration_logs Ledger  │
└──────────────────────────────────────┘

## 💻 System Configuration & Hardware Bounds

The development stack is explicitly throttled to run smoothly within an 8GB RAM host machine environment using strict virtualization caps:
* **Operating System Layer:** Windows 10 Home/Pro with active Windows Subsystem for Linux 2 (WSL2 Ubuntu layer).
* **Hardware Ceiling Limits (`.wslconfig`):**
  * `memory=3GB` (Strict hardware cap to prevent host system freezes).
  * `processors=2` (Capped at 2 virtual CPU cores max).
  * `swap=1GB` (Virtual memory overflow buffer).
* **Database Execution Layer:** Containerized PostgreSQL 15-Alpine container (`local_postgres_engine`) bound directly to the native Linux loopback (`127.0.0.1:5432`).
* **Concurrency Configuration:** dbt execution paths are restricted inside `profiles.yml` to a maximum allocation pool of `threads: 2` to prevent memory thrashing.

## ⚙️ Core Engine Engineering Features

### 1. Zero-Overhead POSIX Orchestrator (`orchestrate.sh`)
An idling Apache Airflow stack consumes between **1.2GB and 1.5GB of RAM** purely to run its metadata scheduler loops and web server instances. This execution wrapper provides identical production-grade pipeline reliability inside a **<15MB system memory footprint**:
* **Deterministic Fault Traps:** Built with strict Bash error modes (`set -eo pipefail`), forcing immediate execution abandonment if an upstream component throws an unhandled exception.
* **Advisory Concurrency Locks:** Wrapped inside a Linux kernel-level file descriptor lock (`flock -n /tmp/dbt_pipeline.lock`) to prevent concurrent pipeline overlaps and data collision failures.

### 2. Analytical Star Schema Data Mart
Transforms flat transaction logs into clean, vectorized analytics layers:
* **Staging Layer (Views):** `stg_customers`, `stg_products`, and `stg_sales` mask source fields cleanly.
* **Core Warehouse Layer (Tables):** Deploys `dim_customers`, `dim_products`, and `fact_store_sales` straight to disk utilizing cryptographic MD5 surrogate hashing key networks for tracking records.

### 3. Circuit-Breaker Data Quality Assertions
Data integrity constraints are evaluated programmatically on every execution cycle. Running `dbt test` validates uniqueness constraints and referential integrity fields. If any assertion fails, the engine triggers its circuit-breaker logic, halting downstream processing and logging the exact failure vector.

### 4. Operational Telemetry Database Ledger
Pipeline states are written directly to an internal logging schema (`audit.orchestration_logs`) inside the PostgreSQL instance on every run loop:
* **Queryable Lineage Tracking:** Captures unique UUID run signatures, precise execution timestamps, and specific transformation block error outputs.
* **Execution Logs Audit Query:**
  ```sql
  SELECT run_id, pipeline_name, dbt_run_status, dbt_test_status, exit_code 
  FROM audit.orchestration_logs 
  ORDER BY execution_start DESC LIMIT 5;
  ```

## 🚀 How to Initialize the Architecture Locally

1. Clone this repository into your native Linux path layout:
   ```bash
   git clone https://github.com/vasanthramdataeng/dbt-postgres-orchestration-engine.git ~/de_workspace/dbt_transformation_layer
   ```
2. Activate your local virtual environment wrapper:
   ```bash
   source ~/de_workspace/phase1_python/de_env/activate
   ```
3. Initialize the background system scheduling automation service matrix:
   ```bash
   sudo service cron start
   ```
4. Register the production crontab execution path layout:
   ```bash
   crontab -e
   # Append the following configuration string block:
   # */30 * * * * /usr/bin/flock -n /tmp/dbt_pipeline.lock /bin/bash /home/vasanth/de_workspace/dbt_transformation_layer/orchestrate.sh >> /home/vasanth/de_workspace/dbt_transformation_layer/logs/cron_execution_history.log 2>&1
   ```
