#!/usr/bin/env bash

# Strict Bash Error Handling Mode
set -eo pipefail

# Infrastructure Path Declarations
WORKSPACE_DIR="$HOME/de_workspace/dbt_transformation_layer"
VENV_PATH="$HOME/de_workspace/phase1_python/de_env"
LOG_DIR="$WORKSPACE_DIR/logs/$(date +%Y-%m-%d)"
mkdir -p "$LOG_DIR"

# Initialize Telemetry Variables
PIPELINE_NAME="dbt_star_schema_pipeline"
RUN_ID=$(dbus-uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)
START_TIME=$(date -u +"%Y-%m-%d %H:%M:%S+00")

echo "[INFO] [$(date)] Initializing Run ID: ${RUN_ID} under 3GB RAM Ceiling."

# Activate isolated Python Runtime environment
source "$VENV_PATH/bin/activate"
cd "$WORKSPACE_DIR"

# Helper Function: Write operational state directly to PostgreSQL
log_state_to_db() {
    local end_time=$(date -u +"%Y-%m-%d %H:%M:%S+00")
    local run_status=$1
    local test_status=$2
    local err_msg=$3
    local code=$4

    docker exec -i local_postgres_engine psql -U postgres -d postgres >> /dev/null 2>&1 << SQL
INSERT INTO audit.orchestration_logs 
(run_id, pipeline_name, execution_start, execution_end, dbt_run_status, dbt_test_status, error_message, exit_code)
VALUES 
('${RUN_ID}', '${PIPELINE_NAME}', '${START_TIME}', '${end_time}', '${run_status}', '${test_status}', '${err_msg}', ${code})
ON CONFLICT (run_id) DO UPDATE SET 
execution_end = EXCLUDED.execution_end, 
dbt_run_status = EXCLUDED.dbt_run_status, 
dbt_test_status = EXCLUDED.dbt_test_status, 
error_message = EXCLUDED.error_message, 
exit_code = EXCLUDED.exit_code;
SQL
}

# --- CRITICAL PATH STEP 1: COMPILATION & TRANSFORMATION ---
echo "[STAGE] [$(date)] Executing dbt transformations across threads (Cap: 2)..."
if ! dbt run --profiles-dir ~/.dbt > "$LOG_DIR/dbt_run.log" 2>&1; then
    ERR_MSG=$(tail -n 5 "$LOG_DIR/dbt_run.log" | tr -d "'\"")
    echo "[CRITICAL] [$(date)] dbt run execution broken. Initiating Circuit Breaker Logic."
    log_state_to_db "FAILED" "PENDING" "$ERR_MSG" 1
    exit 1
fi
echo "[SUCCESS] [$(date)] Staging Views and Mart Tables compiled cleanly."

# --- CRITICAL PATH STEP 2: ENTERPRISE DATA QUALITY ASSERTIONS ---
echo "[STAGE] [$(date)] Triggering Schema Constraints and Referential Integration Validation Tests..."
if ! dbt test --profiles-dir ~/.dbt > "$LOG_DIR/dbt_test.log" 2>&1; then
    ERR_MSG=$(tail -n 5 "$LOG_DIR/dbt_test.log" | tr -d "'\"")
    echo "[CRITICAL] [$(date)] Data Validation Layer Failed (Unique/Not Null/FK Violation). Tripping Circuit Breaker."
    log_state_to_db "SUCCESS" "FAILED" "$ERR_MSG" 2
    exit 2
fi
echo "[SUCCESS] [$(date)] Data Quality Matrix Verified. Zero assertions tripped."

# --- PIPELINE SUCCESS CLOSURE ---
log_state_to_db "SUCCESS" "SUCCESS" "" 0
echo "[COMPLETE] [$(date)] Pipeline Execution Matrix run completed flawlessly under current resource limits."

