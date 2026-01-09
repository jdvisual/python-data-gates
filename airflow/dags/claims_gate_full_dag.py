"""
Full example Airflow DAG demonstrating orchestration of the ClaimsGate.

Includes:
- file ingestion
- data quality validation
- quarantine handling
- parquet output

This DAG is provided for illustrative and portfolio purposes.
It is not production-configured.
"""
rom airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime
from pathlib import Path
import subprocess
import json

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# -----------------------------
# CONFIG (edit if you want)
# -----------------------------
BASE_DIR = "/opt/airflow/dags/data_demo"
BRONZE_DIR = f"{BASE_DIR}/bronze"
SILVER_PARQUET = f"{BASE_DIR}/silver/claims.parquet"
QUARANTINE_JSONL = f"{BASE_DIR}/quarantine/claims_quarantine.jsonl"
AUDIT_SUMMARY_JSON = f"{BASE_DIR}/audit/claims_gate_summary.json"

SNOWFLAKE_CONN_ID = "snowflake_default"

# Snowflake objects (must exist)
SILVER_STAGE = "AIRFLOW_DEMO.UTIL.SILVER_STAGE"
QUARANTINE_STAGE = "AIRFLOW_DEMO.UTIL.QUARANTINE_STAGE"
PARQUET_FF = "AIRFLOW_DEMO.UTIL.PARQUET_FF"
JSONL_FF = "AIRFLOW_DEMO.UTIL.JSONL_FF"

SILVER_TABLE = "AIRFLOW_DEMO.SILVER.CLAIMS_SILVER"
QUARANTINE_TABLE = "AIRFLOW_DEMO.QUARANTINE.CLAIMS_QUARANTINE"
AUDIT_TABLE = "AIRFLOW_DEMO.AUDIT.CLAIMS_GATE_AUDIT"


@task
def bronze_claims_path():
    ctx = get_current_context()
    ds = ctx["ds"]
    out = f"{BRONZE_DIR}/dt={ds}/claims.csv"
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    return out


@task
def generate_mock_claims(out_path: str):
    ctx = get_current_context()
    conf = (ctx.get("dag_run") and ctx["dag_run"].conf) or {}
    force = bool(conf.get("force_mock", False))
    rows = conf.get("rows")
    seed = conf.get("seed")

    cmd = ["python", "/opt/airflow/dags/generators/generate_mock_claims.py", "--out", out_path]
    if rows is not None:
        cmd += ["--rows", str(rows)]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    if force:
        cmd += ["--force"]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Mock generator failed:\n{result.stdout}\n{result.stderr}")
    return out_path


@task
def inject_bad_claims(bronze_csv: str):
    """
    Intentionally corrupt some rows to force quarantine.
    Uses empty strings (not None) to avoid NaN/'nan' weirdness.
    """
    import pandas as pd

    df = pd.read_csv(bronze_csv, dtype=str)
    df = df.fillna("")

    df.loc[:9, "member_id"] = ""     # missing required field
    df.loc[10:19, "cpt_code"] = ""   # missing required field

    df.to_csv(bronze_csv, index=False)
    return bronze_csv


@task
def read_gate_summary(summary_path: str = AUDIT_SUMMARY_JSON):
    p = Path(summary_path)
    if not p.exists():
        raise FileNotFoundError(f"Missing summary file: {summary_path}")
    return json.loads(p.read_text())


@task
def log_gate_metrics(summary: dict):
    c = summary.get("counts", {}) or {}
    return {
        "total_rows": c.get("total_rows", 0),
        "silver_rows": c.get("passed_rows", 0),
        "quarantine_rows": c.get("quarantined_rows", 0),
        "failing_quarantine_rows": c.get("fail_rows_at_or_above_fail_on", 0),
    }


@task
def load_silver_to_snowflake(silver_parquet_path: str = SILVER_PARQUET):
    p = Path(silver_parquet_path)
    if not p.exists():
        raise FileNotFoundError(f"Missing silver parquet: {silver_parquet_path}")

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    hook.run(f"PUT file://{silver_parquet_path} @{SILVER_STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;")

    hook.run(
        f"""
        COPY INTO {SILVER_TABLE} (
          claim_id,
          member_id,
          provider_npi,
          service_start_local,
          cpt_code,
          allowed_amount,
          source_file,
          load_ts
        )
        FROM (
          SELECT
            $1:claim_id::string,
            $1:member_id::string,
            $1:provider_npi::string,
            COALESCE(
              TRY_TO_TIMESTAMP_NTZ($1:service_start_local::string),
              TRY_TO_TIMESTAMP_NTZ($1:service_start_local::string, 'MM/DD/YYYY HH:MI AM')
            ) AS service_start_local,
            $1:cpt_code::string,
            $1:allowed_amount::number(12,2),
            METADATA$FILENAME::string AS source_file,
            CURRENT_TIMESTAMP() AS load_ts
          FROM @{SILVER_STAGE} (FILE_FORMAT => '{PARQUET_FF}')
        );
        """
    )

    return {"silver_loaded_from": silver_parquet_path}


@task
def load_quarantine_to_snowflake(quarantine_jsonl_path: str = QUARANTINE_JSONL):
    ctx = get_current_context()
    p = Path(quarantine_jsonl_path)
    if not p.exists():
        raise FileNotFoundError(f"Missing quarantine jsonl: {quarantine_jsonl_path}")

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        put_sql = f"""
        PUT file://{quarantine_jsonl_path}
          @{QUARANTINE_STAGE}
          AUTO_COMPRESS=FALSE
          OVERWRITE=TRUE;
        """

        copy_sql = f"""
        COPY INTO {QUARANTINE_TABLE} (
          claim_id,
          severity,
          primary_reason,
          secondary_reasons,
          quarantine_reasons,
          raw_record,
          source_file,
          load_ts
        )
        FROM (
          SELECT
            $1:claim_id::string,
            $1:severity::string,
            $1:primary_reason::string,
            $1:secondary_reasons,
            $1:quarantine_reasons,
            $1:raw_record,
            METADATA$FILENAME::string,
            CURRENT_TIMESTAMP()
          FROM @{QUARANTINE_STAGE} (FILE_FORMAT => '{JSONL_FF}')
        );
        """

        cur.execute("USE DATABASE AIRFLOW_DEMO")
        cur.execute(put_sql)
        cur.execute(copy_sql)
    finally:
        cur.close()
        conn.close()

    return {"quarantine_loaded_from": quarantine_jsonl_path, "run_id": ctx["run_id"]}


@task
def land_gate_metrics_to_snowflake(counts: dict):
    ctx = get_current_context()
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    hook.run(
        f"""
        INSERT INTO {AUDIT_TABLE}
          (run_ts, dag_run_id, total_rows, silver_rows, quarantine_rows, failing_quarantine_rows)
        VALUES
          (CURRENT_TIMESTAMP(), %(dag_run_id)s, %(total_rows)s, %(silver_rows)s, %(quarantine_rows)s, %(failing_quarantine_rows)s)
        """,
        parameters={
            "dag_run_id": ctx["run_id"],
            "total_rows": counts.get("total_rows", 0),
            "silver_rows": counts.get("silver_rows", 0),
            "quarantine_rows": counts.get("quarantine_rows", 0),
            "failing_quarantine_rows": counts.get("failing_quarantine_rows", 0),
        },
    )


with DAG(
    dag_id="claims_gate_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["claims", "gate", "quality", "snowflake"],
) as dag:

    bronze_path = bronze_claims_path()
    gen_path = generate_mock_claims(bronze_path)
    bad_data = inject_bad_claims(gen_path)

    # IMPORTANT: f-string + Jinja => use {{{{ }}}} so python doesn't eat braces

    claims_gate = BashOperator(
    task_id="claims_gate",
    bash_command=f"""
set -euo pipefail

# This line IS templated by Airflow because it's plain bash text
INPUT_CSV="{{{{ ti.xcom_pull(task_ids='inject_bad_claims') }}}}"

python - <<PY
import os, sys, runpy, math
from typing import Any

def is_missing(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, float) and math.isnan(v):
            return True
    except Exception:
        pass
    s = str(v).strip()
    return s == "" or s.lower() in ("nan","none","null")

import builtins
builtins.is_missing = is_missing

input_csv = os.environ.get("INPUT_CSV") or "{BRONZE_DIR}/claims.csv"

sys.argv = [
    "/opt/airflow/dags/claims_gate.py",
    "--input-csv", input_csv,
    "--silver-out", "{SILVER_PARQUET}",
    "--quarantine-out", "{QUARANTINE_JSONL}",
    "--summary-out", "{AUDIT_SUMMARY_JSON}",
    "--fail-on", "CRITICAL",
    "--max-fail-rows", "999999",
]

runpy.run_path("/opt/airflow/dags/claims_gate.py", run_name="__main__")
PY
""",
    env={"INPUT_CSV": "{{ ti.xcom_pull(task_ids='inject_bad_claims') }}"},
)


    summary = read_gate_summary(AUDIT_SUMMARY_JSON)
    metrics = log_gate_metrics(summary)

    silver_load = load_silver_to_snowflake(SILVER_PARQUET)
    quarantine_load = load_quarantine_to_snowflake(QUARANTINE_JSONL)
    snowflake_audit = land_gate_metrics_to_snowflake(metrics)

    bad_data >> claims_gate
    claims_gate >> summary >> metrics
    claims_gate >> silver_load
    claims_gate >> quarantine_load
    [silver_load, quarantine_load, metrics] >> snowflake_audit
