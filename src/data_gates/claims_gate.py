#!/usr/bin/env python3
"""
claims_gate.py

A lightweight "data quality gate" for claims CSV -> Silver Parquet + Quarantine JSONL.

- Reads input CSV (streaming via csv.DictReader)
- Validates each row (adds reasons + severity)
- Writes:
    * Silver output: Parquet of passing rows
    * Quarantine output: JSONL of failing rows (original row + reasons + severity)
    * Summary output: JSON metrics for observability / downstream branching
- Exits non-zero when:
    * any failing rows at/above --fail-on severity exist, OR
    * failed_rows > --max-fail-rows

Severity levels: LOW < MEDIUM < HIGH < CRITICAL
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import math

# ----------------------------
# Utilities / Constants
# ----------------------------

SEVERITY_RANK: Dict[str, int] = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
SEVERITIES: List[str] = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]


def is_missing(v: Any) -> bool:
    """True for None, empty strings, and NaN-ish values."""
    if v is None:
        return True

    # pandas NaN can show up as float nan
    try:
        if isinstance(v, float) and math.isnan(v):
            return True
    except Exception:
        pass

    s = str(v).strip()
    if s == "":
        return True
    return s.lower() in ("nan", "none", "null")


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_float(x: Any) -> Optional[float]:
    if is_missing(x):
        return None
    try:
        return float(str(x).strip())
    except Exception:
        return None


def parse_service_ts(s: Any) -> bool:
    """
    Accepts either:
      - 'MM/DD/YYYY HH:MI AM'  (e.g. 01/01/2026 12:29 PM)
      - ISO-like timestamps (e.g. 2026-01-01T12:29:54.077879 or ...Z)
      - 'YYYY-MM-DD HH:MM:SS'  (common)
    Returns True if parseable.
    """
    if is_missing(s):
        return False

    ss = str(s).strip()

    # Format 1: 01/01/2026 12:29 PM
    try:
        datetime.strptime(ss, "%m/%d/%Y %I:%M %p")
        return True
    except Exception:
        pass

    # Format 2: ISO timestamps (with optional Z)
    try:
        datetime.fromisoformat(ss.replace("Z", "+00:00"))
        return True
    except Exception:
        pass

    # Format 3: "YYYY-MM-DD HH:MM:SS"
    try:
        datetime.strptime(ss, "%Y-%m-%d %H:%M:%S")
        return True
    except Exception:
        return False


def normalize_row(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize basic whitespace and empty strings to None."""
    out: Dict[str, Any] = {}
    for k, v in rec.items():
        if v is None:
            out[k] = None
            continue
        s = str(v).strip()
        out[k] = s if s != "" else None
    return out


def severity_at_or_above_count(counts: Dict[str, int], threshold: str) -> int:
    t = SEVERITY_RANK[threshold]
    return sum(n for sev, n in counts.items() if SEVERITY_RANK.get(sev, 0) >= t)

import math

def is_missing(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, float) and math.isnan(v):
            return True
    except Exception:
        pass
    s = str(v).strip()
    return s == "" or s.lower() in ("nan", "none", "null")

# ----------------------------
# Validation
# ----------------------------

def validate_claim_row(rec: Dict[str, Any]) -> Tuple[List[str], str]:
    """
    Returns (reasons, severity) for a single claim record.

    Rules (simple demo; evolve as your spec grows):
    - CRITICAL: missing claim_id/member_id/cpt_code
    - HIGH: missing or unparseable service timestamp
    - MEDIUM: invalid or negative paid_amount (if present)
    - LOW: missing dx_code (optional example)

    NOTE: Any non-empty `reasons` means the row is quarantined.
    If you truly want dx_code to be *non-quarantine*, remove that check.
    """
    reasons: List[str] = []
    severity = "LOW"

    def bump(new_sev: str) -> None:
        nonlocal severity
        if SEVERITY_RANK[new_sev] > SEVERITY_RANK[severity]:
            severity = new_sev

    claim_id = rec.get("claim_id")
    member_id = rec.get("member_id")
    cpt_code = rec.get("cpt_code")
    svc_ts = rec.get("svc_date") or rec.get("service_date") or rec.get("service_start_local")
    paid_amount = rec.get("paid_amount") or rec.get("allowed_amount") or rec.get("amount")
    dx_code = rec.get("dx_code") or rec.get("icd10_code") or rec.get("diagnosis_code")

    # Required identifiers
    if is_missing(claim_id):
        reasons.append("missing_claim_id")
        bump("CRITICAL")

    if is_missing(member_id):
        reasons.append("missing_member_id")
        bump("CRITICAL")

    if is_missing(cpt_code):
        reasons.append("missing_cpt_code")
        bump("CRITICAL")

    # Service timestamp
    if is_missing(svc_ts):
        reasons.append("missing_service_ts")
        bump("HIGH")
    else:
        if not parse_service_ts(svc_ts):
            reasons.append("invalid_service_ts_unparseable")
            bump("HIGH")

    # Paid amount numeric (optional)
    if not is_missing(paid_amount):
        val = as_float(paid_amount)
        if val is None:
            reasons.append("invalid_paid_amount_not_numeric")
            bump("MEDIUM")
        elif val < 0:
            reasons.append("invalid_paid_amount_negative")
            bump("MEDIUM")

    # Optional dx code example (currently DOES quarantine, as LOW)
    if is_missing(dx_code):
        reasons.append("missing_dx_code_optional")
        bump("LOW")

    return reasons, severity


# ----------------------------
# IO
# ----------------------------

def write_silver_parquet(rows: List[Dict[str, Any]], path: str) -> None:
    """Writes passing rows as Parquet using pandas + pyarrow."""
    try:
        import pandas as pd  # type: ignore
    except Exception as e:
        raise RuntimeError("pandas is required to write Parquet in this demo gate.") from e

    try:
        import pyarrow  # noqa: F401  # type: ignore
    except Exception as e:
        raise RuntimeError("pyarrow is required to write Parquet. Install pyarrow in the Airflow image.") from e

    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)


# ----------------------------
# Args / Main
# ----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Claims gate: CSV -> Silver Parquet + Quarantine JSONL + Summary JSON")
    p.add_argument("--input-csv", required=True, help="Path to input claims CSV")
    p.add_argument("--silver-out", required=True, help="Path to write Silver Parquet")
    p.add_argument("--quarantine-out", required=True, help="Path to write Quarantine JSONL")
    p.add_argument("--summary-out", required=True, help="Path to write Summary JSON")
    p.add_argument(
        "--fail-on",
        default="CRITICAL",
        choices=SEVERITIES,
        help="Fail pipeline if any quarantined rows at/above this severity exist",
    )
    p.add_argument(
        "--max-fail-rows",
        type=int,
        default=0,
        help="Fail pipeline if failed_rows exceeds this number (0 = no failures allowed)",
    )
    p.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    p.add_argument("--encoding", default="utf-8", help="CSV encoding (default: utf-8)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    ensure_parent_dir(args.silver_out)
    ensure_parent_dir(args.quarantine_out)
    ensure_parent_dir(args.summary_out)

    total_rows = 0
    passed_rows = 0
    quarantined_rows = 0
    failed_rows = 0

    severity_counts: Dict[str, int] = {sev: 0 for sev in SEVERITIES}
    silver_buffer: List[Dict[str, Any]] = []

    run_ts = now_utc_iso()

    # Stream CSV in, write quarantine line-by-line
    with open(args.input_csv, "r", encoding=args.encoding, newline="") as f_in, \
         open(args.quarantine_out, "w", encoding="utf-8", newline="\n") as f_q:

        reader = csv.DictReader(f_in, delimiter=args.delimiter)

        if not reader.fieldnames:
            print("ERROR: input CSV has no header row.", file=sys.stderr)
            return 2

        for rec in reader:
            total_rows += 1
            rec_n = normalize_row(rec)

            reasons, severity = validate_claim_row(rec_n)

            if reasons:
                failed_rows += 1
                quarantined_rows += 1
                sev = severity.upper()
                if sev not in severity_counts:
                    # Defensive: keep counts stable
                    sev = "LOW"
                severity_counts[sev] += 1

                quarantine_record = {
                    "run_ts_utc": run_ts,
                    "severity": sev,
                    "reasons": reasons,
                    "row": rec_n,
                }
                f_q.write(json.dumps(quarantine_record, ensure_ascii=False) + "\n")
            else:
                passed_rows += 1
                silver_buffer.append(rec_n)

    # Write silver parquet (even if empty, write empty parquet if possible)
    try:
        write_silver_parquet(silver_buffer, args.silver_out)
    except Exception as e:
        summary = {
            "run_ts_utc": run_ts,
            "status": "ERROR",
            "error": str(e),
            "input_csv": args.input_csv,
            "silver_out": args.silver_out,
            "quarantine_out": args.quarantine_out,
            "summary_out": args.summary_out,
            "fail_on": args.fail_on.upper(),
            "max_fail_rows": int(args.max_fail_rows),
            "counts": {
                "total_rows": total_rows,
                "passed_rows": passed_rows,
                "quarantined_rows": quarantined_rows,
                "failed_rows": failed_rows,
            },
            "severity_counts": severity_counts,
        }
        with open(args.summary_out, "w", encoding="utf-8") as f_sum:
            json.dump(summary, f_sum, indent=2, sort_keys=True)

        print("=== CLAIMS GATE SUMMARY ===")
        print(json.dumps(summary, indent=2, sort_keys=True))
        print(f"ERROR: failed to write silver parquet: {e}", file=sys.stderr)
        return 2

    # Decide pass/fail
    at_or_above = severity_at_or_above_count(severity_counts, args.fail_on.upper())
    exceeds_max = failed_rows > int(args.max_fail_rows)

    should_fail = (at_or_above > 0) or exceeds_max
    status = "FAIL" if should_fail else "PASS"

    summary = {
        "run_ts_utc": run_ts,
        "status": status,
        "input_csv": args.input_csv,
        "silver_out": args.silver_out,
        "quarantine_out": args.quarantine_out,
        "summary_out": args.summary_out,
        "fail_on": args.fail_on.upper(),
        "max_fail_rows": int(args.max_fail_rows),
        "counts": {
            "total_rows": total_rows,
            "passed_rows": passed_rows,
            "quarantined_rows": quarantined_rows,
            "failed_rows": failed_rows,
            "fail_rows_at_or_above_fail_on": at_or_above,
        },
        "severity_counts": severity_counts,
    }

    print("=== CLAIMS GATE SUMMARY ===")
    print(json.dumps(summary, indent=2, sort_keys=True))

    with open(args.summary_out, "w", encoding="utf-8") as f_sum:
        json.dump(summary, f_sum, indent=2, sort_keys=True)

    if should_fail:
        print("=== CLAIMS GATE RESULT: FAIL ===")
        if at_or_above > 0:
            print(f"Reason: {at_or_above} quarantined rows at/above fail_on={args.fail_on.upper()}")
        if exceeds_max:
            print(f"Reason: failed_rows={failed_rows} > max_fail_rows={int(args.max_fail_rows)}")
        return 1

    print("=== CLAIMS GATE RESULT: PASS ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
