from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import re

import pandas as pd

from data_gates.base_gate import BaseGate


@dataclass(frozen=True)
class GateConfig:
    """
    Demo-first config: keep it simple and readable.
    """
    required_fields: Tuple[str, ...] = (
        "claim_id",
        "member_id",
        "provider_npi",
        "service_start_local",
        "cpt_code",
    )
    # If you don't want a code list yet, leave empty and the rule won't run.
    valid_cpt_codes: Tuple[str, ...] = ()
    fail_on_severity: str = "ERROR"  # INFO < WARN < ERROR


SEVERITY_ORDER = {"INFO": 0, "WARN": 1, "ERROR": 2}


def _is_missing(series: pd.Series) -> pd.Series:
    # Treat: None, NaN, "", "null", "none", "nan" as missing
    s = series.astype("string")
    s_clean = s.str.strip().str.lower()
    return series.isna() | s_clean.isin(["", "null", "none", "nan"])


def _normalize_str(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _valid_npi(series: pd.Series) -> pd.Series:
    # NPI is typically 10 digits; for demo purposes enforce 10 digits only
    s = _normalize_str(series)
    return s.str.fullmatch(r"\d{10}")  # True when valid


def _valid_service_ts(series: pd.Series) -> pd.Series:
    """
    Demo timestamp validation.
    Accept a few common forms and attempt parse; invalid => quarantine.
    """
    s = _normalize_str(series)
    # Try parse with pandas; errors='coerce' yields NaT on failure.
    parsed = pd.to_datetime(s, errors="coerce", infer_datetime_format=True, utc=False)
    return parsed.notna()


class ClaimsGate(BaseGate):
    """
    Demo Claims Gate

    Input:  pandas DataFrame
    Output: (valid_df, quarantine_df)

    quarantine_df includes:
      - quarantine_reasons: list[str]  (per-row)
      - severity: INFO/WARN/ERROR
      - primary_reason: first reason (optional)
    """

    def __init__(self, config: Optional[GateConfig] = None):
        self.config = config or GateConfig()

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # Work on a copy so we don’t surprise callers
        df = df.copy()

        # Ensure required columns exist (missing column becomes an ERROR for all rows)
        missing_cols = [c for c in self.config.required_fields if c not in df.columns]
        for c in missing_cols:
            df[c] = pd.NA  # create col so downstream checks don't crash

        # Collect per-row reasons (list) and severity
        reasons_per_row: List[List[str]] = [[] for _ in range(len(df))]
        severity_per_row: List[str] = ["INFO"] * len(df)

        def add_reason(mask: pd.Series, reason: str, severity: str = "ERROR") -> None:
            nonlocal reasons_per_row, severity_per_row
            idxs = mask[mask].index
            for i in idxs:
                reasons_per_row[i].append(reason)
                # escalate severity
                if SEVERITY_ORDER[severity] > SEVERITY_ORDER[severity_per_row[i]]:
                    severity_per_row[i] = severity

        # Rule 0: missing required fields (ERROR)
        for col in self.config.required_fields:
            add_reason(_is_missing(df[col]), f"missing_{col}", "ERROR")

        # Rule 1: NPI format (WARN) — only check if provider_npi exists
        if "provider_npi" in df.columns:
            npi_missing = _is_missing(df["provider_npi"])
            add_reason(~npi_missing & ~_valid_npi(df["provider_npi"]), "invalid_provider_npi", "WARN")

        # Rule 2: service timestamp parseable (WARN)
        if "service_start_local" in df.columns:
            ts_missing = _is_missing(df["service_start_local"])
            add_reason(~ts_missing & ~_valid_service_ts(df["service_start_local"]), "invalid_service_timestamp", "WARN")

        # Rule 3: CPT code in allowed set (WARN) — only if list provided
        if self.config.valid_cpt_codes and "cpt_code" in df.columns:
            cpt_missing = _is_missing(df["cpt_code"])
            allowed = set(self.config.valid_cpt_codes)
            cpt = _normalize_str(df["cpt_code"])
            add_reason(~cpt_missing & ~cpt.isin(list(allowed)), "invalid_cpt_code", "WARN")

        # Rule 4: missing columns detected (ERROR for all rows)
        if missing_cols:
            for c in missing_cols:
                # only add once per row
                add_reason(pd.Series([True] * len(df), index=df.index), f"missing_column_{c}", "ERROR")

        # Build quarantine mask: any row with at least one reason
        quarantine_mask = pd.Series([len(r) > 0 for r in reasons_per_row], index=df.index)

        valid_df = df.loc[~quarantine_mask].copy()
        quarantine_df = df.loc[quarantine_mask].copy()

        # Attach metadata to quarantine rows
        quarantine_df["quarantine_reasons"] = [reasons_per_row[i] for i in quarantine_df.index]
        quarantine_df["severity"] = [severity_per_row[i] for i in quarantine_df.index]
        quarantine_df["primary_reason"] = quarantine_df["quarantine_reasons"].apply(lambda xs: xs[0] if xs else None)

        return valid_df, quarantine_df

    def metrics(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Simple metrics helper for demos.
        Call after validate() if you want counts.
        """
        valid_df, quarantine_df = self.validate(df)
        out: Dict[str, int] = {
            "rows_total": len(df),
            "rows_valid": len(valid_df),
            "rows_quarantined": len(quarantine_df),
        }
        if len(quarantine_df) > 0 and "severity" in quarantine_df.columns:
            out["quarantine_INFO"] = int((quarantine_df["severity"] == "INFO").sum())
            out["quarantine_WARN"] = int((quarantine_df["severity"] == "WARN").sum())
            out["quarantine_ERROR"] = int((quarantine_df["severity"] == "ERROR").sum())
        return out
