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

        def add_reason(mask: pd.Series, reason: str,_


