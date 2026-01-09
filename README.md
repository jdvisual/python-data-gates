# Data Gates

Data Gates is a Python framework for enforcing deterministic data quality checks at ingest time, quarantining invalid records, and emitting structured metrics before data enters analytics pipelines.

---

## Overview

Data Gates is a lightweight Python framework designed to enforce deterministic data quality checks at the point of data ingestion.

Rather than allowing malformed or incomplete records to flow into downstream analytics layers, Data Gates validates incoming datasets early, separates valid and invalid records, and produces auditable quality metrics.

The goal is to **fail fast, preserve bad data for analysis, and protect downstream systems**.

---

## Problem

In many data platforms, ingestion pipelines prioritize throughput over correctness. As a result:

- Invalid records silently enter silver and gold layers
- Data quality issues surface late and are difficult to trace
- Debugging becomes reactive and expensive

---

## Solution

Data Gates introduces a clear quality boundary at ingest time.

Each dataset passes through a gate that:

- Applies explicit validation rules
- Quarantines invalid records with reason codes
- Emits metrics describing data quality and failure patterns

This creates a deterministic and explainable quality contract between ingestion and analytics.

---

## Design Principles

- **Deterministic** – rules are explicit and repeatable  
- **Explainable** – every failure has a reason  
- **Non-destructive** – bad data is quarantined, not discarded  
- **Composable** – works standalone or inside orchestration frameworks  
- **Production-aligned** – outputs common analytics-friendly formats  

---

## Outputs

- **Valid records** → Parquet (silver-ready)
- **Quarantined records** → JSONL with failure reasons
- **Metrics** → counts, categories, severity levels

---

## Intended Use

Data Gates is designed for high-volume, mixed-quality datasets such as:

- Healthcare claims
- Eligibility and enrollment feeds
- Financial transactions
- Any batch ingestion requiring auditable quality enforcement

---

## Run the Demo

```bash
pip install pandas
python examples/run_claims_gate.py
