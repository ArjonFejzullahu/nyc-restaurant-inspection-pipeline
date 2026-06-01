# NYC Restaurant Inspection Pipeline

**Dataset:** DOHMH New York City Restaurant Inspection Results  
**Source:** [NYC Open Data](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j)  
**Team:** Besim Sallahi (124491) · Arjon Fejzulla (127872)

---

## Problem Statement

Every year, New York City health inspectors visit thousands of restaurants and college cafeterias and record violations, scores, and letter grades. The raw dataset covers every sustained or not yet adjudicated violation citation from inspections conducted up to three years prior to the most recent inspection, for all establishments in active status.

This pipeline cleans, structures, and analyzes that data to answer:

> **Which neighborhoods, cuisine types, and restaurants have the worst food safety track records, and are things getting better or worse over time?**

---

## Dataset

| Field | Details |
|---|---|
| Name | DOHMH New York City Restaurant Inspection Results |
| Source | https://data.cityofnewyork.us/resource/43nn-pn8j.json |
| Rows | ~296,000 (rolling 3-year window, updated daily) |
| Columns | 27 |
| Row grain | One violation citation per row; multiple violations from the same inspection repeat restaurant and inspection fields |
| Format | CSV download / Socrata API (JSON, XML) |

---

## Approved Architecture (Milestone 1)

This implementation follows the architecture approved in Milestone 1.

### Medallion Architecture

| Layer | Storage | Role |
|---|---|---|
| **Bronze** | `data/bronze/` (local) / Azure Blob `bronze/` | Raw CSV exactly as downloaded from Socrata API — never modified |
| **Silver** | `data/silver/` (local) / Azure Blob `silver/` | Cleaned Parquet: standardized columns, parsed dates, deduplication, null handling, normalized cuisine values |
| **Gold** | `data/gold/` (local) / Azure Blob `gold/` | Analytical star schema: `fact_inspections`, `dim_restaurant`, `dim_cuisine`, `dim_date` |

Downstream stages profile raw data, process and model inspection records, generate analytical outputs, and document architectural decisions.

### Pipeline Steps

| Step | Script |
|---|---|
| Ingest | `scripts/01_ingest.py` |
| Profile | `scripts/02_profile.py` |
| Process (Silver) | `scripts/03_process_silver.py` |
| Build Gold | `scripts/04_build_gold.py` |
| Expose | `scripts/05_expose.py` |
| Orchestrate | `scripts/run_pipeline.py` |

### Course Concepts Applied

| Concept | Where it applies |
|---|---|
| **Schema-on-read vs schema-on-write** | Bronze stores raw CSV with no enforced schema (schema-on-read); Gold enforces typed columns and foreign keys (schema-on-write) |
| **Batch processing** | The full ~296,000-row dataset is processed in bulk batches — type casting, null filtering, and deduplication applied across all records before loading |
| **Data lineage** | Bronze → Silver → Gold separation means every Gold record is traceable to its source CSV and pipeline run, enabling reprocessing when upstream issues are corrected |

---

## Setup and Reproduction

_Full setup and run commands will be documented here as the pipeline is completed._
