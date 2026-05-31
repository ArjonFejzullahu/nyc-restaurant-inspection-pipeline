# NYC Restaurant Inspection Pipeline

**Dataset:** DOHMH New York City Restaurant Inspection Results

**Source:** [NYC Open Data](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j)

## Goal

Build an end-to-end Big Data pipeline on the full NYC restaurant inspection dataset, covering ingest, store, process, expose, and justify stages.

## Architecture

| Layer | Role |
|-------|------|
| **Bronze** | Raw CSV from NYC Open Data / Socrata API |
| **Silver** | Cleaned Parquet after validation, deduplication, typing, and missing-value handling |
| **Gold** | Analytical fact and dimension tables for inspection analysis |

Downstream steps profile raw data, expose CSV summaries and charts, and document architectural decisions.

Implementation in progress.
