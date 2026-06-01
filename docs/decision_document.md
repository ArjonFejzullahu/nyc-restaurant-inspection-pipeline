# Architectural Decision Document

This document explains the three major architectural decisions made while building the
NYC Restaurant Inspection Pipeline. For each decision it answers: what forced the choice,
what alternatives were evaluated, what was chosen, and what was given up.

> **Note:** All pipeline scripts run on the complete ~296,000-row dataset.
> No row limits, sampling, or subsets are used at any stage.

---

## Decision 1 — Medallion Architecture (Bronze / Silver / Gold) vs a Flat Single-Stage Load

**Situation and constraint**
The NYC restaurant inspection dataset contains documented quality problems that make
loading raw data directly into an analytical layer unsafe.
When an inspection produces multiple violations, all restaurant and inspection fields are
repeated for each violation row — meaning the same inspection visit can appear 5–15 times
in the raw file.
Additionally, 3,417 records carry a `1/1/1900` placeholder in `INSPECTION DATE`,
149,722 rows (50.6%) have no `GRADE`, `BORO` is coded as `'0'` for 333 records,
and `VIOLATION DESCRIPTION` is unstructured free text.
DOHMH itself acknowledges the dataset contains illogical values from data entry or
transfer errors.
A single-stage load would push all of these problems directly into the analytical layer,
making every query result unreliable.
The Socrata API also returns 4 undocumented computed region columns
(computed_region_*) not present in the official 27-column schema; these are
dropped in the Silver stage.

**Alternatives evaluated**
- *Flat single-stage load*: download the CSV and load it directly into a relational database
  for querying. Simple and fast to implement, but any aggregation (average score per borough,
  grade trend over time) would be skewed by duplicate violation rows and null grades
  without per-query filtering logic.
- *Medallion Architecture (Bronze / Silver / Gold)*: store the raw CSV untouched as Bronze,
  apply all cleaning and deduplication in a separate Silver stage, and load only validated,
  typed records into the Gold analytical layer.

**Decision**
Medallion Architecture. Bronze preserves the original download for audit and reprocessing. Silver resolves all known quality issues before any record reaches the Gold layer.

**Cost and justification**
The staged approach adds pipeline complexity and storage overhead compared to a flat load.
For this project that cost is acceptable because the alternative — querying raw data with
50.6% null grades and repeated violation rows — would produce misleading analytical results.
The Bronze layer also directly enables **data lineage**: every Gold record is traceable to
its source CSV and pipeline run, which matters when DOHMH corrects upstream errors in a
future daily update.

**Course concept:** Data lineage — the Bronze→Silver→Gold separation means every
transformation is auditable and reprocessing is possible without re-ingesting from the source.

---

## Decision 2 — Parquet vs CSV for the Silver Layer

**Situation and constraint**
After cleaning, the Silver layer holds ~290,000 rows across 27 columns.
The dominant downstream queries filter on two or three columns —
`INSPECTION DATE` (year/month), `BORO`, and `CUISINE DESCRIPTION` —
while largely ignoring the remaining columns.
The Silver layer is never queried directly by end users; it exists solely as input to
the Gold build step.

**Alternatives evaluated**
- *CSV*: human-readable, universally compatible, no extra dependencies. However, a CSV scan
  always reads all columns regardless of the query — a full ~150 MB read every time the
  Gold build runs.
- *Parquet*: columnar storage format. Because data is stored column-by-column rather than
  row-by-row, queries that filter on `INSPECTION DATE` and `BORO` only read those columns
  from disk. Combined with row-group metadata, this enables predicate pushdown — the reader
  skips row groups that don't match the filter entirely.

**Decision**
Parquet. The Silver layer is filtered by date and borough on every Gold build run.
The dataset has 27 columns; the dominant queries read 3 of them (`INSPECTION DATE`,
`BORO`, `CUISINE DESCRIPTION`). Parquet with predicate pushdown reduces the effective scan from ~150 MB to approximately
16.5 MB (measured on the actual Silver output), skipping the remaining 24 columns entirely.

**Cost and justification**
Parquet is not human-readable and requires a compatible reader (pandas, PyArrow, Spark).
For this project that cost is acceptable because the Silver layer is an intermediate
pipeline artifact, not a file anyone opens manually.
The read performance benefit on repeated Gold builds outweighs the tooling requirement.

**Course concept:** Columnar storage and predicate pushdown — storing data column-by-column
allows the reader to skip irrelevant columns and row groups entirely, reducing I/O
proportionally to the selectivity of the filter.

---

## Decision 3 — Batch Processing vs Stream Processing

**Situation and constraint**
The Socrata API updates the dataset once per day, adding approximately 100–300 new
violation rows per increment.
The analytical questions this pipeline answers — grade trends over time, worst-performing
boroughs and cuisine types — require a complete, consistent view of the full dataset,
not sub-second latency on individual new records.

**Alternatives evaluated**
- *Stream processing*: process each new record as it arrives via the Socrata API.
  Tools like Apache Kafka or Azure Event Hubs would be required. Appropriate when results
  must reflect data within seconds or minutes of arrival — for example, a live dashboard
  showing inspection outcomes as inspectors file reports in the field.
- *Batch processing*: ingest and process the full dataset in a single scheduled run,
  once per day after the API update. The entire ~296,000-row dataset is loaded, cleaned,
  and modelled in one pipeline execution.

**Decision**
Batch processing. The source data updates once daily and the analytical questions require a full-dataset view. There is no requirement for real-time results.

**Cost and justification**
Batch processing means results are at most 24 hours stale relative to the source.
For a public health analytics use case based on inspection records — which are filed
days or weeks after an inspection visit — 24-hour staleness is operationally irrelevant.
Stream processing would add significant infrastructure complexity (message broker,
stateful processing, windowing logic) with no analytical benefit for this dataset's
update frequency.

**Course concept:** Batch processing and the MapReduce paradigm — the pipeline applies
map-style transformations (type casting, null filtering, deduplication) across all
~296,000 rows in a single bulk pass before writing output, consistent with the batch
processing model covered in the course.

---

_Document to be finalised after pipeline runs end-to-end on the full dataset._
