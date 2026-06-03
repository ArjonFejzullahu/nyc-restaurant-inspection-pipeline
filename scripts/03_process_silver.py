"""
03_process_silver.py — Process (Silver layer)

Clean the Bronze dataset: column standardisation, type conversion,
placeholder-date handling, invalid-value replacement, deduplication,
and missing-value handling. Write cleaned output to data/silver/ as Parquet.

Cleaning rules applied
----------------------
1.  Computed region columns: drop the 4 Socrata-internal geometry columns
    (computed_region_*) that are not part of the official dataset schema.
2.  Column names: strip whitespace, lowercase, replace spaces with underscores.
3.  inspection_date: parse to datetime; rows where date is 1900-01-01 set to NaT.
4.  grade_date / record_date: parse to datetime.
5.  score: coerce to numeric (Int64); non-numeric values become NA.
6.  boro: replace '0' and '0.0' with 'UNKNOWN'; strip and uppercase.
7.  cuisine_description: strip whitespace; empty strings set to NA.
8.  dba: strip whitespace; empty strings set to NA.
9.  zipcode: coerce to string; strip; replace '0', 'N/A', '' with NA.
10. critical_flag: strip whitespace; standardise to title case.
11. Deduplication: drop exact duplicate rows (all columns identical).
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_CSV = PROJECT_ROOT / "data" / "bronze" / "restaurant_inspections_raw.csv"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
SILVER_PARQUET = SILVER_DIR / "restaurant_inspections_silver.parquet"
SILVER_BLOB_NAME = "restaurant_inspections_silver.parquet"

DATE_COLUMNS = ("inspection_date", "grade_date", "record_date")
PLACEHOLDER_DATE = "1900-01-01"

# Socrata-internal geometry columns not part of the actual dataset
COMPUTED_REGION_COLUMNS = (
    "computed_region_f5dn_yrer",
    "computed_region_yeji_bk3q",
    "computed_region_sbqj_enih",
    "computed_region_92fq_4b7q",
)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for logger_name in (
        "azure",
        "azure.core.pipeline.policies.http_logging_policy",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def load_config() -> tuple[str, str | None, str]:
    env_path = PROJECT_ROOT / ".env"
    if env_path.is_file():
        load_dotenv(env_path)
        logging.info("Loaded configuration from %s", env_path)
    else:
        logging.info(
            "No .env file found. Using default local configuration. "
            "The pipeline can run locally, but Azure features require a "
            "configured .env file containing Azure connection settings.",
        )
        load_dotenv()

    pipeline_mode = os.getenv("PIPELINE_MODE", "local")
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_silver = os.getenv("AZURE_CONTAINER_SILVER", "silver")
    return pipeline_mode, connection_string, container_silver


def validate_azure_config(pipeline_mode: str, connection_string: str | None) -> None:
    if pipeline_mode.lower() == "azure" and not (
        connection_string and connection_string.strip()
    ):
        raise ValueError(
            "PIPELINE_MODE=azure requires AZURE_STORAGE_CONNECTION_STRING to be set"
        )


def upload_silver_to_azure(
    local_path: Path,
    connection_string: str,
    container_name: str,
    blob_name: str = SILVER_BLOB_NAME,
) -> None:
    """Upload the local Silver Parquet file to Azure Blob Storage."""
    from azure.storage.blob import BlobServiceClient

    logging.info("Azure upload starting")
    logging.info("Container: %s", container_name)
    logging.info("Blob name: %s", blob_name)

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(
        container=container_name,
        blob=blob_name,
    )

    with local_path.open("rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    logging.info("Azure upload succeeded")


# ── 1. Load ────────────────────────────────────────────────────────────────

def load_bronze(path: Path = BRONZE_CSV) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(
            f"Bronze CSV not found at {path}. Run 01_ingest.py first."
        )
    logging.info("Loading Bronze CSV from %s", path)
    df = pd.read_csv(path, low_memory=False, dtype=str)
    logging.info("Loaded %s rows, %s columns", f"{len(df):,}", len(df.columns))
    return df


# ── 2. Drop Socrata computed region columns ───────────────────────────────

def drop_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [c for c in COMPUTED_REGION_COLUMNS if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
        logging.info("Dropped %s computed region columns: %s", len(cols_to_drop), cols_to_drop)
    return df


# ── 3. Standardise column names ────────────────────────────────────────────

def standardise_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    logging.info("Column names standardised: %s", list(df.columns))
    return df


# ── 3. Date columns ────────────────────────────────────────────────────────

def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLUMNS:
        if col not in df.columns:
            logging.warning("Date column '%s' not found — skipping", col)
            continue

        df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)

        if col == "inspection_date":
            placeholder_mask = df[col].dt.date.astype(str).str.startswith(
                PLACEHOLDER_DATE
            )
            placeholder_count = placeholder_mask.sum()
            df.loc[placeholder_mask, col] = pd.NaT
            logging.info(
                "inspection_date: set %s placeholder 1900-01-01 values to NaT",
                f"{placeholder_count:,}",
            )

        null_count = df[col].isna().sum()
        logging.info(
            "%s: %s null/NaT values after parsing",
            col,
            f"{null_count:,}",
        )

    return df


# ── 4. Score ───────────────────────────────────────────────────────────────

def clean_score(df: pd.DataFrame) -> pd.DataFrame:
    if "score" not in df.columns:
        logging.warning("Column 'score' not found — skipping")
        return df

    before_nulls = df["score"].isna().sum()
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["score"] = df["score"].astype("Int64")
    after_nulls = df["score"].isna().sum()
    new_nulls = after_nulls - before_nulls
    logging.info(
        "score: %s null after coercion (%s new nulls from non-numeric values)",
        f"{after_nulls:,}",
        f"{new_nulls:,}",
    )
    return df


# ── 5. Boro ────────────────────────────────────────────────────────────────

def clean_boro(df: pd.DataFrame) -> pd.DataFrame:
    if "boro" not in df.columns:
        logging.warning("Column 'boro' not found — skipping")
        return df

    df["boro"] = df["boro"].astype(str).str.strip().str.upper()
    invalid_mask = df["boro"].isin(["0", "0.0", "NAN", "NONE", ""])
    invalid_count = invalid_mask.sum()
    df.loc[invalid_mask, "boro"] = "UNKNOWN"
    logging.info(
        "boro: replaced %s invalid/zero values with 'UNKNOWN'",
        f"{invalid_count:,}",
    )
    return df


# ── 6. Cuisine description ─────────────────────────────────────────────────

def clean_cuisine(df: pd.DataFrame) -> pd.DataFrame:
    if "cuisine_description" not in df.columns:
        logging.warning("Column 'cuisine_description' not found — skipping")
        return df

    df["cuisine_description"] = (
        df["cuisine_description"]
        .astype(str)
        .str.strip()
    )
    empty_mask = df["cuisine_description"].isin(["", "NAN", "None", "nan"])
    df.loc[empty_mask, "cuisine_description"] = pd.NA
    null_count = df["cuisine_description"].isna().sum()
    logging.info(
        "cuisine_description: %s null/empty values after cleaning",
        f"{null_count:,}",
    )
    return df


# ── 7. DBA (trade name) ────────────────────────────────────────────────────

def clean_dba(df: pd.DataFrame) -> pd.DataFrame:
    if "dba" not in df.columns:
        logging.warning("Column 'dba' not found — skipping")
        return df

    df["dba"] = df["dba"].astype(str).str.strip()
    empty_mask = df["dba"].isin(["", "NAN", "None", "nan"])
    df.loc[empty_mask, "dba"] = pd.NA
    null_count = df["dba"].isna().sum()
    logging.info("dba: %s null/empty values after cleaning", f"{null_count:,}")
    return df


# ── 8. Zipcode ─────────────────────────────────────────────────────────────

def clean_zipcode(df: pd.DataFrame) -> pd.DataFrame:
    if "zipcode" not in df.columns:
        logging.warning("Column 'zipcode' not found — skipping")
        return df

    df["zipcode"] = df["zipcode"].astype(str).str.strip()
    invalid_mask = df["zipcode"].isin(
        ["0", "0.0", "N/A", "n/a", "", "NAN", "nan", "None"]
    )
    df.loc[invalid_mask, "zipcode"] = pd.NA
    null_count = df["zipcode"].isna().sum()
    logging.info(
        "zipcode: %s null/invalid values after cleaning",
        f"{null_count:,}",
    )
    return df


# ── 9. Critical flag ───────────────────────────────────────────────────────

def clean_critical_flag(df: pd.DataFrame) -> pd.DataFrame:
    if "critical_flag" not in df.columns:
        logging.warning("Column 'critical_flag' not found — skipping")
        return df

    df["critical_flag"] = (
        df["critical_flag"]
        .astype(str)
        .str.strip()
        .str.title()
    )
    logging.info(
        "critical_flag distinct values after cleaning: %s",
        df["critical_flag"].unique().tolist(),
    )
    return df


# ── 10. Deduplication ──────────────────────────────────────────────────────

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    removed = before - after
    logging.info(
        "Deduplication: removed %s exact duplicate rows (%s → %s)",
        f"{removed:,}",
        f"{before:,}",
        f"{after:,}",
    )
    return df


# ── 11. Write Silver Parquet ───────────────────────────────────────────────

def write_silver(df: pd.DataFrame) -> Path:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_PARQUET, index=False, engine="pyarrow")
    size_mb = SILVER_PARQUET.stat().st_size / (1024 * 1024)
    logging.info(
        "Wrote Silver Parquet to %s (%.1f MB)",
        SILVER_PARQUET,
        size_mb,
    )
    return SILVER_PARQUET


# ── 12. Post-clean summary ─────────────────────────────────────────────────

def log_summary(df: pd.DataFrame) -> None:
    logging.info("─── Silver layer summary ───────────────────────────")
    logging.info("Rows: %s", f"{len(df):,}")
    logging.info("Columns: %s", len(df.columns))
    null_counts = df.isnull().sum()
    for col, count in null_counts[null_counts > 0].items():
        pct = 100 * count / len(df)
        logging.info("  %-35s %s nulls (%.1f%%)", col, f"{count:,}", pct)
    logging.info("────────────────────────────────────────────────────")


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> int:
    setup_logging()
    logging.info("Starting Silver processing")

    try:
        pipeline_mode, connection_string, container_silver = load_config()
        validate_azure_config(pipeline_mode, connection_string)
    except ValueError as exc:
        logging.error("Invalid configuration: %s", exc)
        return 1

    logging.info("PIPELINE_MODE=%s", pipeline_mode)

    try:
        df = load_bronze(BRONZE_CSV)
    except FileNotFoundError as exc:
        logging.error("%s", exc)
        return 1

    df = standardise_column_names(df)
    df = drop_computed_columns(df)
    df = clean_dates(df)
    df = clean_score(df)
    df = clean_boro(df)
    df = clean_cuisine(df)
    df = clean_dba(df)
    df = clean_zipcode(df)
    df = clean_critical_flag(df)
    df = deduplicate(df)

    log_summary(df)

    try:
        output_path = write_silver(df)
    except OSError as exc:
        logging.error("Failed to write Silver Parquet: %s", exc)
        return 1

    if pipeline_mode.lower() == "azure":
        assert connection_string is not None
        try:
            upload_silver_to_azure(
                output_path,
                connection_string.strip(),
                container_silver,
            )
        except Exception as exc:
            logging.error("Azure upload failed: %s", exc)
            return 1

    print(f"Silver rows: {len(df):,}")
    print(f"Silver columns: {len(df.columns)}")
    print(f"Output: {output_path}")

    logging.info("Silver processing finished successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
