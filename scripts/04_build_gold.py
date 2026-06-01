"""
04_build_gold.py — Build analytical tables (Gold layer)

Derive fact and dimension tables from the Silver Parquet for restaurant
inspection analysis. Write tables to data/gold/ as Parquet files.

Star schema
-----------
fact_inspections  — one row per inspection visit (camis + inspection_date +
                    inspection_type). Violation rows from Silver are aggregated
                    up to inspection level: violation_count,
                    critical_violation_count, has_violation.

dim_restaurant    — one row per unique restaurant (CAMIS).
                    Contains location, borough, cuisine, contact info.

dim_cuisine       — one row per unique cuisine description.
                    Surrogate key cuisine_id.

dim_date          — one row per unique inspection date.
                    Derived calendar fields: year, month, quarter,
                    month_name, day_of_week.

Grain decisions
---------------
- Silver grain: one row per violation citation.
- Gold fact grain: one row per inspection visit. Multiple violation rows
  from the same visit (same camis + inspection_date + inspection_type)
  are collapsed via aggregation before loading.
- Restaurants with no violations appear as a single Silver row with
  violation_code = null; these produce fact rows with violation_count = 0.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_PARQUET = (
    PROJECT_ROOT / "data" / "silver" / "restaurant_inspections_silver.parquet"
)
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

FACT_PATH = GOLD_DIR / "fact_inspections.parquet"
DIM_RESTAURANT_PATH = GOLD_DIR / "dim_restaurant.parquet"
DIM_CUISINE_PATH = GOLD_DIR / "dim_cuisine.parquet"
DIM_DATE_PATH = GOLD_DIR / "dim_date.parquet"

# Columns that uniquely identify an inspection visit
INSPECTION_KEYS = ["camis", "inspection_date", "inspection_type"]


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ── Load Silver ────────────────────────────────────────────────────────────

def load_silver(path: Path = SILVER_PARQUET) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(
            f"Silver Parquet not found at {path}. Run 03_process_silver.py first."
        )
    logging.info("Loading Silver Parquet from %s", path)
    df = pd.read_parquet(path, engine="pyarrow")
    logging.info(
        "Loaded %s rows, %s columns", f"{len(df):,}", len(df.columns)
    )
    return df


# ── dim_restaurant ─────────────────────────────────────────────────────────

def build_dim_restaurant(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per unique restaurant (CAMIS).
    Take the most recent record per CAMIS to get the latest known values
    for name, address, borough, and cuisine.
    """
    cols = [
        "camis", "dba", "boro", "building", "street", "zipcode",
        "phone", "latitude", "longitude", "cuisine_description",
        "nta", "community_board", "council_district",
    ]
    available = [c for c in cols if c in df.columns]

    # Sort by record_date descending so first() gives the most recent row
    df_sorted = df.sort_values("record_date", ascending=False)
    dim = (
        df_sorted[available]
        .groupby("camis", as_index=False)
        .first()
    )

    logging.info(
        "dim_restaurant: %s unique restaurants", f"{len(dim):,}"
    )
    return dim


# ── dim_cuisine ────────────────────────────────────────────────────────────

def build_dim_cuisine(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per unique cuisine description.
    Assigns a surrogate integer key cuisine_id.
    """
    cuisines = (
        df["cuisine_description"]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    dim = pd.DataFrame({
        "cuisine_id": range(1, len(cuisines) + 1),
        "cuisine_description": cuisines.values,
    })
    logging.info(
        "dim_cuisine: %s distinct cuisine types", f"{len(dim):,}"
    )
    return dim


# ── dim_date ───────────────────────────────────────────────────────────────

def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per unique inspection date with derived calendar attributes.
    Rows with NaT inspection_date (placeholder 1900-01-01 records) are excluded.
    """
    dates = (
        df["inspection_date"]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    dim = pd.DataFrame({
        "inspection_date": dates,
        "year": dates.dt.year,
        "month": dates.dt.month,
        "month_name": dates.dt.strftime("%B"),
        "quarter": dates.dt.quarter,
        "day_of_week": dates.dt.strftime("%A"),
    })
    logging.info(
        "dim_date: %s distinct inspection dates (range: %s to %s)",
        f"{len(dim):,}",
        dim["inspection_date"].min().date(),
        dim["inspection_date"].max().date(),
    )
    return dim


# ── fact_inspections ───────────────────────────────────────────────────────

def build_fact_inspections(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per inspection visit (camis + inspection_date + inspection_type).

    Aggregation from Silver violation-level rows:
    - violation_count: total violation rows per visit
    - critical_violation_count: rows where critical_flag = 'Critical'
    - has_violation: True if any violation_code is not null
    - score, grade, grade_date, action: taken from the first row per visit
      (these fields are constant across all violation rows for the same visit)
    """
    # Filter out rows with no inspection date (pre-inspection establishments)
    df_inspected = df.dropna(subset=["inspection_date"]).copy()
    excluded = len(df) - len(df_inspected)
    logging.info(
        "fact_inspections: excluded %s rows with null inspection_date",
        f"{excluded:,}",
    )

    # Aggregation
    agg = df_inspected.groupby(INSPECTION_KEYS, as_index=False).agg(
        action=("action", "first"),
        score=("score", "first"),
        grade=("grade", "first"),
        grade_date=("grade_date", "first"),
        violation_count=("violation_code", "count"),
        critical_violation_count=(
            "critical_flag",
            lambda x: (x == "Critical").sum(),
        ),
        has_violation=(
            "violation_code",
            lambda x: x.notna().any(),
        ),
    )

    logging.info(
        "fact_inspections: %s inspection visits from %s violation rows",
        f"{len(agg):,}",
        f"{len(df_inspected):,}",
    )
    return agg


# ── Write Gold tables ──────────────────────────────────────────────────────

def write_table(df: pd.DataFrame, path: Path, name: str) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")
    size_mb = path.stat().st_size / (1024 * 1024)
    logging.info(
        "Wrote %s to %s (%s rows, %.1f MB)",
        name, path, f"{len(df):,}", size_mb,
    )


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> int:
    setup_logging()
    logging.info("Starting Gold build")

    try:
        df = load_silver()
    except FileNotFoundError as exc:
        logging.error("%s", exc)
        return 1

    dim_restaurant = build_dim_restaurant(df)
    dim_cuisine = build_dim_cuisine(df)
    dim_date = build_dim_date(df)
    fact = build_fact_inspections(df)

    write_table(dim_restaurant, DIM_RESTAURANT_PATH, "dim_restaurant")
    write_table(dim_cuisine, DIM_CUISINE_PATH, "dim_cuisine")
    write_table(dim_date, DIM_DATE_PATH, "dim_date")
    write_table(fact, FACT_PATH, "fact_inspections")

    print(f"dim_restaurant rows : {len(dim_restaurant):,}")
    print(f"dim_cuisine rows    : {len(dim_cuisine):,}")
    print(f"dim_date rows       : {len(dim_date):,}")
    print(f"fact_inspections rows: {len(fact):,}")

    logging.info("Gold build finished successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
