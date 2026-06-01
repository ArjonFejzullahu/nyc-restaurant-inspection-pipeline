"""
02_profile.py — Profile raw data

Summarize the Bronze CSV (row counts, dtypes, nulls, value distributions)
and write profile reports under outputs/profile/.
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_CSV = PROJECT_ROOT / "data" / "bronze" / "restaurant_inspections_raw.csv"
PROFILE_DIR = PROJECT_ROOT / "outputs" / "profile"
SUMMARY_PATH = PROFILE_DIR / "profile_summary.txt"
METRICS_CSV_PATH = PROFILE_DIR / "profile_metrics.csv"

METRIC_DEFINITIONS: tuple[tuple[str, str], ...] = (
    ("row_count", "Total number of rows in the Bronze dataset"),
    ("column_count", "Total number of columns in the Bronze dataset"),
    ("null_score_count", "Rows with null score"),
    ("null_grade_count", "Rows with null grade"),
    ("null_grade_date_count", "Rows with null grade_date"),
    ("null_violation_code_count", "Rows with null violation_code"),
    ("null_violation_description_count", "Rows with null violation_description"),
    ("null_cuisine_description_count", "Rows with null cuisine_description"),
    ("null_zipcode_count", "Rows with null zipcode"),
    ("placeholder_inspection_date_count", "Rows with placeholder inspection_date (1900-01-01)"),
    ("invalid_boro_zero_count", "Rows where boro is 0 (string or numeric)"),
    ("distinct_cuisine_count", "Distinct non-null cuisine_description values"),
    ("duplicate_row_count", "Fully duplicate rows (pandas duplicated())"),
)

PLACEHOLDER_INSPECTION_ISO = re.compile(r"^1900-01-01")
PLACEHOLDER_INSPECTION_SLASH = re.compile(r"^0?1/0?1/1900")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_bronze_dataset(path: Path = BRONZE_CSV) -> pd.DataFrame:
    """Load the Bronze restaurant inspections CSV."""
    if not path.is_file():
        raise FileNotFoundError(
            f"Bronze CSV not found at {path}. Run scripts/01_ingest.py first."
        )

    logging.info("Loading Bronze dataset from %s", path)
    df = pd.read_csv(path, low_memory=False)
    logging.info("Loaded %s rows and %s columns", f"{len(df):,}", len(df.columns))
    return df


def _null_count(series: pd.Series) -> int:
    return int(series.isna().sum())


def count_placeholder_inspection_dates(series: pd.Series) -> int:
    """Count rows where inspection_date is 1900-01-01 or 1/1/1900 (string-safe)."""
    as_string = series.astype(str).str.strip()
    placeholder_mask = as_string.str.match(
        PLACEHOLDER_INSPECTION_ISO, na=False
    ) | as_string.str.match(PLACEHOLDER_INSPECTION_SLASH, na=False)
    return int(placeholder_mask.sum())


def count_invalid_boro_zero(series: pd.Series) -> int:
    """Count rows where boro is the string '0' or numeric 0."""
    as_string = series.astype(str).str.strip()
    string_zero = as_string == "0"
    numeric = pd.to_numeric(series, errors="coerce")
    numeric_zero = numeric == 0
    return int((string_zero | numeric_zero).sum())


def calculate_metrics(df: pd.DataFrame) -> dict[str, Any]:
    """Calculate profiling metrics for the Bronze dataset."""
    metrics: dict[str, Any] = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "null_score_count": _null_count(df["score"]),
        "null_grade_count": _null_count(df["grade"]),
        "null_grade_date_count": _null_count(df["grade_date"]),
        "null_violation_code_count": _null_count(df["violation_code"]),
        "null_violation_description_count": _null_count(df["violation_description"]),
        "null_cuisine_description_count": _null_count(df["cuisine_description"]),
        "null_zipcode_count": _null_count(df["zipcode"]),
        "placeholder_inspection_date_count": count_placeholder_inspection_dates(
            df["inspection_date"]
        ),
        "invalid_boro_zero_count": count_invalid_boro_zero(df["boro"]),
        "distinct_cuisine_count": int(df["cuisine_description"].nunique(dropna=True)),
        "duplicate_row_count": int(df.duplicated().sum()),
    }

    for name, value in metrics.items():
        logging.info("Computed %s: %s", name, f"{value:,}" if isinstance(value, int) else value)

    return metrics


def _format_metric_value(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def build_metrics_dataframe(metrics: dict[str, Any]) -> pd.DataFrame:
    """Build a tabular report of all metrics."""
    rows = []
    for metric_name, description in METRIC_DEFINITIONS:
        value = metrics[metric_name]
        rows.append(
            {
                "metric": metric_name,
                "value": _format_metric_value(value),
                "status": "computed",
                "description": description,
            }
        )

    return pd.DataFrame(rows)


def build_summary_text(metrics: dict[str, Any], source_path: Path) -> str:
    """Build a human-readable profile summary."""
    lines = [
        "NYC Restaurant Inspection Pipeline — Bronze Profile Summary",
        "=" * 60,
        f"Source: {source_path}",
        "",
        "Metrics",
        "-" * 40,
    ]

    for metric_name, _description in METRIC_DEFINITIONS:
        value = metrics[metric_name]
        if isinstance(value, int):
            lines.append(f"{metric_name}: {value:,}")
        else:
            lines.append(f"{metric_name}: {value}")

    lines.extend(["", "End of summary"])
    return "\n".join(lines) + "\n"


def write_outputs(
    metrics: dict[str, Any],
    source_path: Path = BRONZE_CSV,
) -> tuple[Path, Path]:
    """Write profile summary and metrics CSV under outputs/profile/."""
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    logging.info("Ensured profile output directory exists: %s", PROFILE_DIR)

    summary_text = build_summary_text(metrics, source_path)
    SUMMARY_PATH.write_text(summary_text, encoding="utf-8")
    logging.info("Wrote profile summary to %s", SUMMARY_PATH)

    metrics_df = build_metrics_dataframe(metrics)
    metrics_df.to_csv(METRICS_CSV_PATH, index=False)
    logging.info("Wrote profile metrics to %s", METRICS_CSV_PATH)

    return SUMMARY_PATH, METRICS_CSV_PATH


def main() -> int:
    setup_logging()
    logging.info("Starting Bronze profiling")

    try:
        df = load_bronze_dataset()
    except FileNotFoundError as exc:
        logging.error("%s", exc)
        return 1

    metrics = calculate_metrics(df)

    try:
        summary_path, metrics_path = write_outputs(metrics)
    except OSError as exc:
        logging.error("Failed to write profile outputs: %s", exc)
        return 1

    print(f"Rows profiled: {metrics['row_count']:,}")
    print(f"Columns profiled: {metrics['column_count']}")
    print(f"Summary: {summary_path}")
    print(f"Metrics: {metrics_path}")

    logging.info("Bronze profiling finished successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
