"""
02_profile.py — Profile raw data

Summarize the Bronze CSV (row counts, dtypes, nulls, value distributions)
and write profile reports under outputs/profile/.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_CSV = PROJECT_ROOT / "data" / "bronze" / "restaurant_inspections_raw.csv"
PROFILE_DIR = PROJECT_ROOT / "outputs" / "profile"
SUMMARY_PATH = PROFILE_DIR / "profile_summary.txt"
METRICS_CSV_PATH = PROFILE_DIR / "profile_metrics.csv"

# Metric keys in report order (only row_count and column_count are computed for now).
METRIC_DEFINITIONS: tuple[tuple[str, str], ...] = (
    ("row_count", "Total number of rows in the Bronze dataset"),
    ("column_count", "Total number of columns in the Bronze dataset"),
    ("null_values", "Null counts per column"),
    ("duplicate_rows", "Number of fully duplicate rows"),
    ("distinct_cuisine_count", "Distinct values in cuisine_description"),
    ("invalid_borough_values", "Borough values outside valid NYC codes"),
    ("placeholder_inspection_dates", "Inspection dates that are placeholders"),
)


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


def calculate_metrics(df: pd.DataFrame) -> dict[str, Any]:
    """
    Calculate profiling metrics for the Bronze dataset.

    Only row_count and column_count are implemented; other keys are placeholders.
    """
    metrics: dict[str, Any] = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "null_values": None,
        "duplicate_rows": None,
        "distinct_cuisine_count": None,
        "invalid_borough_values": None,
        "placeholder_inspection_dates": None,
    }

    logging.info("Computed row_count: %s", f"{metrics['row_count']:,}")
    logging.info("Computed column_count: %s", metrics["column_count"])

    pending = [name for name, value in metrics.items() if value is None]
    if pending:
        logging.info(
            "Placeholder metrics (not yet implemented): %s",
            ", ".join(pending),
        )

    return metrics


def _metric_status(value: Any) -> str:
    return "computed" if value is not None else "pending"


def _format_metric_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def build_metrics_dataframe(metrics: dict[str, Any]) -> pd.DataFrame:
    """Build a tabular report of all metrics and their implementation status."""
    rows = []
    for metric_name, description in METRIC_DEFINITIONS:
        value = metrics.get(metric_name)
        rows.append(
            {
                "metric": metric_name,
                "value": _format_metric_value(value),
                "status": _metric_status(value),
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
        "Implemented metrics",
        "-" * 40,
        f"row_count:    {metrics['row_count']:,}",
        f"column_count: {metrics['column_count']}",
        "",
        "Planned metrics (not yet implemented)",
        "-" * 40,
    ]

    for metric_name, description in METRIC_DEFINITIONS:
        if metrics.get(metric_name) is not None:
            continue
        lines.append(f"- {metric_name}: {description}")

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
