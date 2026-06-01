"""
run_pipeline.py — End-to-end pipeline runner

Executes all five pipeline stages in order on the full NYC restaurant
inspection dataset:

    Ingest  → 01_ingest.py         (Bronze: raw CSV from Socrata API)
    Profile → 02_profile.py        (Bronze profiling report)
    Silver  → 03_process_silver.py (Silver: cleaned Parquet)
    Gold    → 04_build_gold.py     (Gold: star schema fact + dimensions)
    Expose  → 05_expose.py         (Outputs: CSV tables + PNG charts)

Usage
-----
    python3 scripts/run_pipeline.py

A non-zero exit code from any stage aborts the run immediately.
All stage output is logged to stdout with timestamps.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

STAGES = [
    ("Ingest",   "01_ingest.py"),
    ("Profile",  "02_profile.py"),
    ("Silver",   "03_process_silver.py"),
    ("Gold",     "04_build_gold.py"),
    ("Expose",   "05_expose.py"),
]


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_stage(name: str, script_file: str) -> None:
    """
    Dynamically import and execute a pipeline stage script.
    Calls the script's main() function and raises RuntimeError on failure.
    """
    script_path = SCRIPTS_DIR / script_file

    if not script_path.is_file():
        raise FileNotFoundError(f"Stage script not found: {script_path}")

    logging.info("━━━ Starting stage: %s (%s) ━━━", name, script_file)
    stage_start = time.time()

    spec = importlib.util.spec_from_file_location(name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "main"):
        raise AttributeError(
            f"Stage script {script_file} has no main() function"
        )

    exit_code = module.main()

    elapsed = time.time() - stage_start
    if exit_code != 0:
        raise RuntimeError(
            f"Stage '{name}' failed with exit code {exit_code} "
            f"after {elapsed:.1f}s"
        )

    logging.info(
        "━━━ Completed stage: %s in %.1fs ━━━", name, elapsed
    )


def main() -> int:
    setup_logging()
    pipeline_start = time.time()

    logging.info(
        "╔══════════════════════════════════════════╗"
    )
    logging.info(
        "║   NYC Restaurant Inspection Pipeline     ║"
    )
    logging.info(
        "║   Running all %s stages end-to-end        ║", len(STAGES)
    )
    logging.info(
        "╚══════════════════════════════════════════╝"
    )

    for stage_name, script_file in STAGES:
        try:
            run_stage(stage_name, script_file)
        except (FileNotFoundError, AttributeError, RuntimeError) as exc:
            logging.error("Pipeline aborted at stage '%s': %s", stage_name, exc)
            return 1

    total_elapsed = time.time() - pipeline_start
    logging.info(
        "Pipeline completed all %s stages in %.1fs",
        len(STAGES),
        total_elapsed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
