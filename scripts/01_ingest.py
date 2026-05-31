"""
01_ingest.py — Ingest (Bronze layer)

Download the full NYC DOHMH Restaurant Inspection Results dataset from the
Socrata API and save the raw response as CSV under data/bronze/.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
OUTPUT_CSV = BRONZE_DIR / "restaurant_inspections_raw.csv"
DEFAULT_API_URL = (
    "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
)
REQUEST_TIMEOUT_SECONDS = 120
MAX_PAGE_ATTEMPTS = 4  # 1 initial request + 3 retries
DEFAULT_PAGE_SIZE = 10000


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config() -> tuple[str, str, int]:
    env_path = PROJECT_ROOT / ".env"
    if env_path.is_file():
        load_dotenv(env_path)
        logging.info("Loaded configuration from %s", env_path)
    else:
        logging.warning(
            "No .env file at %s; using defaults and environment variables",
            env_path,
        )
        load_dotenv()

    pipeline_mode = os.getenv("PIPELINE_MODE", "local")
    api_url = os.getenv("SOCRATA_API_URL", DEFAULT_API_URL)
    limit_raw = os.getenv("SOCRATA_LIMIT", str(DEFAULT_PAGE_SIZE))

    try:
        page_size = int(limit_raw)
    except ValueError as exc:
        raise ValueError(
            f"SOCRATA_LIMIT must be an integer, got: {limit_raw!r}"
        ) from exc

    if page_size <= 0:
        raise ValueError("SOCRATA_LIMIT must be a positive integer")

    return pipeline_mode, api_url, page_size


def fetch_page(
    session: requests.Session,
    api_url: str,
    page_size: int,
    offset: int,
) -> list[dict]:
    params = {"$limit": page_size, "$offset": offset}
    response = session.get(
        api_url,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:
        raise ValueError("API response is not valid JSON") from exc

    if not isinstance(payload, list):
        raise ValueError(
            f"Expected a JSON array from Socrata, got {type(payload).__name__}"
        )

    return payload


def fetch_page_with_retries(
    session: requests.Session,
    api_url: str,
    page_size: int,
    offset: int,
) -> list[dict]:
    """Fetch one Socrata page, retrying transient network and HTTP failures."""
    last_error: BaseException | None = None

    for attempt in range(1, MAX_PAGE_ATTEMPTS + 1):
        try:
            if attempt > 1:
                logging.info(
                    "Retrying offset %s (attempt %s/%s)",
                    offset,
                    attempt,
                    MAX_PAGE_ATTEMPTS,
                )
            return fetch_page(session, api_url, page_size, offset)
        except requests.exceptions.HTTPError as exc:
            last_error = exc
            status = (
                exc.response.status_code
                if exc.response is not None
                else "unknown"
            )
            detail = f"HTTP {status}"
        except requests.exceptions.Timeout as exc:
            last_error = exc
            detail = f"timeout after {REQUEST_TIMEOUT_SECONDS}s"
        except requests.exceptions.ChunkedEncodingError as exc:
            last_error = exc
            detail = "chunked encoding / incomplete read"
        except requests.exceptions.RequestException as exc:
            last_error = exc
            detail = str(exc)

        if attempt >= MAX_PAGE_ATTEMPTS:
            break

        backoff_seconds = 2**attempt
        logging.warning(
            "Request failed at offset %s on attempt %s/%s (%s). "
            "Retrying in %s seconds.",
            offset,
            attempt,
            MAX_PAGE_ATTEMPTS,
            detail,
            backoff_seconds,
        )
        time.sleep(backoff_seconds)

    logging.error(
        "All %s attempts failed at offset %s",
        MAX_PAGE_ATTEMPTS,
        offset,
    )
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Failed to fetch offset {offset} with no captured error")


def download_all_records(api_url: str, page_size: int) -> list[dict]:
    all_records: list[dict] = []
    offset = 0
    session = requests.Session()

    while True:
        range_end = offset + page_size - 1
        logging.info(
            "Requesting records with offset=%s (rows %s–%s)",
            offset,
            offset,
            range_end,
        )

        batch = fetch_page_with_retries(session, api_url, page_size, offset)

        if not batch:
            logging.info("Empty batch received; pagination complete")
            break

        all_records.extend(batch)
        logging.info(
            "Retrieved %s records in this batch (%s total so far)",
            len(batch),
            len(all_records),
        )

        if len(batch) < page_size:
            logging.info("Last batch smaller than page size; download complete")
            break

        offset += page_size

    return all_records


def save_bronze_csv(df: pd.DataFrame) -> Path:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    logging.info("Wrote Bronze CSV to %s", OUTPUT_CSV)
    return OUTPUT_CSV


def main() -> int:
    setup_logging()
    logging.info("Starting Bronze ingestion")

    try:
        pipeline_mode, api_url, page_size = load_config()
    except ValueError as exc:
        logging.error("Invalid configuration: %s", exc)
        return 1

    logging.info("PIPELINE_MODE=%s", pipeline_mode)
    logging.info("SOCRATA_API_URL=%s", api_url)
    logging.info("SOCRATA_LIMIT=%s", page_size)

    if pipeline_mode.lower() != "local":
        logging.warning(
            "PIPELINE_MODE is %r; Azure upload is not implemented. "
            "Saving Bronze data locally only.",
            pipeline_mode,
        )

    try:
        records = download_all_records(api_url, page_size)
    except (requests.exceptions.RequestException, ValueError) as exc:
        logging.error("Ingestion failed: %s", exc)
        return 1

    if not records:
        logging.error("No records downloaded from the API")
        return 1

    logging.info("Building pandas DataFrame from %s records", len(records))
    df = pd.DataFrame(records)

    output_path = save_bronze_csv(df)

    print(f"Rows downloaded: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Output path: {output_path}")

    logging.info("Bronze ingestion finished successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
