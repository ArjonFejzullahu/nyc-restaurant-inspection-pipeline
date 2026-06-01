"""
05_expose.py — Expose (analytics outputs)

Query the Gold layer fact and dimension tables and produce:

CSV tables (outputs/tables/)
-----------------------------
  borough_summary.csv   — average score, inspection count, grade distribution
                          per borough
  cuisine_summary.csv   — average score, inspection count, critical violation
                          rate per cuisine type (top 20 worst by avg score)
  worst_restaurants.csv — top 20 restaurants by average inspection score
                          (higher score = worse), with name, borough, cuisine,
                          and inspection count

PNG charts (outputs/charts/)
-----------------------------
  avg_score_by_borough.png — bar chart: average inspection score per borough
  cuisine_risk.png         — horizontal bar chart: top 15 cuisines by avg score
  inspection_trend.png     — line chart: monthly average score over time,
                             showing whether food safety is improving or
                             worsening across NYC
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import seaborn as sns

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

FACT_PATH = GOLD_DIR / "fact_inspections.parquet"
DIM_RESTAURANT_PATH = GOLD_DIR / "dim_restaurant.parquet"
DIM_CUISINE_PATH = GOLD_DIR / "dim_cuisine.parquet"

TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
CHARTS_DIR = PROJECT_ROOT / "outputs" / "charts"

BOROUGH_ORDER = ["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"]
CHART_STYLE = "whitegrid"
PALETTE = "Blues_d"
FIGURE_DPI = 150


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ── Load Gold tables ───────────────────────────────────────────────────────

def load_gold() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    for path in (FACT_PATH, DIM_RESTAURANT_PATH, DIM_CUISINE_PATH):
        if not path.is_file():
            raise FileNotFoundError(
                f"Gold table not found at {path}. Run 04_build_gold.py first."
            )

    logging.info("Loading Gold tables")
    fact = pd.read_parquet(FACT_PATH, engine="pyarrow")
    dim_restaurant = pd.read_parquet(DIM_RESTAURANT_PATH, engine="pyarrow")
    dim_cuisine = pd.read_parquet(DIM_CUISINE_PATH, engine="pyarrow")

    logging.info(
        "Loaded fact_inspections (%s rows), dim_restaurant (%s rows), "
        "dim_cuisine (%s rows)",
        f"{len(fact):,}",
        f"{len(dim_restaurant):,}",
        f"{len(dim_cuisine):,}",
    )
    return fact, dim_restaurant, dim_cuisine


def join_fact_restaurant(
    fact: pd.DataFrame, dim_restaurant: pd.DataFrame
) -> pd.DataFrame:
    """Join fact table with restaurant dimension on camis."""
    merged = fact.merge(
        dim_restaurant[["camis", "dba", "boro", "cuisine_description"]],
        on="camis",
        how="left",
    )
    logging.info(
        "Joined fact with dim_restaurant: %s rows", f"{len(merged):,}"
    )
    return merged


# ── Borough summary ────────────────────────────────────────────────────────

def build_borough_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average score, total inspections, and grade distribution per borough.
    Excludes UNKNOWN borough and rows with null score.
    """
    df_valid = df[df["boro"].isin(BOROUGH_ORDER) & df["score"].notna()].copy()

    summary = (
        df_valid.groupby("boro")
        .agg(
            avg_score=("score", "mean"),
            total_inspections=("camis", "count"),
            grade_A_count=("grade", lambda x: (x == "A").sum()),
            grade_B_count=("grade", lambda x: (x == "B").sum()),
            grade_C_count=("grade", lambda x: (x == "C").sum()),
        )
        .round({"avg_score": 2})
        .reset_index()
    )

    summary["grade_A_pct"] = (
        summary["grade_A_count"] / summary["total_inspections"] * 100
    ).round(1)

    summary = summary.set_index("boro").loc[
        [b for b in BOROUGH_ORDER if b in summary["boro"].values]
    ].reset_index()

    logging.info("borough_summary: %s rows", len(summary))
    return summary


# ── Cuisine summary ────────────────────────────────────────────────────────

def build_cuisine_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Top 20 worst cuisine types by average inspection score.
    Only includes cuisines with at least 50 inspections.
    """
    df_valid = df[df["cuisine_description"].notna() & df["score"].notna()].copy()

    summary = (
        df_valid.groupby("cuisine_description")
        .agg(
            avg_score=("score", "mean"),
            total_inspections=("camis", "count"),
            critical_violation_count=("critical_violation_count", "sum"),
        )
        .reset_index()
    )

    summary["critical_rate_pct"] = (
        summary["critical_violation_count"] / summary["total_inspections"] * 100
    ).round(1)

    summary = (
        summary[summary["total_inspections"] >= 50]
        .sort_values("avg_score", ascending=False)
        .head(20)
        .round({"avg_score": 2})
        .reset_index(drop=True)
    )

    logging.info("cuisine_summary: %s rows", len(summary))
    return summary


# ── Worst restaurants ──────────────────────────────────────────────────────

def build_worst_restaurants(df: pd.DataFrame) -> pd.DataFrame:
    """
    Top 20 restaurants by average inspection score (higher = worse).
    Only includes restaurants with at least 3 inspections.
    """
    df_valid = df[df["score"].notna()].copy()

    summary = (
        df_valid.groupby(["camis", "dba", "boro", "cuisine_description"])
        .agg(
            avg_score=("score", "mean"),
            inspection_count=("camis", "count"),
            max_score=("score", "max"),
        )
        .reset_index()
    )

    worst = (
        summary[summary["inspection_count"] >= 3]
        .sort_values("avg_score", ascending=False)
        .head(20)
        .round({"avg_score": 2})
        .reset_index(drop=True)
    )

    logging.info("worst_restaurants: %s rows", len(worst))
    return worst


# ── Inspection trend ───────────────────────────────────────────────────────

def build_monthly_trend(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Monthly average inspection score over time.
    Used for the trend chart.
    """
    df_valid = fact[fact["score"].notna() & fact["inspection_date"].notna()].copy()
    df_valid["year_month"] = df_valid["inspection_date"].dt.to_period("M")

    trend = (
        df_valid.groupby("year_month")
        .agg(
            avg_score=("score", "mean"),
            inspection_count=("camis", "count"),
        )
        .reset_index()
        .sort_values("year_month")
    )

    # Only keep months with at least 10 inspections to avoid noise
    trend = trend[trend["inspection_count"] >= 10].copy()
    trend["year_month_dt"] = trend["year_month"].dt.to_timestamp()

    logging.info(
        "monthly_trend: %s months (range: %s to %s)",
        len(trend),
        trend["year_month"].min(),
        trend["year_month"].max(),
    )
    return trend


# ── Charts ─────────────────────────────────────────────────────────────────

def chart_avg_score_by_borough(borough_summary: pd.DataFrame) -> Path:
    output_path = CHARTS_DIR / "avg_score_by_borough.png"
    sns.set_style(CHART_STYLE)

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(
        data=borough_summary,
        x="boro",
        y="avg_score",
        hue="boro",
        palette=PALETTE,
        legend=False,
        ax=ax,
    )

    for bar in ax.patches:
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.2,
            f"{bar.get_height():.1f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax.set_title(
        "Average Inspection Score by Borough\n(higher score = more violations)",
        fontsize=12,
        fontweight="bold",
    )
    ax.set_xlabel("Borough", fontsize=10)
    ax.set_ylabel("Average Inspection Score", fontsize=10)
    ax.set_ylim(0, borough_summary["avg_score"].max() * 1.2)
    plt.tight_layout()
    fig.savefig(output_path, dpi=FIGURE_DPI)
    plt.close(fig)
    logging.info("Saved chart: %s", output_path)
    return output_path


def chart_cuisine_risk(cuisine_summary: pd.DataFrame) -> Path:
    output_path = CHARTS_DIR / "cuisine_risk.png"
    sns.set_style(CHART_STYLE)

    top15 = cuisine_summary.head(15).sort_values("avg_score", ascending=True)

    fig, ax = plt.subplots(figsize=(9, 7))
    sns.barplot(
        data=top15,
        x="avg_score",
        y="cuisine_description",
        hue="cuisine_description",
        palette=PALETTE,
        legend=False,
        ax=ax,
    )

    for bar in ax.patches:
        ax.text(
            bar.get_width() + 0.1,
            bar.get_y() + bar.get_height() / 2,
            f"{bar.get_width():.1f}",
            ha="left",
            va="center",
            fontsize=8,
        )

    ax.set_title(
        "Top 15 Cuisine Types by Average Inspection Score\n"
        "(higher score = more violations, min. 50 inspections)",
        fontsize=12,
        fontweight="bold",
    )
    ax.set_xlabel("Average Inspection Score", fontsize=10)
    ax.set_ylabel("Cuisine Type", fontsize=10)
    plt.tight_layout()
    fig.savefig(output_path, dpi=FIGURE_DPI)
    plt.close(fig)
    logging.info("Saved chart: %s", output_path)
    return output_path


def chart_inspection_trend(trend: pd.DataFrame) -> Path:
    output_path = CHARTS_DIR / "inspection_trend.png"
    sns.set_style(CHART_STYLE)

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(
        trend["year_month_dt"],
        trend["avg_score"],
        color="#2c7bb6",
        linewidth=1.5,
        alpha=0.9,
    )
    ax.fill_between(
        trend["year_month_dt"],
        trend["avg_score"],
        alpha=0.15,
        color="#2c7bb6",
    )

    # Rolling 12-month average overlay
    trend["rolling_avg"] = (
        trend["avg_score"].rolling(window=12, min_periods=6).mean()
    )
    ax.plot(
        trend["year_month_dt"],
        trend["rolling_avg"],
        color="#d7191c",
        linewidth=2,
        linestyle="--",
        label="12-month rolling average",
    )

    ax.set_title(
        "NYC Restaurant Inspection Score Trend Over Time\n"
        "(lower score = better food safety — are things improving?)",
        fontsize=12,
        fontweight="bold",
    )
    ax.set_xlabel("Year", fontsize=10)
    ax.set_ylabel("Average Inspection Score", fontsize=10)
    ax.legend(fontsize=9)

    import matplotlib.dates as mdates
    # Labels sit at July — visually centred within each year
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=7))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.tick_params(axis="x", which="major", length=0)  # no tick marks at labels
    # Draw vertical grid lines at January (true year boundaries)
    ax.set_xticks(
        [
            pd.Timestamp(f"{y}-01-01")
            for y in range(
                trend["year_month_dt"].min().year,
                trend["year_month_dt"].max().year + 2,
            )
        ],
        minor=True,
    )
    ax.xaxis.set_minor_formatter(mticker.NullFormatter())
    ax.grid(axis="x", which="minor", color="gray", linewidth=0.5, alpha=0.5)
    ax.grid(axis="x", which="major", visible=False)
    ax.grid(axis="y", which="major", color="gray", linewidth=0.5, alpha=0.5)
    plt.tight_layout()
    fig.savefig(output_path, dpi=FIGURE_DPI)
    plt.close(fig)
    logging.info("Saved chart: %s", output_path)
    return output_path


# ── Write CSV tables ───────────────────────────────────────────────────────

def write_tables(
    borough_summary: pd.DataFrame,
    cuisine_summary: pd.DataFrame,
    worst_restaurants: pd.DataFrame,
) -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    borough_path = TABLES_DIR / "borough_summary.csv"
    cuisine_path = TABLES_DIR / "cuisine_summary.csv"
    worst_path = TABLES_DIR / "worst_restaurants.csv"

    borough_summary.to_csv(borough_path, index=False)
    cuisine_summary.to_csv(cuisine_path, index=False)
    worst_restaurants.to_csv(worst_path, index=False)

    logging.info("Wrote borough_summary to %s", borough_path)
    logging.info("Wrote cuisine_summary to %s", cuisine_path)
    logging.info("Wrote worst_restaurants to %s", worst_path)


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> int:
    setup_logging()
    logging.info("Starting Expose")

    try:
        fact, dim_restaurant, dim_cuisine = load_gold()
    except FileNotFoundError as exc:
        logging.error("%s", exc)
        return 1

    df = join_fact_restaurant(fact, dim_restaurant)

    borough_summary = build_borough_summary(df)
    cuisine_summary = build_cuisine_summary(df)
    worst_restaurants = build_worst_restaurants(df)
    trend = build_monthly_trend(fact)

    write_tables(borough_summary, cuisine_summary, worst_restaurants)

    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    chart_avg_score_by_borough(borough_summary)
    chart_cuisine_risk(cuisine_summary)
    chart_inspection_trend(trend)

    print(f"Tables written to  : {TABLES_DIR}")
    print(f"Charts written to  : {CHARTS_DIR}")
    print(f"Borough rows       : {len(borough_summary)}")
    print(f"Cuisine rows       : {len(cuisine_summary)}")
    print(f"Worst restaurants  : {len(worst_restaurants)}")
    print(f"Trend months       : {len(trend)}")

    logging.info("Expose finished successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
