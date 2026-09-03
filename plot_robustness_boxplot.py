#!/usr/bin/env python3

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


STATUS_COLUMN = "status"
TIME_COLUMN = "execution_time"


def read_results(path: Path) -> pd.DataFrame:
    results = pd.read_csv(path, sep="\t")
    required = {"family", "query", STATUS_COLUMN, TIME_COLUMN}
    missing = required - set(results.columns)
    if missing:
        raise ValueError(f"Missing columns in {path}: {', '.join(sorted(missing))}")

    results["query_type"] = results["query"].str.split("/").str[0]
    results[TIME_COLUMN] = pd.to_numeric(results[TIME_COLUMN], errors="raise")
    return results


def read_baselines(path: Path, dataset: str) -> dict[str, tuple[str, float]]:
    if not path.exists():
        print(f"Baseline file not found: {path}; using plan medians")
        return {}

    baselines = pd.read_csv(path, sep="\t")
    required = {"dataset", "query", STATUS_COLUMN, TIME_COLUMN}
    missing = required - set(baselines.columns)
    if missing:
        raise ValueError(f"Missing columns in {path}: {', '.join(sorted(missing))}")

    baselines[TIME_COLUMN] = pd.to_numeric(baselines[TIME_COLUMN], errors="raise")
    baselines["query_type"] = baselines["query"].str.replace(
        r"\.sql$", "", regex=True
    )
    selected = baselines[baselines["dataset"] == dataset]
    return {
        row["query_type"]: (row[STATUS_COLUMN], row[TIME_COLUMN])
        for _, row in selected.iterrows()
    }


def calculate_slowdowns(
    results: pd.DataFrame, baselines: dict[str, tuple[str, float]]
) -> tuple[pd.DataFrame, dict[str, float]]:
    normalized = results.copy()
    normal_mask = normalized[STATUS_COLUMN] == "NORMAL"
    reference_times = {}

    for query_type, group in normalized[normal_mask].groupby("query_type"):
        baseline = baselines.get(query_type)
        baseline_time = (
            baseline[1]
            if baseline is not None and baseline[0] == "NORMAL" and baseline[1] > 0
            else None
        )
        if baseline_time is None:
            binary_times = group.loc[group["family"] == "binary", TIME_COLUMN]
            baseline_time = (
                binary_times.median()
                if not binary_times.empty
                else group[TIME_COLUMN].median()
            )
            print(
                f"No usable baseline for {query_type}; "
                f"using binary plan median ({baseline_time:.3f}s)"
            )
        reference_times[query_type] = baseline_time
        normalized.loc[group.index, TIME_COLUMN] /= baseline_time

    return normalized, reference_times


def filter_disconnected_binary_plans(results: pd.DataFrame) -> pd.DataFrame:
    plan_root = Path("robustness-test/binary")
    excluded_queries = []
    for query in results.loc[results["family"] == "binary", "query"].unique():
        plan_path = plan_root / query
        plan_text = plan_path.read_text()
        if re.search(r"\bCROSS\s+JOIN\b", plan_text, flags=re.IGNORECASE):
            excluded_queries.append(query)

    if excluded_queries:
        print("Excluded disconnected binary plans:")
        for query in excluded_queries:
            print(f"  {query}")
    return results[~results["query"].isin(excluded_queries)].copy()


def plot_results(
    results: pd.DataFrame,
    output: Path,
    dataset: str,
    baselines: dict[str, tuple[str, float]],
) -> None:
    results, reference_times = calculate_slowdowns(results, baselines)
    normal = results[results[STATUS_COLUMN] == "NORMAL"]
    abnormal = results[results[STATUS_COLUMN].isin(["MEMORY_OUT", "TIMEOUT"])]
    query_types = list(dict.fromkeys(results["query_type"]))
    families = ["binary", "wcoj"]

    figure, axis = plt.subplots(figsize=(13, 7))
    positions = []
    labels = []
    box_data = []
    family_colors = {"binary": "#3264a8", "wcoj": "#d97941"}

    for query_index, query_type in enumerate(query_types):
        for family_index, family in enumerate(families):
            values = normal[
                (normal["query_type"] == query_type)
                & (normal["family"] == family)
            ][TIME_COLUMN].tolist()
            if values and family == "binary":
                values.append(1.0)
            position = query_index * 3 + family_index + 1
            positions.append(position)
            labels.append(family)
            box_data.append(values or [float("nan")])

    boxplot = axis.boxplot(
        box_data,
        positions=positions,
        widths=0.65,
        patch_artist=True,
        whis=(0, 100),
        showfliers=False,
        medianprops={"color": "#202020", "linewidth": 1.5},
        whiskerprops={"color": "#505050"},
        capprops={"color": "#505050"},
    )
    for index, box in enumerate(boxplot["boxes"]):
        box.set_facecolor(family_colors[families[index % 2]])
        box.set_alpha(0.8)

    top = max(normal[TIME_COLUMN].max(), 1) if not normal.empty else 1
    top *= 1.12
    for query_index, query_type in enumerate(query_types):
        group_abnormal = abnormal[abnormal["query_type"] == query_type]
        for family_index, family in enumerate(families):
            position = query_index * 3 + family_index + 1
            count = len(group_abnormal[group_abnormal["family"] == family])
            if count > 0:
                axis.scatter(position, top, color="#c62828", s=42, zorder=5)
                axis.annotate(
                    str(count),
                    (position, top),
                    xytext=(0, 7),
                    textcoords="offset points",
                    ha="center",
                    color="#c62828",
                    fontsize=9,
                    fontweight="bold",
                )

    axis.set_xticks([query_index * 3 + 1.5 for query_index in range(len(query_types))])
    tick_labels = []
    failed_baseline_ticks = []
    for index, query_type in enumerate(query_types):
        baseline = baselines.get(query_type)
        if baseline is not None and baseline[0] in {"TIMEOUT", "MEMORY_OUT"}:
            tick_labels.append(
                f"{query_type}\nmedian, {reference_times[query_type]:.1f}s"
            )
            failed_baseline_ticks.append(index)
        elif baseline is not None:
            tick_labels.append(f"{query_type}\n{baseline[1]:.1f}s")
        else:
            tick_labels.append(
                f"{query_type}\nmedian, {reference_times[query_type]:.1f}s"
            )
    axis.set_xticklabels(tick_labels)
    for index in failed_baseline_ticks:
        axis.get_xticklabels()[index].set_color("#c62828")
        axis.get_xticklabels()[index].set_fontweight("bold")
    axis.set_xlabel("Query type")
    axis.set_yscale("log", base=2)
    axis.set_ylabel("Slowdown relative to baseline (×, log₂ scale)")
    axis.set_title(f"{dataset} robustness test: slowdown relative to baseline")
    axis.grid(axis="y", linestyle="--", alpha=0.3)
    axis.axhline(1, color="#333333", linestyle=":", linewidth=1.5, zorder=1)
    axis.legend(
        [
            *[
                plt.Line2D([0], [0], color=color, linewidth=8)
                for color in family_colors.values()
            ],
            plt.Line2D([0], [0], color="#333333", linestyle=":", linewidth=1.5),
        ],
        [*families, "baseline (1×)"],
        title="Plan",
    )
    figure.text(
        0.99,
        0.01,
        "Red point + number = MEMORY_OUT + TIMEOUT count; excluded from boxplot",
        ha="right",
        fontsize=9,
        color="#c62828",
    )
    figure.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=180, bbox_inches="tight")
    print(f"Saved plot to {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot robustness execution-time boxplots.")
    parser.add_argument(
        "input",
        nargs="?",
        default="robustness-test/results-skitters-15min.tsv",
        type=Path,
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default="robustness-test/results-skitters-15min-boxplot.png",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("robustness-test/baseline.tsv"),
        help="Baseline TSV produced by run_robustness_test.sh -b",
    )
    parser.add_argument(
        "--filtered",
        action="store_true",
        help="Exclude binary plans containing a disconnected CROSS JOIN",
    )
    args = parser.parse_args()
    results = read_results(args.input)
    if args.filtered:
        results = filter_disconnected_binary_plans(results)
    dataset = args.input.stem.removeprefix("results-").removesuffix("-15min")
    baselines = read_baselines(args.baseline, dataset)
    plot_results(results, args.output, dataset, baselines)


if __name__ == "__main__":
    main()
