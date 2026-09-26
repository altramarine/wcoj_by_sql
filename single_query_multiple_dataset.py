#!/usr/bin/env python3

"""Plot one query's robustness results across multiple datasets."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from plot_robustness_boxplot import (
    STATUS_COLUMN,
    TIME_COLUMN,
    calculate_slowdowns,
    filter_disconnected_binary_plans,
    read_baselines,
    read_results,
)


FAMILIES = ("binary", "wcoj")
FAMILY_COLORS = {"binary": "#3264a8", "wcoj": "#d97941"}


def parse_mapping(values: list[str], argument: str) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    for value in values:
        dataset, separator, path = value.partition("=")
        if not separator or not dataset or not path:
            raise ValueError(f"{argument} must use DATASET=PATH: {value}")
        if dataset in mapping:
            raise ValueError(f"duplicate dataset in {argument}: {dataset}")
        mapping[dataset] = Path(path)
    return mapping


def load_results(result_paths: dict[str, Path], query: str) -> pd.DataFrame:
    frames = []
    for dataset, path in result_paths.items():
        frame = read_results(path)
        frame = frame[frame["query_type"] == query].copy()
        if frame.empty:
            raise ValueError(f"No query type {query!r} found for {dataset} in {path}")
        frame["dataset"] = dataset
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def normalize_results(
    results: pd.DataFrame,
    datasets: list[str],
    baseline_paths: dict[str, Path],
    query: str,
) -> tuple[
    pd.DataFrame,
    dict[str, dict[str, tuple[str, float]]],
    dict[str, float],
]:
    normalized_frames = []
    baselines_by_dataset = {}
    reference_times = {}

    for dataset in datasets:
        baseline_path = baseline_paths.get(dataset)
        baselines = (
            read_baselines(baseline_path, dataset)
            if baseline_path is not None
            else {}
        )
        normalized, references = calculate_slowdowns(
            results[results["dataset"] == dataset].copy(), baselines
        )
        normalized_frames.append(normalized)
        baselines_by_dataset[dataset] = baselines
        reference_times[dataset] = references[query]

    return (
        pd.concat(normalized_frames, ignore_index=True),
        baselines_by_dataset,
        reference_times,
    )


def plot_comparison(
    results: pd.DataFrame,
    datasets: list[str],
    query: str,
    baselines_by_dataset: dict[str, dict[str, tuple[str, float]]],
    reference_times: dict[str, float],
    output: Path,
) -> None:
    normal = results[results[STATUS_COLUMN] == "NORMAL"]
    abnormal = results[results[STATUS_COLUMN].isin(["MEMORY_OUT", "TIMEOUT"])]

    figure, axis = plt.subplots(figsize=(4.5 * len(datasets) + 4, 7))
    positions = []
    box_data = []

    for dataset_index, dataset in enumerate(datasets):
        for family_index, family in enumerate(FAMILIES):
            values = normal[
                (normal["dataset"] == dataset) & (normal["family"] == family)
            ][TIME_COLUMN].tolist()
            if values and family == "binary":
                values.append(1.0)
            positions.append(dataset_index * 3 + family_index + 1)
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
        box.set_facecolor(FAMILY_COLORS[FAMILIES[index % len(FAMILIES)]])
        box.set_alpha(0.8)

    top = max(normal[TIME_COLUMN].max(), 1.0) if not normal.empty else 1.0
    top *= 1.12
    for dataset_index, dataset in enumerate(datasets):
        for family_index, family in enumerate(FAMILIES):
            count = len(
                abnormal[
                    (abnormal["dataset"] == dataset)
                    & (abnormal["family"] == family)
                ]
            )
            if count:
                position = dataset_index * 3 + family_index + 1
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

    axis.set_xticks(
        [dataset_index * 3 + 1.5 for dataset_index in range(len(datasets))]
    )
    tick_labels = []
    failed_baseline_ticks = []
    for index, dataset in enumerate(datasets):
        baseline = baselines_by_dataset[dataset].get(query)
        if baseline is not None and baseline[0] == "NORMAL":
            tick_labels.append(f"{dataset}\n{baseline[1]:.1f}s")
        else:
            tick_labels.append(f"{dataset}\nmedian, {reference_times[dataset]:.1f}s")
            if baseline is not None and baseline[0] in {"TIMEOUT", "MEMORY_OUT"}:
                failed_baseline_ticks.append(index)
    axis.set_xticklabels(tick_labels)
    for index in failed_baseline_ticks:
        axis.get_xticklabels()[index].set_color("#c62828")
        axis.get_xticklabels()[index].set_fontweight("bold")

    axis.set_xlabel("Dataset")
    axis.set_yscale("log", base=2)
    axis.set_ylabel("Slowdown relative to baseline (×, log₂ scale)")
    axis.set_title(f"{query} robustness across datasets")
    axis.grid(axis="y", linestyle="--", alpha=0.3)
    axis.axhline(1, color="#333333", linestyle=":", linewidth=1.5, zorder=1)
    axis.legend(
        [
            *[
                plt.Line2D([0], [0], color=FAMILY_COLORS[family], linewidth=8)
                for family in FAMILIES
            ],
            plt.Line2D([0], [0], color="#333333", linestyle=":", linewidth=1.5),
        ],
        [*FAMILIES, "baseline (1×)"],
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--result",
        action="append",
        required=True,
        metavar="DATASET=PATH",
        help="Result TSV for a dataset; repeat for each dataset",
    )
    parser.add_argument(
        "--baseline",
        action="append",
        default=[],
        metavar="DATASET=PATH",
        help="Baseline TSV for a dataset; repeat as needed",
    )
    parser.add_argument("--query", required=True, help="Query type, e.g. 5cycle")
    parser.add_argument("-o", "--output", required=True, type=Path)
    parser.add_argument(
        "--filtered",
        action="store_true",
        help="Exclude binary plans containing a disconnected CROSS JOIN",
    )
    args = parser.parse_args()

    result_paths = parse_mapping(args.result, "--result")
    baseline_paths = parse_mapping(args.baseline, "--baseline")
    unknown_baselines = set(baseline_paths) - set(result_paths)
    if unknown_baselines:
        parser.error(
            "baseline supplied without a result for: "
            + ", ".join(sorted(unknown_baselines))
        )

    datasets = list(result_paths)
    results = load_results(result_paths, args.query)
    if args.filtered:
        results = filter_disconnected_binary_plans(results)
    normalized, baselines, reference_times = normalize_results(
        results, datasets, baseline_paths, args.query
    )
    plot_comparison(
        normalized,
        datasets,
        args.query,
        baselines,
        reference_times,
        args.output,
    )


if __name__ == "__main__":
    main()
