#!/usr/bin/env python3

import argparse
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


def plot_results(results: pd.DataFrame, output: Path) -> None:
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
            position = query_index * 3 + family_index + 1
            positions.append(position)
            labels.append(family)
            box_data.append(values or [float("nan")])

    boxplot = axis.boxplot(
        box_data,
        positions=positions,
        widths=0.65,
        patch_artist=True,
        showfliers=False,
        medianprops={"color": "#202020", "linewidth": 1.5},
        whiskerprops={"color": "#505050"},
        capprops={"color": "#505050"},
    )
    for index, box in enumerate(boxplot["boxes"]):
        box.set_facecolor(family_colors[families[index % 2]])
        box.set_alpha(0.8)

    for query_index, query_type in enumerate(query_types):
        group_normal = normal[normal["query_type"] == query_type]
        group_abnormal = abnormal[abnormal["query_type"] == query_type]
        top = group_normal[TIME_COLUMN].max() if not group_normal.empty else 1
        top *= 1.12
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
    axis.set_xticklabels(query_types)
    axis.set_xlabel("Query type")
    axis.set_ylabel("Execution time (seconds)")
    axis.set_title("Skitter robustness test: execution-time variance")
    axis.grid(axis="y", linestyle="--", alpha=0.3)
    axis.legend(
        [plt.Line2D([0], [0], color=color, linewidth=8) for color in family_colors.values()],
        families,
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
    args = parser.parse_args()
    plot_results(read_results(args.input), args.output)


if __name__ == "__main__":
    main()