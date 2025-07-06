#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob
import numpy as np
from matplotlib.ticker import FuncFormatter


def load_data():
    """Load all benchmark result files and combine them into a single DataFrame."""
    all_data = []

    # Look for benchmark result files in the build directory
    build_dir = os.path.join(os.path.dirname(__file__), "build")
    if not os.path.exists(build_dir):
        print(f"Error: Build directory '{build_dir}' not found!")
        return None

    print(f"Looking for benchmark files in: {os.path.abspath(build_dir)}")
    print("\nFiles in build directory:")
    for file in os.listdir(build_dir):
        print(f"  {file}")

    # Look for benchmark result files
    benchmark_files = glob.glob(os.path.join(build_dir, "benchmark_*_results_*.csv"))
    print(f"\nFound benchmark files: {benchmark_files}")

    if not benchmark_files:
        print("\nNo benchmark result files found!")
        print("Expected files should match pattern: benchmark_*_results_*.csv")
        print("Example: benchmark_READ_UNCOMMITTED_results_arrow.csv")
        print("\nPlease run the benchmark first to generate the CSV files.")
        return None

    for file in benchmark_files:
        try:
            # Extract isolation level and format from filename
            filename = os.path.basename(file)
            parts = (
                filename.replace("benchmark_", "")
                .replace("_results_", ",")
                .replace(".csv", "")
                .split(",")
            )
            if len(parts) != 2:
                print(f"Warning: Skipping file {filename} - unexpected filename format")
                continue

            isolation_level = parts[0]
            format_type = parts[1]

            print(f"Processing {filename}:")
            print(f"  Isolation Level: {isolation_level}")
            print(f"  Format: {format_type}")

            df = pd.read_csv(file)
            df["isolation_level"] = isolation_level
            df["format"] = format_type
            all_data.append(df)

            print(f"  Successfully loaded {len(df)} rows")

        except Exception as e:
            print(f"Error processing {file}: {str(e)}")
            continue

    if not all_data:
        print("\nNo valid benchmark data could be loaded!")
        return None

    combined_data = pd.concat(all_data, ignore_index=True)
    print(
        f"\nSuccessfully combined {len(combined_data)} rows from {len(all_data)} files"
    )
    return combined_data


def plot_detailed_metrics(data, plots_dir):
    """Create detailed plots for each thread count showing import time, throughput, and latency metrics."""
    thread_counts = sorted(data["num_threads"].unique())
    num_threads = len(thread_counts)
    fig, axes = plt.subplots(num_threads, 4, figsize=(20, 5 * num_threads))

    for idx, threads in enumerate(thread_counts):
        thread_data = data[data["num_threads"] == threads].sort_values("batch_size")

        # Plot 1: Import Time vs Batch Size
        ax1 = axes[idx, 0]
        for (iso, fmt), group in thread_data.groupby(["isolation_level", "format"]):
            label = f"{iso} ({fmt})"
            ax1.plot(group["batch_size"], group["import_time"], "o-", label=label)
        ax1.set_xscale("log")
        ax1.set_xlabel("Batch Size")
        ax1.set_ylabel("Time (seconds)")
        ax1.set_title(f"Import Time vs Batch Size ({threads} threads)")
        ax1.grid(True)
        if idx == 0:
            ax1.legend()

        # Plot 2: Throughput vs Batch Size
        ax2 = axes[idx, 1]
        for (iso, fmt), group in thread_data.groupby(["isolation_level", "format"]):
            label = f"{iso} ({fmt})"
            ax2.plot(
                group["batch_size"],
                group["nodes_per_second"],
                "o-",
                label=f"{label} Nodes",
            )
            ax2.plot(
                group["batch_size"],
                group["edges_per_second"],
                "s--",
                label=f"{label} Edges",
            )
        ax2.set_xscale("log")
        ax2.set_xlabel("Batch Size")
        ax2.set_ylabel("Throughput (items/second)")
        ax2.set_title(f"Throughput vs Batch Size ({threads} threads)")
        ax2.grid(True)
        if idx == 0:
            ax2.legend()

        # Plot 3: Average Latency vs Batch Size
        ax3 = axes[idx, 2]
        for (iso, fmt), group in thread_data.groupby(["isolation_level", "format"]):
            label = f"{iso} ({fmt})"
            ax3.plot(
                group["batch_size"],
                group["avg_node_latency_ms"],
                "o-",
                label=f"{label} Nodes",
            )
            ax3.plot(
                group["batch_size"],
                group["avg_edge_latency_ms"],
                "s--",
                label=f"{label} Edges",
            )
        ax3.set_xscale("log")
        ax3.set_xlabel("Batch Size")
        ax3.set_ylabel("Average Latency (ms)")
        ax3.set_title(f"Average Latency vs Batch Size ({threads} threads)")
        ax3.grid(True)
        if idx == 0:
            ax3.legend()

        # Plot 4: 99th Percentile Latency vs Batch Size
        ax4 = axes[idx, 3]
        for (iso, fmt), group in thread_data.groupby(["isolation_level", "format"]):
            label = f"{iso} ({fmt})"
            ax4.plot(
                group["batch_size"],
                group["p99_node_latency_ms"],
                "o-",
                label=f"{label} Nodes",
            )
            ax4.plot(
                group["batch_size"],
                group["p99_edge_latency_ms"],
                "s--",
                label=f"{label} Edges",
            )
        ax4.set_xscale("log")
        ax4.set_xlabel("Batch Size")
        ax4.set_ylabel("99th Percentile Latency (ms)")
        ax4.set_title(f"99th Percentile Latency vs Batch Size ({threads} threads)")
        ax4.grid(True)
        if idx == 0:
            ax4.legend()

    plt.tight_layout()
    plt.savefig(
        os.path.join(plots_dir, "detailed_metrics.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()


def plot_summary_metrics(data, plots_dir):
    """Create summary plots showing best performance across thread counts."""
    plt.figure(figsize=(15, 10))

    # Plot 1: Best Throughput vs Thread Count
    plt.subplot(2, 2, 1)
    for (iso, fmt), group in data.groupby(["isolation_level", "format"]):
        best_batch_size = group.groupby("num_threads")["nodes_per_second"].idxmax()
        best_results = group.loc[best_batch_size].sort_values("num_threads")

        plt.plot(
            best_results["num_threads"],
            best_results["nodes_per_second"],
            "o-",
            label=f"{iso} ({fmt}) Nodes",
        )
        plt.plot(
            best_results["num_threads"],
            best_results["edges_per_second"],
            "s--",
            label=f"{iso} ({fmt}) Edges",
        )

    plt.xlabel("Number of Threads")
    plt.ylabel("Throughput (items/second)")
    plt.title("Best Throughput vs Thread Count")
    plt.grid(True)
    plt.legend()

    # Plot 2: Best Average Latency vs Thread Count
    plt.subplot(2, 2, 2)
    for (iso, fmt), group in data.groupby(["isolation_level", "format"]):
        best_batch_size = group.groupby("num_threads")["nodes_per_second"].idxmax()
        best_results = group.loc[best_batch_size].sort_values("num_threads")

        plt.plot(
            best_results["num_threads"],
            best_results["avg_node_latency_ms"],
            "o-",
            label=f"{iso} ({fmt}) Nodes",
        )
        plt.plot(
            best_results["num_threads"],
            best_results["avg_edge_latency_ms"],
            "s--",
            label=f"{iso} ({fmt}) Edges",
        )

    plt.xlabel("Number of Threads")
    plt.ylabel("Average Latency (ms)")
    plt.title("Best Average Latency vs Thread Count")
    plt.grid(True)
    plt.legend()

    # Plot 3: Best 99th Percentile Latency vs Thread Count
    plt.subplot(2, 2, 3)
    for (iso, fmt), group in data.groupby(["isolation_level", "format"]):
        best_batch_size = group.groupby("num_threads")["nodes_per_second"].idxmax()
        best_results = group.loc[best_batch_size].sort_values("num_threads")

        plt.plot(
            best_results["num_threads"],
            best_results["p99_node_latency_ms"],
            "o-",
            label=f"{iso} ({fmt}) Nodes",
        )
        plt.plot(
            best_results["num_threads"],
            best_results["p99_edge_latency_ms"],
            "s--",
            label=f"{iso} ({fmt}) Edges",
        )

    plt.xlabel("Number of Threads")
    plt.ylabel("99th Percentile Latency (ms)")
    plt.title("Best 99th Percentile Latency vs Thread Count")
    plt.grid(True)
    plt.legend()

    # Plot 4: Latency Distribution (99th percentile / average ratio)
    plt.subplot(2, 2, 4)
    for (iso, fmt), group in data.groupby(["isolation_level", "format"]):
        best_batch_size = group.groupby("num_threads")["nodes_per_second"].idxmax()
        best_results = group.loc[best_batch_size].sort_values("num_threads")

        plt.plot(
            best_results["num_threads"],
            best_results["p99_node_latency_ms"] / best_results["avg_node_latency_ms"],
            "o-",
            label=f"{iso} ({fmt}) Nodes",
        )
        plt.plot(
            best_results["num_threads"],
            best_results["p99_edge_latency_ms"] / best_results["avg_edge_latency_ms"],
            "s--",
            label=f"{iso} ({fmt}) Edges",
        )

    plt.xlabel("Number of Threads")
    plt.ylabel("Latency Ratio (p99/avg)")
    plt.title("Latency Distribution vs Thread Count")
    plt.grid(True)
    plt.legend()

    plt.tight_layout()
    plt.savefig(
        os.path.join(plots_dir, "summary_metrics.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()


def plot_heatmaps(data, plots_dir):
    """Create heatmaps showing performance metrics across different configurations."""
    metrics = [
        "nodes_per_second",
        "edges_per_second",
        "avg_node_latency_ms",
        "avg_edge_latency_ms",
        "p99_node_latency_ms",
        "p99_edge_latency_ms",
    ]

    for metric in metrics:
        plt.figure(figsize=(15, 10))
        pivot_data = data.pivot_table(
            values=metric,
            index="num_threads",
            columns=["isolation_level", "format"],
            aggfunc="mean",
        )

        sns.heatmap(
            pivot_data,
            annot=True,
            fmt=".2e",
            cmap="YlOrRd",
            cbar_kws={"label": metric.replace("_", " ").title()},
        )

        plt.title(f"{metric.replace('_', ' ').title()} Heatmap")
        plt.tight_layout()
        plt.savefig(
            os.path.join(plots_dir, f"{metric}_heatmap.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()


def plot_performance_score(data, plots_dir):
    """Plot the calculated performance score for different configurations."""
    plt.figure(figsize=(15, 10))

    # Calculate performance score for each row
    data["performance_score"] = np.sqrt(
        (data["nodes_per_second"] + data["edges_per_second"])
        / 2.0
        * (
            1000.0
            / (
                data["avg_node_latency_ms"]
                + data["avg_edge_latency_ms"]
                + data["p99_node_latency_ms"]
                + data["p99_edge_latency_ms"]
            )
            / 4.0
        )
    )

    # Create grouped bar chart
    sns.barplot(data=data, x="isolation_level", y="performance_score", hue="format")

    plt.title("Performance Score by Isolation Level")
    plt.xlabel("Isolation Level")
    plt.ylabel("Performance Score")
    plt.xticks(rotation=45)
    plt.legend(title="Format")
    plt.tight_layout()
    plt.savefig(
        os.path.join(plots_dir, "performance_score.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()


def main():
    # Set style
    sns.set_style("whitegrid")
    try:
        plt.style.use("seaborn-v0_8")
    except Exception as e:
        print(f"Warning: Could not set seaborn style: {str(e)}")
        print("Using default style instead")

    # Load data
    data = load_data()
    if data is None:
        return

    # Create plots directory under build if it doesn't exist
    plots_dir = os.path.join("build", "plots")
    os.makedirs(plots_dir, exist_ok=True)

    print("\nGenerating plots...")

    # Generate all plots
    plot_detailed_metrics(data, plots_dir)
    plot_summary_metrics(data, plots_dir)
    plot_heatmaps(data, plots_dir)
    plot_performance_score(data, plots_dir)

    print(f"\nAll plots have been generated in the '{plots_dir}' directory.")


if __name__ == "__main__":
    main()
