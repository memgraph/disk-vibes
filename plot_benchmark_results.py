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
    build_dir = "build"
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
            # Example: benchmark_READ_UNCOMMITTED_results_arrow.csv
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


def plot_throughput_vs_batch_size(data, metric, title, filename):
    """Plot throughput vs batch size for different isolation levels and formats."""
    plt.figure(figsize=(15, 10))

    # Create a grid of subplots for each thread count
    thread_counts = sorted(data["num_threads"].unique())
    n_cols = min(3, len(thread_counts))
    n_rows = (len(thread_counts) + n_cols - 1) // n_cols

    for idx, threads in enumerate(thread_counts, 1):
        plt.subplot(n_rows, n_cols, idx)

        # Filter data for current thread count
        thread_data = data[data["num_threads"] == threads]

        # Plot for each isolation level and format combination
        for (iso, fmt), group in thread_data.groupby(["isolation_level", "format"]):
            label = f"{iso} ({fmt})"
            plt.plot(
                group["batch_size"], group[metric], "o-", label=label, markersize=4
            )

        plt.xscale("log")
        plt.yscale("log")
        plt.grid(True, which="both", ls="-", alpha=0.2)
        plt.title(f"Threads: {threads}")
        plt.xlabel("Batch Size")
        plt.ylabel(metric.replace("_", " ").title())

        # Add legend only to the first subplot
        if idx == 1:
            plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")

    plt.suptitle(title, y=1.02, fontsize=16)
    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=300)
    plt.close()


def plot_latency_vs_batch_size(data, metric, title, filename):
    """Plot latency vs batch size for different isolation levels and formats."""
    plt.figure(figsize=(15, 10))

    # Create a grid of subplots for each thread count
    thread_counts = sorted(data["num_threads"].unique())
    n_cols = min(3, len(thread_counts))
    n_rows = (len(thread_counts) + n_cols - 1) // n_cols

    for idx, threads in enumerate(thread_counts, 1):
        plt.subplot(n_rows, n_cols, idx)

        # Filter data for current thread count
        thread_data = data[data["num_threads"] == threads]

        # Plot for each isolation level and format combination
        for (iso, fmt), group in thread_data.groupby(["isolation_level", "format"]):
            label = f"{iso} ({fmt})"
            plt.plot(
                group["batch_size"], group[metric], "o-", label=label, markersize=4
            )

        plt.xscale("log")
        plt.yscale("log")
        plt.grid(True, which="both", ls="-", alpha=0.2)
        plt.title(f"Threads: {threads}")
        plt.xlabel("Batch Size")
        plt.ylabel(f'{metric.replace("_", " ").title()} (ms)')

        # Add legend only to the first subplot
        if idx == 1:
            plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")

    plt.suptitle(title, y=1.02, fontsize=16)
    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=300)
    plt.close()


def plot_heatmap(data, metric, title, filename):
    """Create a heatmap showing the performance metric across different configurations."""
    # Pivot the data to create a matrix for the heatmap
    pivot_data = data.pivot_table(
        values=metric,
        index="num_threads",
        columns=["isolation_level", "format"],
        aggfunc="mean",
    )

    plt.figure(figsize=(15, 10))
    sns.heatmap(
        pivot_data,
        annot=True,
        fmt=".2e",
        cmap="YlOrRd",
        cbar_kws={"label": metric.replace("_", " ").title()},
    )
    plt.title(title)
    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=300)
    plt.close()


def plot_isolation_level_comparison(data, metric, title, filename):
    """Create a grouped bar chart comparing isolation levels."""
    plt.figure(figsize=(15, 10))

    # Calculate mean values for each isolation level and format
    mean_data = data.groupby(["isolation_level", "format"])[metric].mean().reset_index()

    # Create grouped bar chart
    sns.barplot(data=mean_data, x="isolation_level", y=metric, hue="format")

    plt.title(title)
    plt.xlabel("Isolation Level")
    plt.ylabel(metric.replace("_", " ").title())
    plt.xticks(rotation=45)
    plt.legend(title="Format")
    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=300)
    plt.close()


def plot_performance_score(data, title, filename):
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

    plt.title(title)
    plt.xlabel("Isolation Level")
    plt.ylabel("Performance Score")
    plt.xticks(rotation=45)
    plt.legend(title="Format")
    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=300)
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

    # Plot throughput metrics
    plot_throughput_vs_batch_size(
        data,
        "nodes_per_second",
        "Node Throughput vs Batch Size",
        os.path.join(plots_dir, "node_throughput_vs_batch_size.png"),
    )
    plot_throughput_vs_batch_size(
        data,
        "edges_per_second",
        "Edge Throughput vs Batch Size",
        os.path.join(plots_dir, "edge_throughput_vs_batch_size.png"),
    )

    # Plot latency metrics
    plot_latency_vs_batch_size(
        data,
        "avg_node_latency_ms",
        "Average Node Latency vs Batch Size",
        os.path.join(plots_dir, "avg_node_latency_vs_batch_size.png"),
    )
    plot_latency_vs_batch_size(
        data,
        "p99_node_latency_ms",
        "P99 Node Latency vs Batch Size",
        os.path.join(plots_dir, "p99_node_latency_vs_batch_size.png"),
    )
    plot_latency_vs_batch_size(
        data,
        "avg_edge_latency_ms",
        "Average Edge Latency vs Batch Size",
        os.path.join(plots_dir, "avg_edge_latency_vs_batch_size.png"),
    )
    plot_latency_vs_batch_size(
        data,
        "p99_edge_latency_ms",
        "P99 Edge Latency vs Batch Size",
        os.path.join(plots_dir, "p99_edge_latency_vs_batch_size.png"),
    )

    # Plot heatmaps
    plot_heatmap(
        data,
        "nodes_per_second",
        "Node Throughput Heatmap",
        os.path.join(plots_dir, "node_throughput_heatmap.png"),
    )
    plot_heatmap(
        data,
        "edges_per_second",
        "Edge Throughput Heatmap",
        os.path.join(plots_dir, "edge_throughput_heatmap.png"),
    )

    # Plot isolation level comparisons
    plot_isolation_level_comparison(
        data,
        "nodes_per_second",
        "Node Throughput by Isolation Level",
        os.path.join(plots_dir, "node_throughput_by_isolation.png"),
    )
    plot_isolation_level_comparison(
        data,
        "edges_per_second",
        "Edge Throughput by Isolation Level",
        os.path.join(plots_dir, "edge_throughput_by_isolation.png"),
    )

    # Plot performance score
    plot_performance_score(
        data,
        "Performance Score by Isolation Level",
        os.path.join(plots_dir, "performance_score_by_isolation.png"),
    )

    print(f"\nAll plots have been generated in the '{plots_dir}' directory.")


if __name__ == "__main__":
    main()
