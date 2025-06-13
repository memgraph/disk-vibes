#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Read the benchmark results from the build directory
build_dir = os.path.join(os.path.dirname(__file__), "build")

# Read both isolation levels for each format
isolation_levels = ["no_isolation", "read_uncommitted"]
formats = ["arrow", "parquet"]

dfs = []
for isolation in isolation_levels:
    for fmt in formats:
        csv_path = os.path.join(build_dir, f"benchmark_{isolation}_results_{fmt}.csv")
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            df["format"] = fmt.upper()
            df["isolation"] = isolation
            dfs.append(df)

# Combine all dataframes
df = pd.concat(dfs)

# Set the style
plt.style.use("seaborn-v0_8")
sns.set_palette("husl")

# Create a figure with 4 subplots for each thread count
thread_counts = sorted(df["num_threads"].unique())
num_threads = len(thread_counts)
fig, axes = plt.subplots(num_threads, 4, figsize=(20, 5 * num_threads))

# Plot for each thread count
for idx, threads in enumerate(thread_counts):
    thread_data = df[df["num_threads"] == threads].sort_values("batch_size")

    # Plot 1: Import Time vs Batch Size
    ax1 = axes[idx, 0]
    for fmt in formats:
        for isolation in isolation_levels:
            data = thread_data[
                (thread_data["format"] == fmt.upper())
                & (thread_data["isolation"] == isolation)
            ]
            if not data.empty:
                ax1.plot(
                    data["batch_size"],
                    data["import_time"],
                    "o-",
                    label=f"{fmt.upper()} {isolation} ({threads} threads)",
                )
    ax1.set_xscale("log")
    ax1.set_xlabel("Batch Size")
    ax1.set_ylabel("Time (seconds)")
    ax1.set_title(f"Import Time vs Batch Size ({threads} threads)")
    ax1.grid(True)
    ax1.legend()

    # Plot 2: Throughput vs Batch Size
    ax2 = axes[idx, 1]
    for fmt in formats:
        for isolation in isolation_levels:
            data = thread_data[
                (thread_data["format"] == fmt.upper())
                & (thread_data["isolation"] == isolation)
            ]
            if not data.empty:
                ax2.plot(
                    data["batch_size"],
                    data["nodes_per_second"],
                    "o-",
                    label=f"{fmt.upper()} {isolation} Nodes/second",
                )
                ax2.plot(
                    data["batch_size"],
                    data["edges_per_second"],
                    "s--",
                    label=f"{fmt.upper()} {isolation} Edges/second",
                )
    ax2.set_xscale("log")
    ax2.set_xlabel("Batch Size")
    ax2.set_ylabel("Throughput (items/second)")
    ax2.set_title(f"Throughput vs Batch Size ({threads} threads)")
    ax2.grid(True)
    ax2.legend()

    # Plot 3: Average Latency vs Batch Size
    ax3 = axes[idx, 2]
    for fmt in formats:
        for isolation in isolation_levels:
            data = thread_data[
                (thread_data["format"] == fmt.upper())
                & (thread_data["isolation"] == isolation)
            ]
            if not data.empty:
                ax3.plot(
                    data["batch_size"],
                    data["avg_node_latency_ms"],
                    "o-",
                    label=f"{fmt.upper()} {isolation} Nodes",
                )
                ax3.plot(
                    data["batch_size"],
                    data["avg_edge_latency_ms"],
                    "s--",
                    label=f"{fmt.upper()} {isolation} Edges",
                )
    ax3.set_xscale("log")
    ax3.set_xlabel("Batch Size")
    ax3.set_ylabel("Average Latency (ms)")
    ax3.set_title(f"Average Latency vs Batch Size ({threads} threads)")
    ax3.grid(True)
    ax3.legend()

    # Plot 4: 99th Percentile Latency vs Batch Size
    ax4 = axes[idx, 3]
    for fmt in formats:
        for isolation in isolation_levels:
            data = thread_data[
                (thread_data["format"] == fmt.upper())
                & (thread_data["isolation"] == isolation)
            ]
            if not data.empty:
                ax4.plot(
                    data["batch_size"],
                    data["p99_node_latency_ms"],
                    "o-",
                    label=f"{fmt.upper()} {isolation} Nodes",
                )
                ax4.plot(
                    data["batch_size"],
                    data["p99_edge_latency_ms"],
                    "s--",
                    label=f"{fmt.upper()} {isolation} Edges",
                )
    ax4.set_xscale("log")
    ax4.set_xlabel("Batch Size")
    ax4.set_ylabel("99th Percentile Latency (ms)")
    ax4.set_title(f"99th Percentile Latency vs Batch Size ({threads} threads)")
    ax4.grid(True)
    ax4.legend()

# Adjust layout and save detailed plots
plt.tight_layout()
detailed_plot_path = os.path.join(build_dir, "benchmark_results_detailed.png")
plt.savefig(detailed_plot_path, dpi=300, bbox_inches="tight")
plt.close()

# Create summary plots
plt.figure(figsize=(15, 10))

# Create 2x2 subplot layout for summary plots
plt.subplot(2, 2, 1)
# Plot throughput vs thread count for the best batch size of each format and isolation level
for fmt in formats:
    for isolation in isolation_levels:
        format_data = df[(df["format"] == fmt.upper()) & (df["isolation"] == isolation)]
        if not format_data.empty:
            best_batch_size = format_data.groupby("num_threads")[
                "nodes_per_second"
            ].idxmax()
            best_results = format_data.loc[best_batch_size].sort_values("num_threads")

            plt.plot(
                best_results["num_threads"],
                best_results["nodes_per_second"],
                "o-",
                label=f"{fmt.upper()} {isolation} Nodes/second",
            )
            plt.plot(
                best_results["num_threads"],
                best_results["edges_per_second"],
                "s--",
                label=f"{fmt.upper()} {isolation} Edges/second",
            )

plt.xlabel("Number of Threads")
plt.ylabel("Throughput (items/second)")
plt.title("Best Throughput vs Thread Count")
plt.grid(True)
plt.legend()

# Plot average latency vs thread count
plt.subplot(2, 2, 2)
for fmt in formats:
    for isolation in isolation_levels:
        format_data = df[(df["format"] == fmt.upper()) & (df["isolation"] == isolation)]
        if not format_data.empty:
            best_batch_size = format_data.groupby("num_threads")[
                "nodes_per_second"
            ].idxmax()
            best_results = format_data.loc[best_batch_size].sort_values("num_threads")

            plt.plot(
                best_results["num_threads"],
                best_results["avg_node_latency_ms"],
                "o-",
                label=f"{fmt.upper()} {isolation} Nodes",
            )
            plt.plot(
                best_results["num_threads"],
                best_results["avg_edge_latency_ms"],
                "s--",
                label=f"{fmt.upper()} {isolation} Edges",
            )

plt.xlabel("Number of Threads")
plt.ylabel("Average Latency (ms)")
plt.title("Best Average Latency vs Thread Count")
plt.grid(True)
plt.legend()

# Plot 99th percentile latency vs thread count
plt.subplot(2, 2, 3)
for fmt in formats:
    for isolation in isolation_levels:
        format_data = df[(df["format"] == fmt.upper()) & (df["isolation"] == isolation)]
        if not format_data.empty:
            best_batch_size = format_data.groupby("num_threads")[
                "nodes_per_second"
            ].idxmax()
            best_results = format_data.loc[best_batch_size].sort_values("num_threads")

            plt.plot(
                best_results["num_threads"],
                best_results["p99_node_latency_ms"],
                "o-",
                label=f"{fmt.upper()} {isolation} Nodes",
            )
            plt.plot(
                best_results["num_threads"],
                best_results["p99_edge_latency_ms"],
                "s--",
                label=f"{fmt.upper()} {isolation} Edges",
            )

plt.xlabel("Number of Threads")
plt.ylabel("99th Percentile Latency (ms)")
plt.title("Best 99th Percentile Latency vs Thread Count")
plt.grid(True)
plt.legend()

# Plot latency distribution (99th percentile / average ratio)
plt.subplot(2, 2, 4)
for fmt in formats:
    for isolation in isolation_levels:
        format_data = df[(df["format"] == fmt.upper()) & (df["isolation"] == isolation)]
        if not format_data.empty:
            best_batch_size = format_data.groupby("num_threads")[
                "nodes_per_second"
            ].idxmax()
            best_results = format_data.loc[best_batch_size].sort_values("num_threads")

            plt.plot(
                best_results["num_threads"],
                best_results["p99_node_latency_ms"]
                / best_results["avg_node_latency_ms"],
                "o-",
                label=f"{fmt.upper()} {isolation} Nodes",
            )
            plt.plot(
                best_results["num_threads"],
                best_results["p99_edge_latency_ms"]
                / best_results["avg_edge_latency_ms"],
                "s--",
                label=f"{fmt.upper()} {isolation} Edges",
            )

plt.xlabel("Number of Threads")
plt.ylabel("Latency Ratio (p99/avg)")
plt.title("Latency Distribution vs Thread Count")
plt.grid(True)
plt.legend()

plt.tight_layout()
# Save summary plot
summary_plot_path = os.path.join(build_dir, "benchmark_results_summary.png")
plt.savefig(summary_plot_path, dpi=300, bbox_inches="tight")
plt.close()

print(f"Detailed plots saved as {detailed_plot_path}")
print(f"Summary plot saved as {summary_plot_path}")
