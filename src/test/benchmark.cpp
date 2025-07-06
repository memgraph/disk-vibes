#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <sstream>
#include <thread>
#include <vector>

#include "spdlog/spdlog.h"
#include "storage/edge.hpp"
#include "storage/graph.hpp"
#include "storage/graph_transaction.hpp"
#include "storage/isolation_level.hpp"

using namespace memgraph;
using Node = memgraph::Node;

std::string IsolationLevelToString(memgraph::IsolationLevel level) {
  switch (level) {
  case memgraph::IsolationLevel::NO_ISOLATION:
    return "NO_ISOLATION";
  case memgraph::IsolationLevel::READ_UNCOMMITTED:
    return "READ_UNCOMMITTED";
  case memgraph::IsolationLevel::READ_COMMITTED:
    return "READ_COMMITTED";
  default:
    return "UNKNOWN";
  }
}

struct BenchmarkResult {
  int64_t batch_size;
  int num_threads;
  memgraph::IsolationLevel isolation_level;
  double import_time;
  double nodes_per_second;
  double edges_per_second;
  double avg_node_latency_ms;
  double p99_node_latency_ms;
  double avg_edge_latency_ms;
  double p99_edge_latency_ms;

  // Calculate a balanced score using geometric mean of throughput and inverse
  // latency NOTE: Use of the β-harmonic mean of throughput and the inverse of
  // latency—β lets you dial the trade-off BUT: A simpler quick-and-dirty
  // variant is to use the geometric mean
  // TODO(gitbuda): Ask user questions about what's more important; throughput
  // or latency, read/write ration, etc. Make sure the difference between
  // node&edge-related performance is take into the account.
  double calculate_score() const {
    // Calculate average throughput (higher is better)
    double throughput = (nodes_per_second + edges_per_second) / 2.0;
    // Calculate average latency (lower is better)
    double latency = (avg_node_latency_ms + avg_edge_latency_ms + p99_node_latency_ms + p99_edge_latency_ms) / 4.0;
    // Take geometric mean of throughput and 1000/latency
    // Using 1000 as a scaling factor to make the inverse latency comparable to throughput
    return std::sqrt(throughput * (1000.0 / latency));
  }
};

std::mutex cout_mutex;

void print_progress(const std::string &prefix, int64_t current, int64_t total) {
  // NOTE: std::cout is important here because of printing progress in one line
  // (don't know how to achieve the same using spdlog).
  std::lock_guard<std::mutex> lock(cout_mutex);
  float percentage = (float)current / total * 100;
  std::cout << "\r" << prefix << ": " << std::fixed << std::setprecision(1) << percentage << "% (" << current << "/"
            << total << ")" << std::flush;
}

BenchmarkResult run_benchmark(Graph &graph, int64_t batch_size, int num_threads, int64_t total_nodes,
                              int64_t total_edges, memgraph::IsolationLevel isolation_level) {
  // Create TransactionalGraph if isolation is enabled
  std::unique_ptr<TransactionalGraph> tx_graph;
  if (isolation_level != memgraph::IsolationLevel::NO_ISOLATION) {
    tx_graph = std::make_unique<TransactionalGraph>(graph);
  }

  // Helper function to generate random properties
  auto generate_properties = [](int64_t id, int num_props) {
    std::string props = "{";
    for (int i = 0; i < num_props; ++i) {
      if (i > 0)
        props += ",";
      props += "\"prop" + std::to_string(i) + "\":\"" + std::to_string(id * 1000 + i) + "\"";
    }
    props += "}";
    return props;
  };

  // Helper function to generate random labels
  auto generate_labels = [](int64_t id) {
    std::vector<std::string> all_labels = {"Person", "Employee", "Customer", "User", "Developer"};
    std::vector<std::string> labels;
    int num_labels = 2 + (id % 2); // 2-3 labels
    for (int i = 0; i < num_labels; ++i) {
      labels.push_back(all_labels[(id + i) % all_labels.size()]);
    }
    return labels;
  };

  // Create and import nodes in parallel batches
  std::atomic<int64_t> nodes_processed{0};
  std::vector<std::thread> node_threads;
  auto node_start_time = std::chrono::high_resolution_clock::now();

  std::vector<double> node_latencies;
  std::mutex node_latency_mutex;
  // Calculate number of batches and reserve space
  int64_t num_node_batches = (total_nodes + batch_size - 1) / batch_size;
  node_latencies.reserve(num_node_batches);

  for (int t = 0; t < num_threads; ++t) {
    node_threads.emplace_back([&, t]() {
      for (int64_t i = t * batch_size; i < total_nodes; i += num_threads * batch_size) {
        std::vector<Node> nodes;
        int64_t end = std::min(i + batch_size, total_nodes);
        nodes.reserve(end - i);

        for (int64_t j = i; j < end; ++j) {
          auto labels = generate_labels(j);
          int num_props = 8 + (j % 5); // 8-12 properties
          nodes.push_back(Node(j, labels, generate_properties(j, num_props), j));
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        ::arrow::Status status;
        if (isolation_level != memgraph::IsolationLevel::NO_ISOLATION) {
          auto tx_result = tx_graph->BeginTransaction(isolation_level);
          if (!tx_result.ok()) {
            spdlog::error("Error beginning transaction: {}", tx_result.status().ToString());
            return;
          }
          auto tx = std::move(tx_result).ValueOrDie();
          status = tx->AddNodes(nodes);
          if (!status.ok()) {
            spdlog::error("Error adding nodes: {}", status.ToString());
            return;
          }
          status = tx->Commit();
          if (!status.ok()) {
            spdlog::error("Error committing transaction: {}", status.ToString());
            return;
          }
        } else {
          status = graph.AddNodes(nodes);
        }
        auto end_time = std::chrono::high_resolution_clock::now();

        if (!status.ok()) {
          spdlog::error("Error adding nodes: {}", status.ToString());
          return;
        }

        double latency_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
        {
          std::lock_guard<std::mutex> lock(node_latency_mutex);
          node_latencies.push_back(latency_ms);
        }

        nodes_processed += (end - i);
        print_progress("Importing nodes", nodes_processed.load(), total_nodes);
      }
    });
  }
  for (auto &thread : node_threads) {
    thread.join();
  }

  auto node_end_time = std::chrono::high_resolution_clock::now();
  std::cout << std::endl;
  std::chrono::duration<double> node_duration = node_end_time - node_start_time;
  std::cout << "Node import time: " << node_duration.count() << " seconds" << std::endl;

  // Create and import edges in parallel batches
  std::atomic<int64_t> edges_processed{0};
  std::vector<std::thread> edge_threads;
  auto edge_start_time = std::chrono::high_resolution_clock::now();

  std::vector<double> edge_latencies;
  std::mutex edge_latency_mutex;
  // Calculate number of batches and reserve space
  int64_t num_edge_batches = (total_edges + batch_size - 1) / batch_size;
  edge_latencies.reserve(num_edge_batches);

  for (int t = 0; t < num_threads; ++t) {
    edge_threads.emplace_back([&, t]() {
      for (int64_t i = t * batch_size; i < total_edges; i += num_threads * batch_size) {
        std::vector<memgraph::Edge> edges;
        int64_t end = std::min(i + batch_size, total_edges);
        edges.reserve(end - i);

        for (int64_t j = i; j < end; ++j) {
          int64_t src = j % total_nodes;
          int64_t dst = (j + 1) % total_nodes;
          std::string props = "{\"since\":" + std::to_string(j) + ",\"weight\":" + std::to_string(j % 100) +
                              ",\"type\":" + std::to_string(j % 3) + "}";
          edges.emplace_back(j, src, dst, "KNOWS", props, j);
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        ::arrow::Status status;
        if (isolation_level != memgraph::IsolationLevel::NO_ISOLATION) {
          auto tx_result = tx_graph->BeginTransaction(isolation_level);
          if (!tx_result.ok()) {
            spdlog::error("Error beginning transaction: {}", tx_result.status().ToString());
            return;
          }
          auto tx = std::move(tx_result).ValueOrDie();
          status = tx->AddEdges(edges);
          if (!status.ok()) {
            spdlog::error("Error adding edges: {}", status.ToString());
            return;
          }
          status = tx->Commit();
          if (!status.ok()) {
            spdlog::error("Error committing transaction: {}", status.ToString());
            return;
          }
        } else {
          status = graph.AddEdges(edges);
        }
        auto end_time = std::chrono::high_resolution_clock::now();

        if (!status.ok()) {
          spdlog::error("Error adding edges: {}", status.ToString());
          return;
        }

        double latency_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
        {
          std::lock_guard<std::mutex> lock(edge_latency_mutex);
          edge_latencies.push_back(latency_ms);
        }

        edges_processed += (end - i);
        print_progress("Importing edges", edges_processed.load(), total_edges);
      }
    });
  }

  for (auto &thread : edge_threads) {
    thread.join();
  }

  auto edge_end_time = std::chrono::high_resolution_clock::now();
  std::cout << std::endl;
  std::chrono::duration<double> edge_duration = edge_end_time - edge_start_time;
  std::cout << "Edge import time: " << edge_duration.count() << " seconds" << std::endl;

  // Calculate total duration
  auto total_duration = node_duration + edge_duration;
  std::cout << "Total elapsed time: " << total_duration.count() << " seconds" << std::endl;

  // Calculate latency statistics
  auto calculate_stats = [](const std::vector<double> &latencies) -> std::pair<double, double> {
    if (latencies.empty())
      return {0.0, 0.0};

    std::vector<double> sorted_latencies = latencies;
    std::sort(sorted_latencies.begin(), sorted_latencies.end());

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    double p99 = sorted_latencies[static_cast<size_t>(latencies.size() * 0.99)];

    return {avg, p99};
  };

  auto [avg_node_latency, p99_node_latency] = calculate_stats(node_latencies);
  auto [avg_edge_latency, p99_edge_latency] = calculate_stats(edge_latencies);

  std::cout << "\nLatency Statistics:" << std::endl;
  std::cout << "Node Operations:" << std::endl;
  std::cout << "  Average Latency: " << avg_node_latency << " ms" << std::endl;
  std::cout << "  99th Percentile: " << p99_node_latency << " ms" << std::endl;
  std::cout << "Edge Operations:" << std::endl;
  std::cout << "  Average Latency: " << avg_edge_latency << " ms" << std::endl;
  std::cout << "  99th Percentile: " << p99_edge_latency << " ms" << std::endl;

  return BenchmarkResult{batch_size,
                         num_threads,
                         isolation_level,
                         total_duration.count(),
                         total_nodes / node_duration.count(),
                         total_edges / edge_duration.count(),
                         avg_node_latency,
                         p99_node_latency,
                         avg_edge_latency,
                         p99_edge_latency};
}

int main(int argc, char *argv[]) {
  const bool KEEP_TEST_DATA = false; // Set to true to keep test data, false to clean up

  // Default values
  int64_t TOTAL_NODES = 1'000'000;
  int64_t TOTAL_EDGES = 1'000'000;
  std::vector<int64_t> batch_sizes = {100, 1'000, 10'000, 100'000};
  std::vector<int> thread_counts;
  std::vector<memgraph::IsolationLevel> isolation_levels = {memgraph::IsolationLevel::NO_ISOLATION,
                                                            memgraph::IsolationLevel::READ_UNCOMMITTED,
                                                            memgraph::IsolationLevel::READ_COMMITTED};

  // Parse command line arguments
  if (argc > 1) {
    TOTAL_NODES = std::stoll(argv[1]);
    TOTAL_EDGES = TOTAL_NODES; // Keep edges equal to nodes by default
  }
  if (argc > 2) {
    TOTAL_EDGES = std::stoll(argv[2]);
  }
  if (argc > 3) {
    // Parse batch sizes as comma-separated list
    std::string batch_sizes_str = argv[3];
    batch_sizes.clear();
    std::stringstream ss(batch_sizes_str);
    std::string item;
    while (std::getline(ss, item, ',')) {
      batch_sizes.push_back(std::stoll(item));
    }
  }
  if (argc > 4) {
    // Parse thread counts as comma-separated list
    std::string thread_counts_str = argv[4];
    std::stringstream ss(thread_counts_str);
    std::string item;
    while (std::getline(ss, item, ',')) {
      thread_counts.push_back(std::stoi(item));
    }
  }

  // If thread counts weren't provided, generate them based on hardware
  if (thread_counts.empty()) {
    int max_threads = std::thread::hardware_concurrency();
    for (int threads = 1; threads <= max_threads; threads *= 2) {
      thread_counts.push_back(threads);
    }
    if (thread_counts.back() != max_threads) {
      thread_counts.push_back(max_threads);
    }
  }

  // Create main benchmark directory
  std::filesystem::path main_benchmark_dir = std::filesystem::temp_directory_path() / "graph_benchmark";
  if (!KEEP_TEST_DATA) {
    std::filesystem::remove_all(main_benchmark_dir);
  }
  std::filesystem::create_directories(main_benchmark_dir);

  // Run benchmarks for both ARROW and PARQUET formats
  std::vector<PageType> page_types = {PageType::ARROW, PageType::PARQUET};
  for (const auto &page_type : page_types) {
    std::string format_name = (page_type == PageType::ARROW) ? "arrow" : "parquet";
    std::cout << "\n=== Running benchmarks for " << format_name << " format ===" << std::endl;

    for (const auto &isolation_level : isolation_levels) {
      std::string isolation_name = IsolationLevelToString(isolation_level);
      std::cout << "\nBenchmark Configuration:" << std::endl;
      std::cout << "Format: " << format_name << std::endl;
      std::cout << "Isolation Level: " << isolation_name << std::endl;
      std::cout << "Keep Test Data: " << (KEEP_TEST_DATA ? "Yes" : "No") << std::endl;
      std::cout << "Total Nodes: " << TOTAL_NODES << std::endl;
      std::cout << "Total Edges: " << TOTAL_EDGES << std::endl;
      std::cout << "Batch Sizes: ";
      for (size_t i = 0; i < batch_sizes.size(); ++i) {
        std::cout << batch_sizes[i];
        if (i < batch_sizes.size() - 1)
          std::cout << ", ";
      }
      std::cout << std::endl;
      std::cout << "Thread Counts: ";
      for (size_t i = 0; i < thread_counts.size(); ++i) {
        std::cout << thread_counts[i];
        if (i < thread_counts.size() - 1)
          std::cout << ", ";
      }
      std::cout << std::endl << std::endl;

      std::vector<BenchmarkResult> results;

      // Run benchmarks for each thread count and batch size combination
      for (int num_threads : thread_counts) {
        std::cout << "\nTesting with " << num_threads << " threads:" << std::endl;
        for (int64_t batch_size : batch_sizes) {
          std::cout << "\nBenchmarking batch size: " << batch_size << std::endl;
          // Create test directory with format, isolation level, thread count, and batch size in the path
          std::filesystem::path test_dir =
              main_benchmark_dir / (format_name + "_" + isolation_name + "_t" + std::to_string(num_threads) + "_b" +
                                    std::to_string(batch_size));
          if (!KEEP_TEST_DATA) {
            std::filesystem::remove_all(test_dir);
          }
          std::filesystem::create_directories(test_dir);

          Graph graph(test_dir, page_type, batch_size);
          auto result = run_benchmark(graph, batch_size, num_threads, TOTAL_NODES, TOTAL_EDGES, isolation_level);
          if (result.import_time < 0) {
            spdlog::error("Benchmark failed for batch size {} with {} threads", batch_size, num_threads);
            continue;
          }
          results.push_back(result);

          if (KEEP_TEST_DATA) {
            std::cout << "Benchmark data kept in: " << test_dir << std::endl;
          }
        }
      }

      // Save results to CSV
      std::filesystem::path results_path("benchmark_" + isolation_name + "_results_" + format_name + ".csv");
      std::ofstream csv(results_path);
      csv << "batch_size,num_threads,isolation_level,import_time,nodes_per_second,edges_per_second,avg_node_latency_ms,"
             "p99_node_latency_ms,avg_edge_latency_ms,p99_edge_latency_ms\n";

      // Find the configuration with the best balanced score
      auto best_result =
          std::max_element(results.begin(), results.end(), [](const BenchmarkResult &a, const BenchmarkResult &b) {
            return a.calculate_score() < b.calculate_score();
          });

      // Write results to CSV
      for (const auto &result : results) {
        csv << result.batch_size << "," << result.num_threads << "," << IsolationLevelToString(result.isolation_level)
            << "," << result.import_time << "," << result.nodes_per_second << "," << result.edges_per_second << ","
            << result.avg_node_latency_ms << "," << result.p99_node_latency_ms << "," << result.avg_edge_latency_ms
            << "," << result.p99_edge_latency_ms << "\n";
      }

      // Print recommendation
      std::cout << "\nRecommended configuration for " << format_name << " with " << isolation_name
                << " (best balanced performance):" << std::endl;
      std::cout << "Batch size: " << best_result->batch_size << std::endl;
      std::cout << "Number of threads: " << best_result->num_threads << std::endl;
      std::cout << "Import time: " << best_result->import_time << " seconds" << std::endl;
      std::cout << "Node throughput: " << best_result->nodes_per_second << " nodes/second" << std::endl;
      std::cout << "Edge throughput: " << best_result->edges_per_second << " edges/second" << std::endl;
      std::cout << "Average Node Latency: " << best_result->avg_node_latency_ms << " ms" << std::endl;
      std::cout << "99th Percentile Node Latency: " << best_result->p99_node_latency_ms << " ms" << std::endl;
      std::cout << "Average Edge Latency: " << best_result->avg_edge_latency_ms << " ms" << std::endl;
      std::cout << "99th Percentile Edge Latency: " << best_result->p99_edge_latency_ms << " ms" << std::endl;
      std::cout << "Performance Score: " << best_result->calculate_score() << std::endl;
      std::cout << "\nResults saved to " << results_path << std::endl;
    }
  }

  if (KEEP_TEST_DATA) {
    std::cout << "\nGenerated benchmark data files kept under: " << main_benchmark_dir << std::endl;
  }

  return 0;
}
