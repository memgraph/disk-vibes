#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>
#include <string>
#include <vector>

#include "rocks/rocks.hpp"
#include "rocks/rocks_index.hpp"
#include "storage/arrow.hpp"
#include "storage/common.hpp"
#include "storage/edge.hpp"
#include "storage/graph.hpp"
#include "storage/graph_transaction.hpp"
#include "storage/node.hpp"
#include "storage/page_edges_arrow.hpp"
#include "storage/page_nodes_arrow.hpp"

// NOTES
// * Is RocksDB a good idea for indexing? RocksDB-backed indexes in front of Arrow/Parquet give you a neat hybrid:
//   OLTP-like lookup latency with OLAP-friendly storage economics. The trade-off is complexity—two durability paths,
//   two tuning knobs, and subtle edge cases around consistency and compaction. It excels when writes are append-only
//   and reads are either key-based or big analytical scans, but starts to hurt when high-churn updates or rich
//   multi-column filtering enters the picture.

// MISC?
// TODO(gitbuda): Add JSON (simdjson).
// TODO(gitbuda): Add Result/Error object.

// INDICES?
// TODO(gitbuda): For "id" partitioning, Label+prop index is label+prop -> entity_id.
// TODO(gitbuda): For any partitioning, reverse index is entity_id -> chunks.
// TODO(gitbuda): Efficient indices require type system!

// TRANSACTIONS?
// * From https://15721.courses.cs.cmu.edu/spring2016/papers/p677-neumann.pdf, to implement fast serializable check, the
// storage engine has to know the predicate to evaluate it against the undo buffers -> query engine dependency.

using namespace memgraph;

void PrintNodeDetails(const std::vector<Node> &nodes) {
  for (const auto &node : nodes) {
    std::string labels;
    for (const auto &label : node.labels) {
      labels += label + " ";
    }
    spdlog::info("NODE id: {} labels: {} properties: {}", node.id, labels, node.props);
  }
}

int main() {
  init_spdlog();

  constexpr char kDBName[] = "gpage_db";
  std::vector<memgraph::Node> nodes = {memgraph::Node(1, {"Person"}, "{\"name\": \"Alice\"}", 1234567890),
                                       memgraph::Node(2, {"Person"}, "{\"name\": \"Bob\"}", 1234567891),
                                       memgraph::Node(3, {"Person", "Employee"},
                                                      "{\"name\": \"Charlie\", \"role\": \"Developer\", "
                                                      "\"company\":\"Large Corporation\"}",
                                                      1234567892)};
  auto maybe_npage = node_schema::Make(nodes, ::arrow::default_memory_pool());
  if (!maybe_npage.ok()) {
    spdlog::error("{}", maybe_npage.status().ToString());
    return 1;
  }
  std::shared_ptr<::arrow::Table> npage = *maybe_npage;
  (void)npage_arrow::PrintPage(npage);

  std::vector<memgraph::Edge> edges = {
      memgraph::Edge(1, 1, 2, "KNOWS", "{\"since\": 2020}", 1234567890),
      memgraph::Edge(2, 2, 3, "WORKS_WITH", "{\"project\": \"Graph Database\"}", 1234567891)};
  auto maybe_epage = edge_schema::Make(edges, ::arrow::default_memory_pool());
  if (!maybe_epage.ok()) {
    spdlog::error("{}", maybe_epage.status().ToString());
    return 1;
  }
  std::shared_ptr<::arrow::Table> epage = *maybe_epage;
  (void)epage_arrow::PrintPage(epage);

  constexpr char kNPagePath[] = "npage.arrow";
  constexpr char kEPagePath[] = "epage.arrow";
  if (!memgraph::arrow::WriteTableToFile(kNPagePath, npage).ok()) {
    spdlog::error("Failed to write file");
    return 1;
  }
  if (!memgraph::arrow::WriteTableToFile(kEPagePath, epage).ok()) {
    spdlog::error("Failed to write file");
    return 1;
  }
  if (!npage_arrow::ReadAndPrint(kNPagePath).ok()) {
    spdlog::error("Failed to read file");
    return 1;
  }
  if (!epage_arrow::ReadAndPrint(kEPagePath).ok()) {
    spdlog::error("Failed to read file");
    return 1;
  }

  spdlog::info("Write+Read NPage <> RocksDB");
  // Write npage to RocksDB
  auto npage_buffer = memgraph::arrow::SerializeTable(npage);
  if (!npage_buffer.ok()) {
    spdlog::error("Failed to serialize npage: {}", npage_buffer.status().ToString());
    return 1;
  }
  if (!experimental::rocks::PutArrowBuffer(kDBName, "npage", *npage_buffer).ok()) {
    spdlog::error("Failed to write npage to RocksDB");
    return 1;
  }

  auto maybe_npage_from_rocks = experimental::rocks::GetArrowTable(kDBName, "npage");
  if (!maybe_npage_from_rocks.ok()) {
    spdlog::error("{}", maybe_npage_from_rocks.status().ToString());
    return 1;
  }
  auto npage_from_rocks = *maybe_npage_from_rocks;
  (void)npage_arrow::PrintPage(npage_from_rocks);

  spdlog::info("Write+Read EPage <> RocksDB");
  // Write epage to RocksDB
  auto epage_buffer = memgraph::arrow::SerializeTable(epage);
  if (!epage_buffer.ok()) {
    spdlog::error("Failed to serialize epage: {}", epage_buffer.status().ToString());
    return 1;
  }
  if (!experimental::rocks::PutArrowBuffer(kDBName, "epage", *epage_buffer).ok()) {
    spdlog::error("Failed to write epage to RocksDB");
    return 1;
  }

  auto maybe_epage_from_rocks = experimental::rocks::GetArrowTable(kDBName, "epage");
  if (!maybe_epage_from_rocks.ok()) {
    spdlog::error("{}", maybe_epage_from_rocks.status().ToString());
    return 1;
  }
  auto epage_from_rocks = *maybe_epage_from_rocks;
  (void)epage_arrow::PrintPage(epage_from_rocks);

  memgraph::experimental::rocks::ReverseIndex index{"reverse_index"};
  index.AddChunkId('N', 1, 1, 1);
  index.AddChunkIds('E', 1, 1, {3, 4});
  auto nodes_reverse_index = index.GetChunkIds('N', 1, 1);
  auto edges_reverse_index = index.GetChunkIds('E', 1, 1);

  // Join chunk IDs for logging
  std::stringstream nodes_ss;
  for (const auto &chunk_id : nodes_reverse_index) {
    nodes_ss << chunk_id << " ";
  }
  spdlog::info("Node reverse index for entity 1, txid 1: {}", nodes_ss.str());

  std::stringstream edges_ss;
  for (const auto &chunk_id : edges_reverse_index) {
    edges_ss << chunk_id << " ";
  }
  spdlog::info("Edge reverse index for entity 1, txid 1: {}", edges_ss.str());

  memgraph::Graph graph("main_quick_test_graph", PageType::PARQUET, 100);
  if (!graph.AddNodes(nodes).ok())
    return 1;
  if (!graph.AddEdges(edges).ok())
    return 1;

  // Create a transactional graph
  memgraph::TransactionalGraph tx_graph(graph, 2);

  spdlog::trace("Graph batch size: {}", graph.GetPageSize());
  spdlog::trace("Transactional graph batch size: {}", tx_graph.GetPageSize());

  // Start first transaction
  auto tx1_result = tx_graph.BeginTransaction(memgraph::IsolationLevel::READ_UNCOMMITTED);
  if (!tx1_result.ok()) {
    spdlog::error("Error beginning transaction: {}", tx1_result.status().ToString());
    return 1;
  }
  auto tx1 = std::move(tx1_result).ValueOrDie();

  // Start second transaction
  auto tx2_result = tx_graph.BeginTransaction(memgraph::IsolationLevel::READ_UNCOMMITTED);
  if (!tx2_result.ok()) {
    spdlog::error("Error beginning transaction: {}", tx2_result.status().ToString());
    return 1;
  }
  auto tx2 = std::move(tx2_result).ValueOrDie();

  // Add nodes in first transaction
  std::vector<memgraph::Node> tx1_nodes = {memgraph::Node(1, {"Person"}, "{\"name\": \"Alice\", \"age\": 30}"),
                                           memgraph::Node(2, {"Person"}, "{\"name\": \"Bob\", \"age\": 25}")};
  auto status = tx1->AddNodes(tx1_nodes);
  if (!status.ok()) {
    spdlog::error("Error adding nodes in transaction {}", tx1->GetTransactionId());
    return 1;
  }

  // Add nodes in second transaction
  std::vector<memgraph::Node> tx2_nodes = {memgraph::Node(101, {"Person"}, "{\"name\": \"Charlie\", \"age\": 35}"),
                                           memgraph::Node(102, {"Person"}, "{\"name\": \"David\", \"age\": 40}")};
  status = tx2->AddNodes(tx2_nodes);
  if (!status.ok()) {
    spdlog::error("Error adding nodes in transaction {}", tx2->GetTransactionId());
    return 1;
  }

  // Add edges in first transaction
  std::vector<memgraph::Edge> tx1_edges = {memgraph::Edge(101, 1, 2, "KNOWS", "{\"since\": 2020}", 1000)};
  status = tx1->AddEdges(tx1_edges);
  if (!status.ok()) {
    spdlog::error("Error adding edges in transaction {}", tx1->GetTransactionId());
    return 1;
  }

  // Add edges in second transaction
  std::vector<memgraph::Edge> tx2_edges = {memgraph::Edge(201, 3, 4, "KNOWS", "{\"since\": 2021}", 1001)};
  status = tx2->AddEdges(tx2_edges);
  if (!status.ok()) {
    spdlog::error("Error adding edges in transaction {}", tx2->GetTransactionId());
    return 1;
  }

  // Verify data in first transaction
  auto nodes_result1 = tx1->GetNodes(0, 1000);
  if (!nodes_result1.ok()) {
    spdlog::error("Error getting nodes in transaction {}", tx1->GetTransactionId());
    return 1;
  }
  auto nodes1 = nodes_result1.ValueOrDie();
  std::cout << "Transaction " << tx1->GetTransactionId() << " nodes: " << nodes1.size() << std::endl;

  // Verify data in second transaction
  auto nodes_result2 = tx2->GetNodes(0, 1000);
  if (!nodes_result2.ok()) {
    spdlog::error("Error getting nodes in transaction {}", tx2->GetTransactionId());
    return 1;
  }
  auto nodes2 = nodes_result2.ValueOrDie();
  std::cout << "Transaction " << tx2->GetTransactionId() << " nodes: " << nodes2.size() << std::endl;

  // Commit first transaction
  status = tx1->Commit();
  if (!status.ok()) {
    spdlog::error("Error committing transaction {}", tx1->GetTransactionId());
    return 1;
  }

  // Commit second transaction
  status = tx2->Commit();
  if (!status.ok()) {
    spdlog::error("Error committing transaction {}", tx2->GetTransactionId());
    return 1;
  }

  return 0;
}
