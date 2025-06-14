#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <thread>

#include "storage/graph.hpp"
#include "storage/graph_transaction.hpp"
#include "storage/isolation_level.hpp"

// isolation level  | G0  |
// -----------------|-----|
// no isolation     |  -  |
// read uncommitted |  +  |

class HermitageTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create temporary directory for test data
    test_dir_ = std::filesystem::temp_directory_path() / "hermitage_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    // Create base graph
    graph_ = std::make_unique<memgraph::Graph>(test_dir_, memgraph::PageType::ARROW, 1000);
    tx_graph_ = std::make_unique<memgraph::TransactionalGraph>(*graph_);
    auto tx_result = tx_graph_->BeginTransaction(memgraph::IsolationLevel::READ_UNCOMMITTED);
    if (!tx_result.ok()) {
      spdlog::error("Error beginning transaction: {}", tx_result.status().ToString());
      return;
    }
    auto tx = std::move(tx_result).ValueOrDie();

    // Initialize test data
    std::vector<memgraph::Node> initial_nodes = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 11}")};
    auto status = tx->AddNodes(initial_nodes);
    if (!status.ok()) {
      spdlog::error("Error adding nodes: {}", status.ToString());
      return;
    }
    status = tx->Commit();
    if (!status.ok()) {
      spdlog::error("Error committing transaction: {}", status.ToString());
      return;
    }
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::filesystem::path test_dir_;
  std::unique_ptr<memgraph::Graph> graph_;
  std::unique_ptr<memgraph::TransactionalGraph> tx_graph_;
};

// G0: Write Cycles (dirty writes)
// Two transactions try to write to the same key
// TODO(gitbuda): Test
TEST_F(HermitageTest, G0) {
  // Start first transaction
  auto tx1_result = tx_graph_->BeginTransaction(memgraph::IsolationLevel::READ_UNCOMMITTED);
  ASSERT_TRUE(tx1_result.ok());
  auto tx1 = std::move(tx1_result).ValueOrDie();

  // Start second transaction
  auto tx2_result = tx_graph_->BeginTransaction(memgraph::IsolationLevel::READ_UNCOMMITTED);
  ASSERT_TRUE(tx2_result.ok());
  auto tx2 = std::move(tx2_result).ValueOrDie();

  // First transaction writes to key 1
  std::vector<memgraph::Node> nodes1 = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 11}")};
  ASSERT_TRUE(tx1->AddNodes(nodes1).ok());

  // Second transaction tries to write to key 1 (should fail)
  std::vector<memgraph::Node> nodes2 = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 12}")};
  // NOTE: If the next line succeeded (staging of changes done) -> there should be an error on commit.
  ASSERT_FALSE(tx2->AddNodes(nodes2).ok());
  // TODO(gitbuda): After this happens, the tx2 should automatically be aborted (not yet implemented).

  // First transaction writes to key 2
  std::vector<memgraph::Node> nodes3 = {memgraph::Node(2, {"Kv"}, "{\"key\": 2, \"value\": 21}")};
  ASSERT_TRUE(tx1->AddNodes(nodes3).ok());

  // Add edges in first transaction
  std::vector<memgraph::Edge> edges1 = {memgraph::Edge(1, 1, 2, "CONNECTS", "{\"weight\": 1.0}", 1000)};
  ASSERT_TRUE(tx1->AddEdges(edges1).ok());

  // Add edges in second transaction
  std::vector<memgraph::Edge> edges2 = {memgraph::Edge(2, 2, 3, "CONNECTS", "{\"weight\": 2.0}", 1001)};
  ASSERT_FALSE(tx2->AddEdges(edges2).ok());

  // Commit first transaction
  ASSERT_TRUE(tx1->Commit().ok());

  // Start another transaction after tx1 and tx2 should see 1:11 and 2:21 (uncommitted one)
  auto tx12_result = tx_graph_->BeginTransaction(memgraph::IsolationLevel::READ_UNCOMMITTED);
  ASSERT_TRUE(tx12_result.ok());
  auto tx12 = std::move(tx12_result).ValueOrDie();
  auto result12 = tx12->GetNodes(1, 3);
  ASSERT_TRUE(result12.ok());
  auto nodes = result12.ValueOrDie();
  ASSERT_EQ(nodes.size(), 2);
  ASSERT_EQ(nodes[0].props, "{\"key\": 1, \"value\": 11}");
  ASSERT_EQ(nodes[1].props, "{\"key\": 2, \"value\": 21}");

  // Second transaction should see the commited value and own uncommitted value
  auto result2 = tx2->GetNodes(1, 3);
  ASSERT_TRUE(result2.ok());
  nodes = result2.ValueOrDie();
  ASSERT_EQ(nodes.size(), 2);
  ASSERT_EQ(nodes[0].props, "{\"key\": 1, \"value\": 11}");
  ASSERT_EQ(nodes[1].props, "{\"key\": 2, \"value\": 21}");

  // Second transaction should also be able to commit. NOTE: The success of
  // commit call might be different depending on
  // optimistic(staging+failing_later)/pessimistic (early failing).
  ASSERT_TRUE(tx1->Commit().ok());
  ASSERT_TRUE(tx12->Commit().ok());
  ASSERT_TRUE(tx2->Commit().ok());
}