#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

#include "edge.hpp"
#include "graph.hpp"
#include "graph_transaction.hpp"
#include "isolation_level.hpp"

namespace memgraph {
namespace {

using Node = memgraph::Node;

class GraphTransactionTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create temporary directory for test data
    // test_dir_ = std::filesystem::temp_directory_path() / "graph_transaction_test";
    test_dir_ = std::filesystem::path("graph_transaction_test_data");
    // std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    // Clean up test directories
  }

  std::filesystem::path GetTestDir(const std::string &test_name) {
    auto test_path = test_dir_ / test_name;
    std::filesystem::remove_all(test_path);
    std::filesystem::create_directories(test_path);
    return test_path;
  }

  std::filesystem::path test_dir_;
};

TEST_F(GraphTransactionTest, BasicNodeOperations) {
  auto test_path = GetTestDir("basic_node_operations");
  Graph graph(test_path, PageType::ARROW, 1000); // batch size of 1000
  TransactionalGraph tx_graph(graph);
  auto tx_result = tx_graph.BeginTransaction(IsolationLevel::READ_UNCOMMITTED);
  ASSERT_TRUE(tx_result.ok());
  auto tx = std::move(tx_result.ValueOrDie());

  std::vector<Node> nodes = {Node(1, {"Node1"}, "{\"prop1\": \"value1\"}"),
                             Node(2, {"Node2"}, "{\"prop2\": \"value2\"}"),
                             Node(3, {"Node3"}, "{\"prop3\": \"value3\"}")};
  ASSERT_TRUE(tx->AddNodes(nodes).ok());

  auto result = tx->GetNodes(1, 4);
  ASSERT_TRUE(result.ok());
  auto retrieved_nodes = result.ValueOrDie();
  ASSERT_EQ(retrieved_nodes.size(), 3);

  // Delete a node
  ASSERT_TRUE(tx->DeleteNodes({2}).ok());

  // Verify node was deleted
  result = tx->GetNodes(1, 4);
  ASSERT_TRUE(result.ok());
  retrieved_nodes = result.ValueOrDie();
  ASSERT_EQ(retrieved_nodes.size(), 2);

  // Commit the transaction
  ASSERT_TRUE(tx->Commit().ok());
}

TEST_F(GraphTransactionTest, BasicEdgeOperations) {
  auto test_path = GetTestDir("basic_edge_operations");
  Graph graph(test_path, PageType::ARROW, 1000);
  TransactionalGraph tx_graph(graph);

  auto tx_result = tx_graph.BeginTransaction(IsolationLevel::READ_UNCOMMITTED);
  ASSERT_TRUE(tx_result.ok());
  auto tx = std::move(tx_result.ValueOrDie());

  // Add some edges
  std::vector<memgraph::Edge> edges = {memgraph::Edge(1, 1, 2, "RELATES_TO", "{\"weight\": 1.0}"),
                                       memgraph::Edge(2, 2, 3, "FOLLOWS", "{\"since\": 2024}"),
                                       memgraph::Edge(3, 3, 1, "KNOWS", "{\"strength\": \"high\"}")};

  ASSERT_TRUE(tx->AddEdges(edges).ok());

  // Verify edges were added
  auto result = tx->GetEdges(1, 4);
  ASSERT_TRUE(result.ok());
  auto retrieved_edges = result.ValueOrDie();
  ASSERT_EQ(retrieved_edges.size(), 3);

  // Delete an edge
  ASSERT_TRUE(tx->DeleteEdges({2}).ok());

  // Verify edge was deleted
  result = tx->GetEdges(1, 4);
  ASSERT_TRUE(result.ok());
  retrieved_edges = result.ValueOrDie();
  ASSERT_EQ(retrieved_edges.size(), 2);

  // Commit the transaction
  ASSERT_TRUE(tx->Commit().ok());
}

TEST_F(GraphTransactionTest, TransactionAbort) {
  auto test_path = GetTestDir("transaction_abort");
  Graph graph(test_path, PageType::ARROW, 1000);
  TransactionalGraph tx_graph(graph);
  auto tx_result = tx_graph.BeginTransaction(IsolationLevel::READ_UNCOMMITTED);
  ASSERT_TRUE(tx_result.ok());
  auto tx = std::move(tx_result.ValueOrDie());

  // Add some initial data
  std::vector<Node> initial_nodes = {Node(1, {"InitialNode"}, "{\"prop\": \"value\"}")};
  ASSERT_TRUE(tx->AddNodes(initial_nodes).ok());
  // Verify the new node is visible within the transaction
  auto result = tx->GetNodes(1, 3);
  ASSERT_TRUE(result.ok());
  auto retrieved_nodes = result.ValueOrDie();
  ASSERT_EQ(retrieved_nodes.size(), 1);

  // Abort the transaction
  ASSERT_TRUE(tx->Abort().ok());
  // Verify that reading from an aborted transaction returns an error
  result = tx->GetNodes(1, 3);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.status().ToString(), "Invalid: Transaction has been aborted");
}

TEST_F(GraphTransactionTest, NoIsolationLevel) {
  auto test_path = GetTestDir("no_isolation_level");
  Graph graph(test_path, PageType::ARROW, 1000);
  TransactionalGraph tx_graph(graph);

  // Begin a transaction with NO_ISOLATION
  auto tx_result = tx_graph.BeginTransaction(IsolationLevel::NO_ISOLATION);
  ASSERT_TRUE(tx_result.ok());
  auto tx = std::move(tx_result.ValueOrDie());

  // Add some nodes
  std::vector<Node> nodes = {Node(1, {"Node1"}, "{\"prop1\": \"value1\"}"),
                             Node(2, {"Node2"}, "{\"prop2\": \"value2\"}")};

  ASSERT_TRUE(tx->AddNodes(nodes).ok());

  // Verify nodes were added
  auto result = tx->GetNodes(1, 3);
  ASSERT_TRUE(result.ok());
  auto retrieved_nodes = result.ValueOrDie();
  ASSERT_EQ(retrieved_nodes.size(), 2);

  // Commit should be a no-op with NO_ISOLATION
  ASSERT_TRUE(tx->Commit().ok());
}

TEST_F(GraphTransactionTest, PageSizeConsistency) {
  auto test_path = GetTestDir("page_size_consistency");
  const size_t expected_page_size = 1000;
  Graph graph(test_path, PageType::ARROW, expected_page_size);
  TransactionalGraph tx_graph(graph);

  // Verify that both Graph and TransactionalGraph return the same page size
  ASSERT_EQ(graph.GetPageSize(), expected_page_size);
  ASSERT_EQ(tx_graph.GetPageSize(), expected_page_size);
}

} // namespace
} // namespace memgraph

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}