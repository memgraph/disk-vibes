#include <filesystem>
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <set>
#include <thread>

#include "storage/graph.hpp"
#include "storage/graph_transaction.hpp"
#include "storage/isolation_level.hpp"

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

// Global test result tracker
class TestResultTracker {
public:
  static TestResultTracker &GetInstance() {
    static TestResultTracker instance;
    return instance;
  }

  void RecordTestResult(const std::string &test_name, memgraph::IsolationLevel level, bool passed) {
    results_[test_name][level] = passed;
  }

  void PrintResults() {
    std::cout << "\nIsolation Level Anomaly Test Results:\n";
    std::cout << "=====================================\n";

    // Print header
    std::cout << std::left << std::setw(15) << "Test Case"
              << "|";
    std::set<memgraph::IsolationLevel> levels;
    for (const auto &[test_name, level_results] : results_) {
      for (const auto &[level, _] : level_results) {
        levels.insert(level);
      }
    }
    for (const auto &level : levels) {
      std::cout << std::setw(20) << IsolationLevelToString(level) << "|";
    }
    std::cout << "\n";

    // Print separator
    std::cout << std::string(15, '-') << "+";
    for (size_t i = 0; i < levels.size(); ++i) {
      std::cout << std::string(20, '-') << "+";
    }
    std::cout << "\n";

    // Print results
    for (const auto &[test_name, level_results] : results_) {
      std::cout << std::left << std::setw(15) << test_name << "|";
      for (const auto &level : levels) {
        auto it = level_results.find(level);
        if (it != level_results.end()) {
          std::cout << std::setw(20) << (it->second ? "PASS" : "FAIL") << "|";
        } else {
          std::cout << std::setw(20) << "N/A"
                    << "|";
        }
      }
      std::cout << "\n";
    }
    std::cout << "\n";
  }

private:
  TestResultTracker() = default;
  std::map<std::string, std::map<memgraph::IsolationLevel, bool>> results_;
};

class HermitageTest : public ::testing::TestWithParam<memgraph::IsolationLevel> {
protected:
  void SetUp() override {
    // Create temporary directory for test data
    test_dir_ = std::filesystem::temp_directory_path() / "hermitage_test";
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);

    // Create base graph
    graph_ = std::make_unique<memgraph::Graph>(test_dir_, memgraph::PageType::ARROW, 1);
    tx_graph_ = std::make_unique<memgraph::TransactionalGraph>(*graph_);
    auto tx_result = tx_graph_->BeginTransaction(GetParam());
    if (!tx_result.ok()) {
      spdlog::error("Error beginning transaction: {}", tx_result.status().ToString());
      return;
    }
    auto tx = std::move(tx_result).ValueOrDie();
    spdlog::info("Initial transaction started with isolation level: {}", IsolationLevelToString(GetParam()));

    // Initialize test data
    std::vector<memgraph::Node> initial_nodes = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 10}"),
                                                 memgraph::Node(2, {"Kv"}, "{\"key\": 2, \"value\": 20}")};
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
TEST_P(HermitageTest, G0) {
  bool test_passed = true;
  try {
    // Start first transaction
    auto tx1_result = tx_graph_->BeginTransaction(GetParam());
    ASSERT_TRUE(tx1_result.ok());
    auto tx1 = std::move(tx1_result).ValueOrDie();
    spdlog::info("Transaction 1 started with isolation level: {}", IsolationLevelToString(GetParam()));

    // Start second transaction
    auto tx2_result = tx_graph_->BeginTransaction(GetParam());
    ASSERT_TRUE(tx2_result.ok());
    auto tx2 = std::move(tx2_result).ValueOrDie();
    spdlog::info("Transaction 2 started with isolation level: {}", IsolationLevelToString(GetParam()));

    // First transaction writes to key 1
    std::vector<memgraph::Node> nodes1 = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 11}")};
    ASSERT_TRUE(tx1->AddNodes(nodes1).ok());

    // Second transaction tries to write to key 1 (should fail)
    std::vector<memgraph::Node> nodes2 = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 12}")};
    ASSERT_FALSE(tx2->AddNodes(nodes2).ok());

    // First transaction writes to key 2
    std::vector<memgraph::Node> nodes3 = {memgraph::Node(2, {"Kv"}, "{\"key\": 2, \"value\": 21}")};
    ASSERT_TRUE(tx1->AddNodes(nodes3).ok());

    // Commit first transaction
    ASSERT_TRUE(tx1->Commit().ok());

    // Start another transaction after tx1 and tx2 should see 1:11 and 2:21
    auto tx12_result = tx_graph_->BeginTransaction(GetParam());
    ASSERT_TRUE(tx12_result.ok());
    auto tx12 = std::move(tx12_result).ValueOrDie();
    spdlog::info("Transaction 12 started with isolation level: {}", IsolationLevelToString(GetParam()));
    auto result12 = tx12->GetNodes(1, 3);
    ASSERT_TRUE(result12.ok());
    auto nodes = result12.ValueOrDie();
    ASSERT_EQ(nodes.size(), 2);
    ASSERT_EQ(nodes[0].props, "{\"key\": 1, \"value\": 11}");
    ASSERT_EQ(nodes[1].props, "{\"key\": 2, \"value\": 21}");

    // Second transaction should see the committed values
    auto result2 = tx2->GetNodes(1, 3);
    ASSERT_TRUE(result2.ok());
    nodes = result2.ValueOrDie();
    ASSERT_EQ(nodes.size(), 2);
    ASSERT_EQ(nodes[0].props, "{\"key\": 1, \"value\": 11}");
    ASSERT_EQ(nodes[1].props, "{\"key\": 2, \"value\": 21}");

    // Second transaction should also be able to commit
    ASSERT_TRUE(tx12->Commit().ok());
    ASSERT_TRUE(tx2->Commit().ok());
  } catch (const std::exception &e) {
    test_passed = false;
  }
  TestResultTracker::GetInstance().RecordTestResult("G0", GetParam(), test_passed);
}

// G1a: Dirty Reads
// One transaction reads a value that was written by another uncommitted transaction
TEST_P(HermitageTest, G1a) {
  bool test_passed = true;
  try {
    // Start first transaction
    auto tx1_result = tx_graph_->BeginTransaction(GetParam());
    ASSERT_TRUE(tx1_result.ok());
    auto tx1 = std::move(tx1_result).ValueOrDie();
    spdlog::info("Transaction 1 started with isolation level: {}", IsolationLevelToString(GetParam()));

    // Start second transaction
    auto tx2_result = tx_graph_->BeginTransaction(GetParam());
    ASSERT_TRUE(tx2_result.ok());
    auto tx2 = std::move(tx2_result).ValueOrDie();
    spdlog::info("Transaction 2 started with isolation level: {}", IsolationLevelToString(GetParam()));

    // First transaction updates key 1
    std::vector<memgraph::Node> nodes1 = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 101}")};
    ASSERT_TRUE(tx1->AddNodes(nodes1).ok());

    // Second transaction reads key 1 (should see initial value)
    auto result2 = tx2->GetNodes(1, 2);
    ASSERT_TRUE(result2.ok());
    auto nodes = result2.ValueOrDie();
    ASSERT_EQ(nodes.size(), 1);
    ASSERT_EQ(nodes[0].props, "{\"key\": 1, \"value\": 10}");

    // First transaction aborts
    ASSERT_TRUE(tx1->Abort().ok());

    // Second transaction reads key 1 again (should still see original value 10)
    result2 = tx2->GetNodes(1, 2);
    ASSERT_TRUE(result2.ok());
    nodes = result2.ValueOrDie();
    ASSERT_EQ(nodes.size(), 1);
    ASSERT_EQ(nodes[0].props, "{\"key\": 1, \"value\": 10}");

    // Second transaction commits
    ASSERT_TRUE(tx2->Commit().ok());
  } catch (const std::exception &e) {
    test_passed = false;
  }
  TestResultTracker::GetInstance().RecordTestResult("G1a", GetParam(), test_passed);
}

// G1b: Intermediate Reads (dirty reads)
// One transaction reads a value that was written by another transaction before it commits
TEST_P(HermitageTest, G1b) {
  bool test_passed = true;
  try {
    // Start first transaction
    auto tx1_result = tx_graph_->BeginTransaction(GetParam());
    ASSERT_TRUE(tx1_result.ok());
    auto tx1 = std::move(tx1_result).ValueOrDie();
    spdlog::info("Transaction 1 started with isolation level: {}", IsolationLevelToString(GetParam()));

    // Start second transaction
    auto tx2_result = tx_graph_->BeginTransaction(GetParam());
    ASSERT_TRUE(tx2_result.ok());
    auto tx2 = std::move(tx2_result).ValueOrDie();
    spdlog::info("Transaction 2 started with isolation level: {}", IsolationLevelToString(GetParam()));

    // First transaction updates key 1
    std::vector<memgraph::Node> nodes1 = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 101}")};
    ASSERT_TRUE(tx1->AddNodes(nodes1).ok());

    // Second transaction reads key 1 (should see initial value)
    auto result2 = tx2->GetNodes(1, 2);
    ASSERT_TRUE(result2.ok());
    auto nodes = result2.ValueOrDie();
    ASSERT_EQ(nodes.size(), 1);
    ASSERT_EQ(nodes[0].props, "{\"key\": 1, \"value\": 10}");

    // First transaction updates key 1 again
    std::vector<memgraph::Node> nodes2 = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 11}")};
    ASSERT_TRUE(tx1->AddNodes(nodes2).ok());

    // First transaction commits
    ASSERT_TRUE(tx1->Commit().ok());

    // Second transaction reads key 1 again
    result2 = tx2->GetNodes(1, 2);
    ASSERT_TRUE(result2.ok());
    nodes = result2.ValueOrDie();
    ASSERT_EQ(nodes.size(), 1);

    // For READ_COMMITTED, should see the committed value 11
    // For READ_UNCOMMITTED, should see the initial value 10
    ASSERT_EQ(nodes[0].props, "{\"key\": 1, \"value\": 11}");

    // Second transaction commits
    ASSERT_TRUE(tx2->Commit().ok());
  } catch (const std::exception &e) {
    test_passed = false;
  }
  TestResultTracker::GetInstance().RecordTestResult("G1b", GetParam(), test_passed);
}

// G1c: Circular Information Flow (dirty reads)
// Two transactions read each other's uncommitted writes
TEST_P(HermitageTest, G1c) {
  bool test_passed = true;
  try {
    // Start first transaction
    auto tx1_result = tx_graph_->BeginTransaction(GetParam());
    ASSERT_TRUE(tx1_result.ok());
    auto tx1 = std::move(tx1_result).ValueOrDie();
    spdlog::info("Transaction 1 started with isolation level: {}", IsolationLevelToString(GetParam()));

    // Start second transaction
    auto tx2_result = tx_graph_->BeginTransaction(GetParam());
    ASSERT_TRUE(tx2_result.ok());
    auto tx2 = std::move(tx2_result).ValueOrDie();
    spdlog::info("Transaction 2 started with isolation level: {}", IsolationLevelToString(GetParam()));

    // First transaction updates key 1
    std::vector<memgraph::Node> nodes1 = {memgraph::Node(1, {"Kv"}, "{\"key\": 1, \"value\": 11}")};
    ASSERT_TRUE(tx1->AddNodes(nodes1).ok());

    // Second transaction updates key 2
    std::vector<memgraph::Node> nodes2 = {memgraph::Node(2, {"Kv"}, "{\"key\": 2, \"value\": 22}")};
    ASSERT_TRUE(tx2->AddNodes(nodes2).ok());

    // First transaction reads key 2 (should see initial value)
    auto result1 = tx1->GetNodes(2, 3);
    ASSERT_TRUE(result1.ok());
    auto nodes = result1.ValueOrDie();
    ASSERT_EQ(nodes.size(), 1);
    ASSERT_EQ(nodes[0].props, "{\"key\": 2, \"value\": 20}");

    // Second transaction reads key 1 (should see initial value)
    auto result2 = tx2->GetNodes(1, 2);
    ASSERT_TRUE(result2.ok());
    nodes = result2.ValueOrDie();
    ASSERT_EQ(nodes.size(), 1);
    ASSERT_EQ(nodes[0].props, "{\"key\": 1, \"value\": 10}");

    // Both transactions commit
    ASSERT_TRUE(tx1->Commit().ok());
    ASSERT_TRUE(tx2->Commit().ok());
  } catch (const std::exception &e) {
    test_passed = false;
  }
  TestResultTracker::GetInstance().RecordTestResult("G1c", GetParam(), test_passed);
}

INSTANTIATE_TEST_SUITE_P(IsolationLevels, HermitageTest,
                         ::testing::Values(memgraph::IsolationLevel::READ_UNCOMMITTED,
                                           memgraph::IsolationLevel::READ_COMMITTED),
                         [](const ::testing::TestParamInfo<memgraph::IsolationLevel> &info) {
                           switch (info.param) {
                           case memgraph::IsolationLevel::READ_UNCOMMITTED:
                             return "ReadUncommitted";
                           case memgraph::IsolationLevel::READ_COMMITTED:
                             return "ReadCommitted";
                           default:
                             return "Unknown";
                           }
                         });

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();
  TestResultTracker::GetInstance().PrintResults();
  return result;
}