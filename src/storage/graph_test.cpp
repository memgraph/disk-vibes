#include <filesystem>
#include <vector>

#include <gtest/gtest.h>

#include "edge.hpp"
#include "graph.hpp"
#include "node.hpp"

using namespace memgraph;
using Node = memgraph::Node;

class GraphTest : public ::testing::Test {
protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() / "graph_test";
    std::filesystem::create_directories(test_dir_);
  }
  void TearDown() override { std::filesystem::remove_all(test_dir_); }
  std::filesystem::path test_dir_;
};

TEST_F(GraphTest, AddAndRetrieveNodes) {
  Graph graph(test_dir_, PageType::ARROW);
  std::vector<Node> nodes = {Node(1, {"Person"}, "{\"name\":\"Alice\",\"age\":30}", 1000),
                             Node(2, {"Person"}, "{\"name\":\"Bob\",\"age\":25}", 1001),
                             Node(3, {"Person", "Employee"}, "{\"name\":\"Charlie\",\"age\":35}", 1002)};
  ASSERT_TRUE(graph.AddNodes(nodes).ok());

  auto result = graph.GetNodes(1, 10);
  ASSERT_TRUE(result.ok());
  auto retrieved_nodes = result.ValueOrDie();

  ASSERT_EQ(retrieved_nodes.size(), 3);
  EXPECT_EQ(retrieved_nodes[0].id, 1);
  EXPECT_EQ(retrieved_nodes[0].labels[0], "Person");
  EXPECT_EQ(retrieved_nodes[0].props, "{\"name\":\"Alice\",\"age\":30}");
  EXPECT_EQ(retrieved_nodes[0].ts, 1000);

  EXPECT_EQ(retrieved_nodes[1].id, 2);
  EXPECT_EQ(retrieved_nodes[1].labels[0], "Person");
  EXPECT_EQ(retrieved_nodes[1].props, "{\"name\":\"Bob\",\"age\":25}");
  EXPECT_EQ(retrieved_nodes[1].ts, 1001);

  EXPECT_EQ(retrieved_nodes[2].id, 3);
  EXPECT_EQ(retrieved_nodes[2].labels.size(), 2);
  EXPECT_EQ(retrieved_nodes[2].labels[0], "Person");
  EXPECT_EQ(retrieved_nodes[2].labels[1], "Employee");
  EXPECT_EQ(retrieved_nodes[2].props, "{\"name\":\"Charlie\",\"age\":35}");
  EXPECT_EQ(retrieved_nodes[2].ts, 1002);
}

TEST_F(GraphTest, AddAndRetrieveEdges) {
  Graph graph(test_dir_, PageType::ARROW);
  std::vector<memgraph::Edge> edges = {memgraph::Edge(1, 1, 2, "KNOWS", "{\"since\":2020}", 1000),
                                       memgraph::Edge(2, 2, 3, "WORKS_WITH", "{\"project\":\"X\"}", 1001),
                                       memgraph::Edge(3, 3, 1, "REPORTS_TO", "{\"department\":\"IT\"}", 1002)};
  ASSERT_TRUE(graph.AddEdges(edges).ok());

  auto result = graph.GetEdges(1, 10);
  ASSERT_TRUE(result.ok());
  auto retrieved_edges = result.ValueOrDie();

  ASSERT_EQ(retrieved_edges.size(), 3);
  EXPECT_EQ(retrieved_edges[0].id, 1);
  EXPECT_EQ(retrieved_edges[0].src, 1);
  EXPECT_EQ(retrieved_edges[0].dst, 2);
  EXPECT_EQ(retrieved_edges[0].type, "KNOWS");
  EXPECT_EQ(retrieved_edges[0].props, "{\"since\":2020}");
  EXPECT_EQ(retrieved_edges[0].ts, 1000);

  EXPECT_EQ(retrieved_edges[1].id, 2);
  EXPECT_EQ(retrieved_edges[1].src, 2);
  EXPECT_EQ(retrieved_edges[1].dst, 3);
  EXPECT_EQ(retrieved_edges[1].type, "WORKS_WITH");
  EXPECT_EQ(retrieved_edges[1].props, "{\"project\":\"X\"}");
  EXPECT_EQ(retrieved_edges[1].ts, 1001);

  EXPECT_EQ(retrieved_edges[2].id, 3);
  EXPECT_EQ(retrieved_edges[2].src, 3);
  EXPECT_EQ(retrieved_edges[2].dst, 1);
  EXPECT_EQ(retrieved_edges[2].type, "REPORTS_TO");
  EXPECT_EQ(retrieved_edges[2].props, "{\"department\":\"IT\"}");
  EXPECT_EQ(retrieved_edges[2].ts, 1002);
}

TEST_F(GraphTest, DeleteNodes) {
  Graph graph(test_dir_);
  std::vector<Node> nodes = {Node(1, {"Person"}, "{\"name\":\"Alice\"}", 1000),
                             Node(2, {"Person"}, "{\"name\":\"Bob\"}", 1001),
                             Node(3, {"Person"}, "{\"name\":\"Charlie\"}", 1002)};
  ASSERT_TRUE(graph.AddNodes(nodes).ok());
  ASSERT_TRUE(graph.DeleteNodes({2}).ok());

  auto result = graph.GetNodes(1, 10);
  ASSERT_TRUE(result.ok());
  auto retrieved_nodes = result.ValueOrDie();

  ASSERT_EQ(retrieved_nodes.size(), 2);
  EXPECT_EQ(retrieved_nodes[0].id, 1);
  EXPECT_EQ(retrieved_nodes[1].id, 3);
}

TEST_F(GraphTest, DeleteEdges) {
  Graph graph(test_dir_);
  std::vector<memgraph::Edge> edges = {memgraph::Edge(1, 1, 2, "KNOWS", "{}", 1000),
                                       memgraph::Edge(2, 2, 3, "KNOWS", "{}", 1001),
                                       memgraph::Edge(3, 3, 1, "KNOWS", "{}", 1002)};
  ASSERT_TRUE(graph.AddEdges(edges).ok());
  ASSERT_TRUE(graph.DeleteEdges({2}).ok());

  auto result = graph.GetEdges(1, 4);
  ASSERT_TRUE(result.ok());
  auto retrieved_edges = result.ValueOrDie();

  ASSERT_EQ(retrieved_edges.size(), 2);
  EXPECT_EQ(retrieved_edges[0].id, 1);
  EXPECT_EQ(retrieved_edges[1].id, 3);
}

TEST_F(GraphTest, IndexLookupByLabelAndProperty) {
  // Create index configurations - only index Person.name and Company.industry
  std::vector<memgraph::NodeIndex::IndexConfig> index_configs = {{"Person", "name"}, {"Company", "industry"}};

  Graph graph(test_dir_, index_configs, PageType::ARROW);

  // Add nodes with different labels and properties
  std::vector<Node> nodes = {
      Node(1, {"Person"}, "{\"name\":\"Alice\",\"age\":30}", 1000),
      Node(2, {"Person"}, "{\"name\":\"Bob\",\"age\":25}", 1001),
      Node(3, {"Person", "Employee"}, "{\"name\":\"Charlie\",\"age\":35}", 1002),
      Node(4, {"Company"}, "{\"name\":\"TechCorp\",\"industry\":\"tech\"}", 1003),
      Node(5, {"Company"}, "{\"name\":\"DataCorp\",\"industry\":\"data\"}", 1004),
      Node(6, {"Product"}, "{\"name\":\"Software\",\"category\":\"tools\"}", 1005) // Not indexed
  };

  ASSERT_TRUE(graph.AddNodes(nodes).ok());

  // Test that indexes are configured
  ASSERT_TRUE(graph.HasAnyIndexes());
  ASSERT_TRUE(graph.IsIndexed("Person", "name"));
  ASSERT_TRUE(graph.IsIndexed("Company", "industry"));
  ASSERT_FALSE(graph.IsIndexed("Product", "name")); // Not indexed
  ASSERT_FALSE(graph.IsIndexed("Person", "age"));   // Not indexed

  // Test finding nodes by indexed label
  auto person_ids = graph.FindNodesByLabel("Person");
  ASSERT_EQ(person_ids.size(), 3);
  ASSERT_TRUE(std::find(person_ids.begin(), person_ids.end(), 1) != person_ids.end());
  ASSERT_TRUE(std::find(person_ids.begin(), person_ids.end(), 2) != person_ids.end());
  ASSERT_TRUE(std::find(person_ids.begin(), person_ids.end(), 3) != person_ids.end());

  auto company_ids = graph.FindNodesByLabel("Company");
  ASSERT_EQ(company_ids.size(), 2);
  ASSERT_TRUE(std::find(company_ids.begin(), company_ids.end(), 4) != company_ids.end());
  ASSERT_TRUE(std::find(company_ids.begin(), company_ids.end(), 5) != company_ids.end());

  // Test finding nodes by non-indexed label (should return empty)
  auto product_ids = graph.FindNodesByLabel("Product");
  ASSERT_EQ(product_ids.size(), 0);

  // Test finding nodes by label and property (indexed combinations)
  auto alice_ids = graph.FindNodesByLabelAndProperty("Person", "name", "{\"name\":\"Alice\",\"age\":30}");
  ASSERT_EQ(alice_ids.size(), 1);
  ASSERT_EQ(alice_ids[0], 1);

  auto tech_companies =
      graph.FindNodesByLabelAndProperty("Company", "industry", "{\"name\":\"TechCorp\",\"industry\":\"tech\"}");
  ASSERT_EQ(tech_companies.size(), 1);
  ASSERT_EQ(tech_companies[0], 4);

  // Test finding nodes by non-indexed property (should return empty)
  auto age_30_ids = graph.FindNodesByLabelAndProperty("Person", "age", "{\"name\":\"Alice\",\"age\":30}");
  ASSERT_EQ(age_30_ids.size(), 0);

  // Test getting nodes by IDs
  auto person_nodes_result = graph.GetNodesByIds(person_ids);
  ASSERT_TRUE(person_nodes_result.ok());
  auto person_nodes = person_nodes_result.ValueOrDie();
  ASSERT_EQ(person_nodes.size(), 3);

  // Test deleting a node and verifying index is updated
  ASSERT_TRUE(graph.DeleteNodes({2}).ok());

  person_ids = graph.FindNodesByLabel("Person");
  ASSERT_EQ(person_ids.size(), 2);
  ASSERT_TRUE(std::find(person_ids.begin(), person_ids.end(), 1) != person_ids.end());
  ASSERT_TRUE(std::find(person_ids.begin(), person_ids.end(), 3) != person_ids.end());
  ASSERT_TRUE(std::find(person_ids.begin(), person_ids.end(), 2) == person_ids.end());
}

TEST_F(GraphTest, NoIndexGraph) {
  // Create graph without any indexes
  Graph graph(test_dir_, PageType::ARROW);

  std::vector<Node> nodes = {Node(1, {"Person"}, "{\"name\":\"Alice\",\"age\":30}", 1000),
                             Node(2, {"Company"}, "{\"name\":\"TechCorp\",\"industry\":\"tech\"}", 1001)};

  ASSERT_TRUE(graph.AddNodes(nodes).ok());

  // Test that no indexes are configured
  ASSERT_FALSE(graph.HasAnyIndexes());
  ASSERT_FALSE(graph.IsIndexed("Person", "name"));
  ASSERT_FALSE(graph.IsIndexed("Company", "industry"));

  // Test that index lookups return empty results
  auto person_ids = graph.FindNodesByLabel("Person");
  ASSERT_EQ(person_ids.size(), 0);

  auto alice_ids = graph.FindNodesByLabelAndProperty("Person", "name", "{\"name\":\"Alice\",\"age\":30}");
  ASSERT_EQ(alice_ids.size(), 0);

  // But regular node retrieval still works
  auto result = graph.GetNodes(1, 3);
  ASSERT_TRUE(result.ok());
  auto retrieved_nodes = result.ValueOrDie();
  ASSERT_EQ(retrieved_nodes.size(), 2);
}

TEST_F(GraphTest, IndexOperations) {
  // Create index configurations
  std::vector<NodeIndex::IndexConfig> index_configs = {NodeIndex::IndexConfig("Person", "name"),
                                                       NodeIndex::IndexConfig("Company", "industry")};

  // Create graph with indexes
  Graph graph(test_dir_, index_configs, PageType::ARROW);

  std::vector<Node> nodes = {Node(1, {"Person"}, "{\"name\":\"Alice\",\"age\":30}", 1000),
                             Node(2, {"Person"}, "{\"name\":\"Bob\",\"age\":25}", 1001),
                             Node(3, {"Company"}, "{\"name\":\"TechCorp\",\"industry\":\"tech\"}", 1002),
                             Node(4, {"Company"}, "{\"name\":\"FinanceCorp\",\"industry\":\"finance\"}", 1003)};

  ASSERT_TRUE(graph.AddNodes(nodes).ok());

  // Test index status
  ASSERT_TRUE(graph.HasAnyIndexes());
  ASSERT_TRUE(graph.IsIndexed("Person", "name"));
  ASSERT_TRUE(graph.IsIndexed("Company", "industry"));
  ASSERT_FALSE(graph.IsIndexed("Person", "age"));
  ASSERT_FALSE(graph.IsIndexed("Company", "name"));

  // Test finding nodes by label
  auto person_ids = graph.FindNodesByLabel("Person");
  ASSERT_EQ(person_ids.size(), 2);
  ASSERT_TRUE(std::find(person_ids.begin(), person_ids.end(), 1) != person_ids.end());
  ASSERT_TRUE(std::find(person_ids.begin(), person_ids.end(), 2) != person_ids.end());

  auto company_ids = graph.FindNodesByLabel("Company");
  ASSERT_EQ(company_ids.size(), 2);
  ASSERT_TRUE(std::find(company_ids.begin(), company_ids.end(), 3) != company_ids.end());
  ASSERT_TRUE(std::find(company_ids.begin(), company_ids.end(), 4) != company_ids.end());

  // Test finding nodes by label and property
  auto alice_ids = graph.FindNodesByLabelAndProperty("Person", "name", "{\"name\":\"Alice\",\"age\":30}");
  ASSERT_EQ(alice_ids.size(), 1);
  ASSERT_EQ(alice_ids[0], 1);

  auto tech_company_ids =
      graph.FindNodesByLabelAndProperty("Company", "industry", "{\"name\":\"TechCorp\",\"industry\":\"tech\"}");
  ASSERT_EQ(tech_company_ids.size(), 1);
  ASSERT_EQ(tech_company_ids[0], 3);

  // Test finding nodes by non-indexed property (should return empty)
  auto age_30_ids = graph.FindNodesByLabelAndProperty("Person", "age", "30");
  ASSERT_EQ(age_30_ids.size(), 0);

  // Test finding nodes by non-existent label
  auto non_existent_ids = graph.FindNodesByLabel("NonExistent");
  ASSERT_EQ(non_existent_ids.size(), 0);

  // Test getting nodes by IDs
  auto person_nodes_result = graph.GetNodesByIds(person_ids);
  ASSERT_TRUE(person_nodes_result.ok());
  auto person_nodes = person_nodes_result.ValueOrDie();
  ASSERT_EQ(person_nodes.size(), 2);

  // Test deleting nodes and verifying index is updated
  ASSERT_TRUE(graph.DeleteNodes({1, 3}).ok());

  person_ids = graph.FindNodesByLabel("Person");
  ASSERT_EQ(person_ids.size(), 1);
  ASSERT_EQ(person_ids[0], 2);

  company_ids = graph.FindNodesByLabel("Company");
  ASSERT_EQ(company_ids.size(), 1);
  ASSERT_EQ(company_ids[0], 4);

  // Test that deleted nodes are no longer found by property
  alice_ids = graph.FindNodesByLabelAndProperty("Person", "name", "{\"name\":\"Alice\",\"age\":30}");
  ASSERT_EQ(alice_ids.size(), 0);

  tech_company_ids =
      graph.FindNodesByLabelAndProperty("Company", "industry", "{\"name\":\"TechCorp\",\"industry\":\"tech\"}");
  ASSERT_EQ(tech_company_ids.size(), 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}