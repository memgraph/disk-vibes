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

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}