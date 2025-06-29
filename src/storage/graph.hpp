#pragma once

#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "page_edges_arrow.hpp"
#include "page_edges_parquet.hpp"
#include "page_nodes_arrow.hpp"
#include "page_nodes_parquet.hpp"
#include "page_type.hpp"

namespace memgraph {

////////
// DESIGN
//   On this layer (Graph stored under any filesystem-like storage), we only
//   care about the notions of memory (how the data is storage/serialized) and
//   time (when something happens, WALL time or any logical time like
//   transaction id). The "Graph" layer will only restructure and reorder data
//   based on the user inputs.
//
// USAGE
//   The Graph object should be useful as-is. Concurrent access should work. You
//   can use the Graph directly to mange your graph data, but to get different
//   transactional correctness guarantees, there has to be a higher-levle logic
//   (e.g. TransactionalGraph implementation).
////////

class GraphMutexManager {
public:
  void CreatePageMutexIfMissing(const std::filesystem::path &path) {
    std::lock_guard<std::mutex> lock(graph_mutex_);
    if (page_mutexes_.find(path) == page_mutexes_.end()) {
      page_mutexes_[path] = std::make_unique<std::shared_mutex>();
    }
  }
  std::shared_mutex &GetPageMutex(const std::filesystem::path &path) {
    std::lock_guard<std::mutex> lock(graph_mutex_);
    return *page_mutexes_[path];
  }

private:
  // NOTE: It's important to guard the std::map on both read and writes, that's
  // why there is the graph mutex.
  std::mutex graph_mutex_;
  std::map<std::string, std::unique_ptr<std::shared_mutex>> page_mutexes_;
};

// Index structure for quick node lookups by label and property value
class NodeIndex {
public:
  // Index structure: label -> property_name -> property_value -> set of node IDs
  using IndexMap =
      std::unordered_map<std::string,
                         std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_set<int64_t>>>>;

  // Configuration for which labels and properties to index
  struct IndexConfig {
    std::string label;
    std::string property_name;

    IndexConfig(const std::string &l, const std::string &p) : label(l), property_name(p) {}

    bool operator==(const IndexConfig &other) const {
      return label == other.label && property_name == other.property_name;
    }
  };

  // Constructor with optional index configurations
  NodeIndex() = default;

  explicit NodeIndex(const std::vector<IndexConfig> &configs) : index_configs_(configs) {
    // Create empty index entries for each configured label/property combination
    for (const auto &config : index_configs_) {
      index_[config.label][config.property_name];
    }
  }

  void AddNode(const Node &node) {
    if (index_configs_.empty()) {
      return; // Skip indexing if no configurations
    }

    std::lock_guard<std::shared_mutex> lock(index_mutex_);

    // Only index if the node has labels that match our configurations
    for (const auto &label : node.labels) {
      for (const auto &config : index_configs_) {
        if (config.label == label) {
          // For now, we'll index by the entire props string
          // In a more sophisticated implementation, you'd parse the JSON and extract specific properties
          index_[label][config.property_name][node.props].insert(node.id);
        }
      }
    }
  }

  void RemoveNode(const Node &node) {
    if (index_configs_.empty()) {
      return; // Skip indexing if no configurations
    }

    std::lock_guard<std::shared_mutex> lock(index_mutex_);

    for (const auto &label : node.labels) {
      for (const auto &config : index_configs_) {
        if (config.label == label) {
          auto label_it = index_.find(label);
          if (label_it != index_.end()) {
            auto props_it = label_it->second.find(config.property_name);
            if (props_it != label_it->second.end()) {
              auto value_it = props_it->second.find(node.props);
              if (value_it != props_it->second.end()) {
                value_it->second.erase(node.id);
                if (value_it->second.empty()) {
                  props_it->second.erase(value_it);
                }
              }
            }
            if (label_it->second.empty()) {
              index_.erase(label_it);
            }
          }
        }
      }
    }
  }

  std::vector<int64_t> FindNodesByLabelAndProperty(const std::string &label, const std::string &property_name,
                                                   const std::string &property_value) const {
    if (index_configs_.empty()) {
      return {}; // Return empty if no indexes configured
    }

    // Check if this label/property combination is indexed
    bool is_indexed = false;
    for (const auto &config : index_configs_) {
      if (config.label == label && config.property_name == property_name) {
        is_indexed = true;
        break;
      }
    }

    if (!is_indexed) {
      return {}; // Return empty if not indexed
    }

    std::shared_lock<std::shared_mutex> lock(index_mutex_);

    std::vector<int64_t> result;

    auto label_it = index_.find(label);
    if (label_it != index_.end()) {
      auto props_it = label_it->second.find(property_name);
      if (props_it != label_it->second.end()) {
        auto value_it = props_it->second.find(property_value);
        if (value_it != props_it->second.end()) {
          result.insert(result.end(), value_it->second.begin(), value_it->second.end());
        }
      }
    }

    return result;
  }

  std::vector<int64_t> FindNodesByLabel(const std::string &label) const {
    if (index_configs_.empty()) {
      return {}; // Return empty if no indexes configured
    }

    // Check if this label is indexed
    bool is_indexed = false;
    for (const auto &config : index_configs_) {
      if (config.label == label) {
        is_indexed = true;
        break;
      }
    }

    if (!is_indexed) {
      return {}; // Return empty if not indexed
    }

    std::shared_lock<std::shared_mutex> lock(index_mutex_);

    std::vector<int64_t> result;

    auto label_it = index_.find(label);
    if (label_it != index_.end()) {
      for (const auto &[prop_name, prop_values] : label_it->second) {
        for (const auto &[prop_value, node_ids] : prop_values) {
          result.insert(result.end(), node_ids.begin(), node_ids.end());
        }
      }
    }

    return result;
  }

  bool IsIndexed(const std::string &label, const std::string &property_name) const {
    for (const auto &config : index_configs_) {
      if (config.label == label && config.property_name == property_name) {
        return true;
      }
    }
    return false;
  }

  bool HasAnyIndexes() const { return !index_configs_.empty(); }

  void Clear() {
    std::lock_guard<std::shared_mutex> lock(index_mutex_);
    index_.clear();
  }

private:
  mutable std::shared_mutex index_mutex_;
  IndexMap index_;
  std::vector<IndexConfig> index_configs_;
};

class Graph {
public:
  explicit Graph(const std::filesystem::path &data_dir, PageType page_type = PageType::ARROW,
                 const int64_t batch_size = 100'000)
      : data_dir_(data_dir), page_type_(page_type), batch_size_(batch_size) {
    std::filesystem::create_directories(data_dir_);
  }

  // Constructor with index configurations
  Graph(const std::filesystem::path &data_dir, const std::vector<NodeIndex::IndexConfig> &index_configs,
        PageType page_type = PageType::ARROW, const int64_t batch_size = 100'000)
      : data_dir_(data_dir), page_type_(page_type), batch_size_(batch_size), node_index_(index_configs) {
    std::filesystem::create_directories(data_dir_);
  }

  const std::filesystem::path &GetDirectory() const { return data_dir_; }
  PageType GetPageType() const { return page_type_; }
  size_t GetPageSize() const { return batch_size_; }

  // Index-based lookup methods
  std::vector<int64_t> FindNodesByLabel(const std::string &label) const { return node_index_.FindNodesByLabel(label); }

  std::vector<int64_t> FindNodesByLabelAndProperty(const std::string &label, const std::string &property_name,
                                                   const std::string &property_value) const {
    return node_index_.FindNodesByLabelAndProperty(label, property_name, property_value);
  }

  // Index status methods
  bool IsIndexed(const std::string &label, const std::string &property_name) const {
    return node_index_.IsIndexed(label, property_name);
  }

  bool HasAnyIndexes() const { return node_index_.HasAnyIndexes(); }

  // Get nodes by their IDs (useful after index lookups)
  ::arrow::Result<std::vector<Node>> GetNodesByIds(const std::vector<int64_t> &node_ids,
                                                   std::optional<int64_t> tx_id = std::nullopt) {
    if (node_ids.empty()) {
      return std::vector<Node>{};
    }

    // Find the min and max IDs to determine the page range
    auto min_max = std::minmax_element(node_ids.begin(), node_ids.end());
    int64_t min_id = *min_max.first;
    int64_t max_id = *min_max.second;

    // Get all nodes in the range
    auto all_nodes_result = GetNodes(min_id, max_id + 1, tx_id);
    if (!all_nodes_result.ok()) {
      return all_nodes_result.status();
    }

    auto all_nodes = all_nodes_result.ValueOrDie();

    // Filter to only the requested IDs
    std::unordered_set<int64_t> requested_ids(node_ids.begin(), node_ids.end());
    std::vector<Node> result;
    result.reserve(node_ids.size());

    for (const auto &node : all_nodes) {
      if (requested_ids.find(node.id) != requested_ids.end()) {
        result.push_back(node);
      }
    }

    return result;
  }

  auto GetPageIdsSetForNodes(const std::vector<Node> &nodes) {
    std::unordered_set<int64_t> page_ids;
    page_ids.reserve(nodes.size());
    for (const auto &node : nodes) {
      page_ids.insert(node.id / GetPageSize());
    }
    return page_ids;
  }

  auto GetPageIdsSetForEdges(const std::vector<Edge> &edges) {
    std::unordered_set<int64_t> page_ids;
    page_ids.reserve(edges.size());
    for (const auto &edge : edges) {
      page_ids.insert(edge.id / GetPageSize());
    }
    return page_ids;
  }

  auto GetPageIdsNodesMap(const std::vector<Node> &nodes) {
    std::map<int64_t, std::vector<Node>> page_nodes;
    for (const auto &node : nodes) {
      int64_t page_id = node.id / batch_size_;
      page_nodes[page_id].push_back(node);
    }
    return page_nodes;
  }

  auto GetPageIdsEdgesMap(const std::vector<Edge> &edges) {
    // Group edges by their page based on ID
    std::map<int64_t, std::vector<memgraph::Edge>> page_edges;
    for (const auto &edge : edges) {
      int64_t page_id = edge.id / batch_size_;
      page_edges[page_id].push_back(edge);
    }
    return page_edges;
  }

  std::map<int64_t, std::vector<int64_t>> GetPageToIdsMapping(const std::vector<int64_t> &ids) const {
    std::map<int64_t, std::vector<int64_t>> page_to_ids;
    for (int64_t id : ids) {
      int64_t page_id = id / batch_size_;
      page_to_ids[page_id].push_back(id);
    }
    return page_to_ids;
  }

  std::filesystem::path GetFilePath(char prefix, int64_t page_id, std::optional<int64_t> tx_id) const {
    std::string filename = prefix + std::string("__") + std::to_string(page_id * batch_size_);
    if (tx_id) {
      filename += "__tx_" + std::to_string(*tx_id);
    }
    filename += [this]() {
      switch (page_type_) {
      case PageType::ARROW:
        return ".arrow";
      case PageType::PARQUET:
        return ".parquet";
      default:
        throw std::runtime_error("Unsupported page type");
      }
    }();
    return data_dir_ / filename;
  }
  std::filesystem::path GetNodeFilePath(int64_t page_id, std::optional<int64_t> tx_id) const {
    return GetFilePath('N', page_id, tx_id);
  }
  std::filesystem::path GetEdgeFilePath(int64_t page_id, std::optional<int64_t> tx_id) const {
    return GetFilePath('E', page_id, tx_id);
  }

  ::arrow::Status AddNodes(const std::vector<Node> &nodes, std::optional<int64_t> tx_id = std::nullopt) {
    auto page_nodes = GetPageIdsNodesMap(nodes);
    // Write each page
    for (const auto &[page_id, new_nodes] : page_nodes) {
      auto filepath = GetNodeFilePath(page_id, tx_id);
      mutex_manager_.CreatePageMutexIfMissing(filepath);
      std::vector<Node> existing_nodes = ReadExistingNodes(filepath);
      std::vector<Node> final_nodes = MergeNodes(existing_nodes, new_nodes);
      ARROW_RETURN_NOT_OK(WriteNodes(final_nodes, filepath));
    }

    // Update the index with new nodes
    for (const auto &node : nodes) {
      node_index_.AddNode(node);
    }

    return ::arrow::Status::OK();
  }

  ::arrow::Status AddEdges(const std::vector<memgraph::Edge> &edges, std::optional<int64_t> tx_id = std::nullopt) {
    auto page_edges = GetPageIdsEdgesMap(edges);

    // Write each page
    for (const auto &[page_id, new_edges] : page_edges) {
      auto filepath = GetEdgeFilePath(page_id, tx_id);
      mutex_manager_.CreatePageMutexIfMissing(filepath);
      std::vector<memgraph::Edge> existing_edges = ReadExistingEdges(filepath);
      std::vector<memgraph::Edge> final_edges = MergeEdges(existing_edges, new_edges);
      ARROW_RETURN_NOT_OK(WriteEdges(final_edges, filepath));
    }
    return ::arrow::Status::OK();
  }

  ::arrow::Result<std::vector<Node>> GetNodes(int64_t start_id, int64_t end_id,
                                              std::optional<int64_t> tx_id = std::nullopt,
                                              std::optional<std::vector<std::string>> page_filenames = std::nullopt) {
    std::vector<Node> result;
    int64_t start_page = start_id / batch_size_;
    int64_t end_page = (end_id - 1) / batch_size_;
    for (int64_t page_id = start_page; page_id <= end_page; ++page_id) {
      auto filepath = page_filenames ? std::filesystem::path((*page_filenames)[page_id - start_page])
                                     : GetNodeFilePath(page_id, tx_id);
      if (!std::filesystem::exists(filepath))
        continue;
      // Ensure mutex exists before trying to acquire it
      mutex_manager_.CreatePageMutexIfMissing(filepath);

      switch (page_type_) {
      case PageType::ARROW: {
        memgraph::npage_arrow::Page page(filepath);
        {
          std::shared_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
          ARROW_ASSIGN_OR_RAISE(page, memgraph::npage_arrow::Page::Deserialize(filepath, batch_size_));
        }
        for (const auto &node : page.Nodes()) {
          if (node.id >= start_id && node.id < end_id) {
            result.push_back(node);
          }
        }
        break;
      }

      case PageType::PARQUET: {
        memgraph::npage_parquet::Page page(filepath);
        {
          std::shared_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
          ARROW_ASSIGN_OR_RAISE(page, memgraph::npage_parquet::Page::Deserialize(filepath, batch_size_));
        }
        for (const auto &node : page.Nodes()) {
          if (node.id >= start_id && node.id < end_id) {
            result.push_back(node);
          }
        }
        break;
      }

      default:
        return ::arrow::Status::Invalid("Unsupported page type");
      }
    }
    return result;
  }

  ::arrow::Result<std::vector<memgraph::Edge>>
  GetEdges(int64_t start_id, int64_t end_id, std::optional<int64_t> tx_id = std::nullopt,
           std::optional<std::vector<std::string>> page_filenames = std::nullopt) {
    std::vector<memgraph::Edge> result;
    int64_t start_page = start_id / batch_size_;
    int64_t end_page = (end_id - 1) / batch_size_;
    for (int64_t page_id = start_page; page_id <= end_page; ++page_id) {
      auto filepath = page_filenames ? std::filesystem::path((*page_filenames)[page_id - start_page])
                                     : GetEdgeFilePath(page_id, tx_id);
      if (!std::filesystem::exists(filepath))
        continue;
      mutex_manager_.CreatePageMutexIfMissing(filepath);

      switch (page_type_) {
      case PageType::ARROW: {
        memgraph::epage_arrow::Page page(filepath);
        {
          std::shared_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
          ARROW_ASSIGN_OR_RAISE(page, memgraph::epage_arrow::Page::Deserialize(filepath, batch_size_));
        }
        for (const auto &edge : page.Edges()) {
          if (edge.id >= start_id && edge.id < end_id) {
            result.push_back(edge);
          }
        }
        break;
      }

      case PageType::PARQUET: {
        memgraph::epage_parquet::Page page(filepath);
        {
          std::shared_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
          ARROW_ASSIGN_OR_RAISE(page, memgraph::epage_parquet::Page::Deserialize(filepath, batch_size_));
        }
        for (const auto &edge : page.Edges()) {
          if (edge.id >= start_id && edge.id < end_id) {
            result.push_back(edge);
          }
        }
        break;
      }

      default:
        return ::arrow::Status::Invalid("Unsupported page type");
      }
    }
    return result;
  }

  ::arrow::Status DeleteNodes(const std::vector<int64_t> &node_ids, std::optional<int64_t> tx_id = std::nullopt) {
    // Create a set for O(1) lookup of IDs to delete
    std::unordered_set<int64_t> ids_to_delete(node_ids.begin(), node_ids.end());
    auto page_to_ids = GetPageToIdsMapping(node_ids);

    // First, get the nodes that will be deleted to update the index
    std::vector<Node> nodes_to_delete;
    for (const auto &[page_id, ids] : page_to_ids) {
      auto filepath = GetNodeFilePath(page_id, tx_id);
      if (!std::filesystem::exists(filepath)) {
        continue;
      }
      auto existing_nodes = ReadExistingNodes(filepath);
      for (const auto &node : existing_nodes) {
        if (ids_to_delete.find(node.id) != ids_to_delete.end()) {
          nodes_to_delete.push_back(node);
        }
      }
    }

    // Process each page
    for (const auto &[page_id, ids] : page_to_ids) {
      auto filepath = GetNodeFilePath(page_id, tx_id);
      if (!std::filesystem::exists(filepath)) {
        continue;
      }
      auto existing_nodes = ReadExistingNodes(filepath);
      auto remaining_nodes = FilterNodesToDelete(existing_nodes, ids_to_delete);
      ARROW_RETURN_NOT_OK(WriteNodes(remaining_nodes, filepath));
    }

    // Update the index by removing deleted nodes
    for (const auto &node : nodes_to_delete) {
      node_index_.RemoveNode(node);
    }

    return ::arrow::Status::OK();
  }

  ::arrow::Status DeleteEdges(const std::vector<int64_t> &edge_ids, std::optional<int64_t> tx_id = std::nullopt) {
    // Create a set for O(1) lookup of IDs to delete
    std::unordered_set<int64_t> ids_to_delete(edge_ids.begin(), edge_ids.end());
    auto page_to_ids = GetPageToIdsMapping(edge_ids);
    // Process each page
    for (const auto &[page_id, ids] : page_to_ids) {
      auto filepath = GetEdgeFilePath(page_id, tx_id);
      if (!std::filesystem::exists(filepath)) {
        continue;
      }
      auto exisitng_edges = ReadExistingEdges(filepath);
      auto remaining_edges = FilterEdgesToDelete(exisitng_edges, ids_to_delete);
      ARROW_RETURN_NOT_OK(WriteEdges(remaining_edges, filepath));
    }
    return ::arrow::Status::OK();
  }

private:
  std::vector<Node> ReadExistingNodes(const std::filesystem::path &filepath) {
    std::shared_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
    std::vector<Node> existing_nodes;
    if (std::filesystem::exists(filepath)) {
      switch (page_type_) {
      case PageType::ARROW: {
        auto page_result = memgraph::npage_arrow::Page::Deserialize(filepath, batch_size_);
        if (page_result.ok()) {
          existing_nodes = page_result.ValueOrDie().Nodes();
        }
        break;
      }
      case PageType::PARQUET: {
        auto page_result = npage_parquet::Page::Deserialize(filepath, batch_size_);
        if (page_result.ok()) {
          existing_nodes = page_result.ValueOrDie().Nodes();
        }
        break;
      }
      }
    }
    return existing_nodes;
  }

  ::arrow::Status WriteNodes(const std::vector<Node> &nodes, const std::filesystem::path &filepath) {
    switch (page_type_) {
    case PageType::ARROW: {
      auto page = memgraph::npage_arrow::Page(nodes, filepath.string());
      std::unique_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
      return page.Serialize();
    }
    case PageType::PARQUET: {
      auto page = npage_parquet::Page(nodes, filepath.string());
      std::unique_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
      return page.Serialize();
    }
    default:
      return ::arrow::Status::Invalid("Unsupported page type");
    }
  }

  std::vector<Node> MergeNodes(const std::vector<Node> &existing_nodes, const std::vector<Node> &new_nodes) {
    std::map<int64_t, Node> merged_nodes;
    for (const auto &node : existing_nodes) {
      merged_nodes[node.id] = node;
    }
    for (const auto &node : new_nodes) {
      merged_nodes[node.id] = node;
    }
    std::vector<Node> final_nodes;
    final_nodes.reserve(merged_nodes.size());
    for (const auto &[_, node] : merged_nodes) {
      final_nodes.push_back(node);
    }
    return final_nodes;
  }

  std::vector<memgraph::Edge> ReadExistingEdges(const std::filesystem::path &filepath) {
    std::shared_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
    std::vector<memgraph::Edge> existing_edges;
    if (std::filesystem::exists(filepath)) {
      switch (page_type_) {
      case PageType::ARROW: {
        auto page_result = epage_arrow::Page::Deserialize(filepath, batch_size_);
        if (page_result.ok()) {
          existing_edges = page_result.ValueOrDie().Edges();
        }
        break;
      }
      case PageType::PARQUET: {
        auto page_result = epage_parquet::Page::Deserialize(filepath, batch_size_);
        if (page_result.ok()) {
          existing_edges = page_result.ValueOrDie().Edges();
        }
        break;
      }
      }
    }
    return existing_edges;
  }

  ::arrow::Status WriteEdges(const std::vector<memgraph::Edge> &edges, const std::filesystem::path &filepath) {
    switch (page_type_) {
    case PageType::ARROW: {
      auto page = epage_arrow::Page(edges, filepath);
      std::unique_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
      return page.Serialize();
    }
    case PageType::PARQUET: {
      auto page = epage_parquet::Page(edges, filepath);
      std::unique_lock<std::shared_mutex> lock(mutex_manager_.GetPageMutex(filepath));
      return page.Serialize();
    }
    default:
      return ::arrow::Status::Invalid("Unsupported page type");
    }
  }

  std::vector<memgraph::Edge> MergeEdges(const std::vector<memgraph::Edge> &existing_edges,
                                         const std::vector<memgraph::Edge> &new_edges) {
    std::map<int64_t, memgraph::Edge> merged_edges;
    for (const auto &edge : existing_edges) {
      merged_edges[edge.id] = edge;
    }
    for (const auto &edge : new_edges) {
      merged_edges[edge.id] = edge;
    }
    std::vector<memgraph::Edge> final_edges;
    final_edges.reserve(merged_edges.size());
    for (const auto &[_, edge] : merged_edges) {
      final_edges.push_back(edge);
    }
    return final_edges;
  }

  std::vector<Node> FilterNodesToDelete(const std::vector<Node> &existing_nodes,
                                        const std::unordered_set<int64_t> &ids_to_delete) {
    std::vector<Node> remaining_nodes;
    for (const auto &node : existing_nodes) {
      if (ids_to_delete.find(node.id) == ids_to_delete.end()) {
        remaining_nodes.push_back(node);
      }
    }
    return remaining_nodes;
  }

  std::vector<Edge> FilterEdgesToDelete(const std::vector<Edge> &existing_edges,
                                        const std::unordered_set<int64_t> &ids_to_delete) {
    std::vector<Edge> remaining_edges;
    for (const auto &edge : existing_edges) {
      if (ids_to_delete.find(edge.id) == ids_to_delete.end()) {
        remaining_edges.push_back(edge);
      }
    }
    return remaining_edges;
  }

  const std::filesystem::path data_dir_;
  const PageType page_type_;
  const int64_t batch_size_;
  GraphMutexManager mutex_manager_;
  NodeIndex node_index_;
};

} // namespace memgraph
