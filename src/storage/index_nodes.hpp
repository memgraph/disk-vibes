#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <folly/ConcurrentSkipList.h>

#include "node.hpp"

namespace memgraph {

// Index structure for quick node lookups by label and property value using Folly's ConcurrentSkipList
class NodeIndex {
public:
  // Configuration for which labels and properties to index
  struct IndexConfig {
    std::string label;
    std::string property_name;

    IndexConfig(const std::string &l, const std::string &p) : label(l), property_name(p) {}

    bool operator==(const IndexConfig &other) const {
      return label == other.label && property_name == other.property_name;
    }
  };

  // Combined key-value structure for the skiplist
  struct IndexEntry {
    std::string label;
    std::string property_name;
    std::string property_value;
    std::unordered_set<int64_t> node_ids;

    // Default constructor required by Folly's ConcurrentSkipList
    IndexEntry() = default;

    IndexEntry(const std::string &l, const std::string &pn, const std::string &pv, int64_t node_id)
        : label(l), property_name(pn), property_value(pv) {
      node_ids.insert(node_id);
    }

    // Comparison operators for skiplist ordering
    bool operator<(const IndexEntry &other) const {
      if (label != other.label)
        return label < other.label;
      if (property_name != other.property_name)
        return property_name < other.property_name;
      return property_value < other.property_value;
    }

    bool operator==(const IndexEntry &other) const {
      return label == other.label && property_name == other.property_name && property_value == other.property_value;
    }

    void AddNodeId(int64_t node_id) { node_ids.insert(node_id); }
    void RemoveNodeId(int64_t node_id) { node_ids.erase(node_id); }
    bool IsEmpty() const { return node_ids.empty(); }
  };

  // Constructor with optional index configurations
  NodeIndex() = default;

  explicit NodeIndex(const std::vector<IndexConfig> &configs) : index_configs_(configs) {
    // Initialize the concurrent skiplist with a reasonable head height
    skiplist_ = folly::ConcurrentSkipList<IndexEntry>::createInstance(16);
  }

  void AddNode(const Node &node) {
    if (index_configs_.empty()) {
      return; // Skip indexing if no configurations
    }

    // Only index if the node has labels that match our configurations
    for (const auto &label : node.labels) {
      for (const auto &config : index_configs_) {
        if (config.label == label) {
          // For now, we'll index by the entire props string
          // In a more sophisticated implementation, you'd parse the JSON and extract specific properties

          // Use accessor for thread-safe operations
          typename folly::ConcurrentSkipList<IndexEntry>::Accessor accessor(skiplist_);

          // Try to find existing entry
          IndexEntry search_key(label, config.property_name, node.props, 0);
          auto it = accessor.find(search_key);
          if (it != accessor.end()) {
            // Update existing entry
            it->AddNodeId(node.id);
          } else {
            // Create new entry
            IndexEntry new_entry(label, config.property_name, node.props, node.id);
            accessor.insert(new_entry);
          }
        }
      }
    }
  }

  void RemoveNode(const Node &node) {
    if (index_configs_.empty()) {
      return; // Skip indexing if no configurations
    }

    for (const auto &label : node.labels) {
      for (const auto &config : index_configs_) {
        if (config.label == label) {
          IndexEntry search_key(label, config.property_name, node.props, 0);

          // Use accessor for thread-safe operations
          typename folly::ConcurrentSkipList<IndexEntry>::Accessor accessor(skiplist_);

          auto it = accessor.find(search_key);
          if (it != accessor.end()) {
            it->RemoveNodeId(node.id);
            if (it->IsEmpty()) {
              accessor.erase(search_key);
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

    IndexEntry search_key(label, property_name, property_value, 0);

    // Use accessor for thread-safe operations
    typename folly::ConcurrentSkipList<IndexEntry>::Accessor accessor(skiplist_);

    auto it = accessor.find(search_key);
    if (it != accessor.end()) {
      std::vector<int64_t> result(it->node_ids.begin(), it->node_ids.end());
      return result;
    }

    return {};
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

    std::unordered_set<int64_t> result_set;

    // Use accessor for thread-safe operations
    typename folly::ConcurrentSkipList<IndexEntry>::Accessor accessor(skiplist_);

    // Iterate through all entries for this label
    for (auto it = accessor.begin(); it != accessor.end(); ++it) {
      if (it->label == label) {
        result_set.insert(it->node_ids.begin(), it->node_ids.end());
      }
    }

    std::vector<int64_t> result(result_set.begin(), result_set.end());
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
    // Create a new skiplist instance to clear all data
    skiplist_ = folly::ConcurrentSkipList<IndexEntry>::createInstance(16);
  }

private:
  std::shared_ptr<folly::ConcurrentSkipList<IndexEntry>> skiplist_;
  std::vector<IndexConfig> index_configs_;
};

} // namespace memgraph