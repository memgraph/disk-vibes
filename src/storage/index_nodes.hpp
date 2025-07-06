#pragma once

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "node.hpp"

namespace memgraph {

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

} // namespace memgraph