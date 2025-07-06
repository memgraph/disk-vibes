#pragma once

#include "edge.hpp"
#include "node.hpp"
#include <variant>
#include <vector>

namespace memgraph::transaction {

enum class TransactionState { ACTIVE, COMMITTED, ABORTED };

struct LogEntry {
  enum class Type { NEW_NODES, DELETE_NODES, NEW_EDGES, DELETE_EDGES };
  int64_t entry_id; // Unique ID for this log entry
  Type type;
  std::variant<std::vector<Node>, std::vector<Edge>> data;
};

struct Transaction {
  int64_t id;
  TransactionState state;
  std::vector<LogEntry> entries;
};

} // namespace memgraph::transaction