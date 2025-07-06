#pragma once

#include <string>
#include <vector>

namespace memgraph {

struct Node {
  int64_t id;
  std::vector<std::string> labels;
  std::string props; // JSON-encoded properties
  int64_t ts;        // timestamp in microseconds
  Node(int64_t id = 0, const std::vector<std::string> &labels = {}, const std::string &props = "", int64_t ts = 0)
      : id(id), labels(labels), props(props), ts(ts) {}
};

} // namespace memgraph