#pragma once

#include <string>

namespace memgraph {

struct Edge {
  int64_t id;
  int64_t src;
  int64_t dst;
  std::string type;
  std::string props; // JSON-encoded properties
  int64_t ts;        // timestamp in microseconds
  // Constructor with default values
  Edge(int64_t id = 0, int64_t src = 0, int64_t dst = 0, const std::string &type = "", const std::string &props = "",
       int64_t ts = 0)
      : id(id), src(src), dst(dst), type(type), props(props), ts(ts) {}
};

} // namespace memgraph