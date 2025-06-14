#pragma once

namespace memgraph {

enum class IsolationLevel {
  NO_ISOLATION,
  READ_UNCOMMITTED,
  READ_COMMITTED,
};

} // namespace memgraph