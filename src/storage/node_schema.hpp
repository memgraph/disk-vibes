#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <vector>

#include "arrow.hpp" // GPageMeta
#include "node.hpp"

namespace memgraph::node_schema {

std::shared_ptr<::arrow::Schema> Schema() {
  return ::arrow::schema(
      {
          ::arrow::field("id", ::arrow::int64()),
          ::arrow::field("labels", ::arrow::list(::arrow::utf8())),
          ::arrow::field("props", ::arrow::utf8()), // JSON-encoded
          ::arrow::field("ts", ::arrow::timestamp(::arrow::TimeUnit::MICRO)),
      },
      memgraph::arrow::GPageMeta());
}

::arrow::Result<std::shared_ptr<::arrow::Table>> Make(const std::vector<Node> &nodes, ::arrow::MemoryPool *pool) {
  auto schema = Schema();
  ::arrow::Int64Builder id_builder{pool};
  ::arrow::ListBuilder labels_builder(pool, std::make_shared<::arrow::StringBuilder>(pool));
  ::arrow::StringBuilder prop_builder{pool};
  ::arrow::TimestampBuilder ts_builder(::arrow::timestamp(::arrow::TimeUnit::MICRO), pool);
  for (const auto &node : nodes) {
    ARROW_RETURN_NOT_OK(id_builder.Append(node.id));
    ARROW_RETURN_NOT_OK(labels_builder.Append());
    auto *vb = static_cast<::arrow::StringBuilder *>(labels_builder.value_builder());
    for (const auto &label : node.labels) {
      ARROW_RETURN_NOT_OK(vb->Append(label));
    }
    ARROW_RETURN_NOT_OK(prop_builder.Append(node.props));
    ARROW_RETURN_NOT_OK(ts_builder.Append(node.ts));
  }
  std::shared_ptr<::arrow::Array> id_array, labels_array, prop_array, ts_array;
  ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
  ARROW_RETURN_NOT_OK(labels_builder.Finish(&labels_array));
  ARROW_RETURN_NOT_OK(prop_builder.Finish(&prop_array));
  ARROW_RETURN_NOT_OK(ts_builder.Finish(&ts_array));
  return ::arrow::Table::Make(schema, {id_array, labels_array, prop_array, ts_array});
}

} // namespace memgraph::node_schema