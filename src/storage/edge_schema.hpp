#pragma once

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <memory>
#include <vector>

#include "arrow.hpp"
#include "edge.hpp"

namespace memgraph::edge_schema {

inline std::shared_ptr<::arrow::Schema> Schema() {
  return ::arrow::schema(
      {
          // NOTE: id type should be configurable by the user (src, dst) also
          // because these are node ids.
          ::arrow::field("id", ::arrow::int64()),
          ::arrow::field("src", ::arrow::int64()),
          ::arrow::field("dst", ::arrow::int64()),
          ::arrow::field("type", ::arrow::utf8()),
          ::arrow::field("props", ::arrow::utf8()), // JSON-encoded
          ::arrow::field("ts", ::arrow::timestamp(::arrow::TimeUnit::MICRO)),
      },
      memgraph::arrow::GPageMeta());
}

inline ::arrow::Result<std::shared_ptr<::arrow::Table>> Make(const std::vector<Edge> &edges,
                                                             ::arrow::MemoryPool *pool) {
  ::arrow::Int64Builder id_builder{pool};
  ::arrow::Int64Builder src_builder{pool};
  ::arrow::Int64Builder dst_builder{pool};
  ::arrow::StringBuilder type_builder{pool};
  ::arrow::StringBuilder props_builder{pool};
  ::arrow::TimestampBuilder ts_builder(::arrow::timestamp(::arrow::TimeUnit::MICRO), pool);
  for (const auto &edge : edges) {
    ARROW_RETURN_NOT_OK(id_builder.Append(edge.id));
    ARROW_RETURN_NOT_OK(src_builder.Append(edge.src));
    ARROW_RETURN_NOT_OK(dst_builder.Append(edge.dst));
    ARROW_RETURN_NOT_OK(type_builder.Append(edge.type));
    ARROW_RETURN_NOT_OK(props_builder.Append(edge.props));
    ARROW_RETURN_NOT_OK(ts_builder.Append(edge.ts));
  }
  std::shared_ptr<::arrow::Array> id_array, src_array, dst_array, type_array, props_array, ts_array;
  ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
  ARROW_RETURN_NOT_OK(src_builder.Finish(&src_array));
  ARROW_RETURN_NOT_OK(dst_builder.Finish(&dst_array));
  ARROW_RETURN_NOT_OK(type_builder.Finish(&type_array));
  ARROW_RETURN_NOT_OK(props_builder.Finish(&props_array));
  ARROW_RETURN_NOT_OK(ts_builder.Finish(&ts_array));
  return ::arrow::Table::Make(Schema(), {id_array, src_array, dst_array, type_array, props_array, ts_array});
}

} // namespace memgraph::edge_schema