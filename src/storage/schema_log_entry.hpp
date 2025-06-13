#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/writer.h>
#include <parquet/arrow/writer.h>
#include <vector>

#include "edge.hpp"
#include "edge_schema.hpp"
#include "node.hpp"
#include "node_schema.hpp"
#include "transaction_types.hpp"

namespace memgraph {

// Forward declarations
class TransactionalGraph;
namespace detail {
class CommitLog;
struct LogEntry;
struct Transaction;
} // namespace detail

namespace log_schema {

inline std::shared_ptr<::arrow::Schema> Schema() {
  return ::arrow::schema({
      ::arrow::field("tx_id", ::arrow::int64()), ::arrow::field("entry_id", ::arrow::int64()),
      ::arrow::field("type", ::arrow::int32()),  // Store enum as int32
      ::arrow::field("data", ::arrow::binary()), // Store serialized data as binary
  });
}

inline ::arrow::Result<std::shared_ptr<::arrow::Table>> Make(const std::vector<transaction::Transaction> &transactions,
                                                             ::arrow::MemoryPool *pool) {
  // Create Arrow arrays for each field
  ::arrow::Int64Builder tx_id_builder{pool};
  ::arrow::Int64Builder entry_id_builder{pool};
  ::arrow::Int32Builder type_builder{pool};
  ::arrow::BinaryBuilder data_builder{pool};

  // Process all transactions
  for (const auto &tx : transactions) {
    for (const auto &entry : tx.entries) {
      ARROW_RETURN_NOT_OK(tx_id_builder.Append(tx.id));
      ARROW_RETURN_NOT_OK(entry_id_builder.Append(entry.entry_id));
      ARROW_RETURN_NOT_OK(type_builder.Append(static_cast<int32_t>(entry.type)));

      // Serialize the data based on type
      if (entry.type == transaction::LogEntry::Type::NEW_NODES ||
          entry.type == transaction::LogEntry::Type::DELETE_NODES) {
        auto nodes = std::get<std::vector<Node>>(entry.data);
        auto table_result = node_schema::Make(nodes, pool);
        if (!table_result.ok()) {
          return table_result.status();
        }
        auto table = table_result.ValueOrDie();
        // Serialize the table to a buffer
        std::shared_ptr<::arrow::Buffer> buffer;
        ARROW_ASSIGN_OR_RAISE(auto output_stream, ::arrow::io::BufferOutputStream::Create());
        ARROW_ASSIGN_OR_RAISE(auto writer, ::arrow::ipc::MakeFileWriter(output_stream, table->schema()));
        ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
        ARROW_RETURN_NOT_OK(writer->Close());
        ARROW_ASSIGN_OR_RAISE(buffer, output_stream->Finish());
        ARROW_RETURN_NOT_OK(data_builder.Append(buffer->data(), buffer->size()));
      } else {
        auto edges = std::get<std::vector<Edge>>(entry.data);
        auto table_result = edge_schema::Make(edges, pool);
        if (!table_result.ok()) {
          return table_result.status();
        }
        auto table = table_result.ValueOrDie();
        // Serialize the table to a buffer
        std::shared_ptr<::arrow::Buffer> buffer;
        ARROW_ASSIGN_OR_RAISE(auto output_stream, ::arrow::io::BufferOutputStream::Create());
        ARROW_ASSIGN_OR_RAISE(auto writer, ::arrow::ipc::MakeFileWriter(output_stream, table->schema()));
        ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
        ARROW_RETURN_NOT_OK(writer->Close());
        ARROW_ASSIGN_OR_RAISE(buffer, output_stream->Finish());
        ARROW_RETURN_NOT_OK(data_builder.Append(buffer->data(), buffer->size()));
      }
    }
  }

  // Finish building arrays
  std::shared_ptr<::arrow::Array> tx_id_array, entry_id_array, type_array, data_array;
  ARROW_RETURN_NOT_OK(tx_id_builder.Finish(&tx_id_array));
  ARROW_RETURN_NOT_OK(entry_id_builder.Finish(&entry_id_array));
  ARROW_RETURN_NOT_OK(type_builder.Finish(&type_array));
  ARROW_RETURN_NOT_OK(data_builder.Finish(&data_array));

  // Create and return the table
  return ::arrow::Table::Make(Schema(), {tx_id_array, entry_id_array, type_array, data_array});
}

} // namespace log_schema
} // namespace memgraph