#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <vector>

#include "arrow.hpp"
#include "edge.hpp"
#include "edge_schema.hpp"
#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <spdlog/spdlog.h>

namespace memgraph::epage_arrow {

class Page {
public:
  explicit Page(const std::filesystem::path &filepath, int64_t batch_size = 1)
      : batch_size_(batch_size), filepath_(filepath) {
    edges_.reserve(batch_size_);
  }
  explicit Page(std::vector<Edge> edges, const std::filesystem::path &filepath)
      : edges_(std::move(edges)), filepath_(filepath) {}
  void Add(const Edge &edge) { edges_.push_back(edge); }
  const std::vector<Edge> &Edges() const { return edges_; }

  ::arrow::Status Serialize() const {
    auto table_result = edge_schema::Make(edges_, ::arrow::default_memory_pool());
    if (!table_result.ok()) {
      return table_result.status();
    }
    auto table = table_result.ValueOrDie();
    return memgraph::arrow::WriteTableToFile(filepath_, table);
  }

  static ::arrow::Result<Page> Deserialize(const std::filesystem::path &filepath_, int64_t batch_size_) {
    ARROW_ASSIGN_OR_RAISE(auto input, ::arrow::io::ReadableFile::Open(filepath_));
    ARROW_ASSIGN_OR_RAISE(auto reader, ::arrow::ipc::RecordBatchFileReader::Open(input));
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(0));
    std::vector<Edge> edges;
    edges.reserve(batch_size_);
    auto id_array = std::static_pointer_cast<::arrow::Int64Array>(batch->column(0));
    auto src_array = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
    auto dst_array = std::static_pointer_cast<::arrow::Int64Array>(batch->column(2));
    auto type_array = std::static_pointer_cast<::arrow::StringArray>(batch->column(3));
    auto props_array = std::static_pointer_cast<::arrow::StringArray>(batch->column(4));
    auto ts_array = std::static_pointer_cast<::arrow::TimestampArray>(batch->column(5));
    for (int64_t i = 0; i < batch->num_rows(); ++i) {
      edges.emplace_back(id_array->Value(i), src_array->Value(i), dst_array->Value(i), type_array->GetString(i),
                         props_array->GetString(i), ts_array->Value(i));
    }
    return Page(std::move(edges), filepath_);
  }

private:
  int64_t batch_size_;
  std::vector<Edge> edges_;
  std::string filepath_;
};

inline ::arrow::Status PrintPage(const std::shared_ptr<::arrow::Table> &page) {
  auto schema = page->schema();
  spdlog::trace("== Schema metadata ==");
  if (schema->metadata()) {
    for (int i = 0; i < schema->metadata()->size(); ++i) {
      spdlog::trace("  {} = {}", schema->metadata()->key(i), schema->metadata()->value(i));
    }
  }

  auto ids = std::static_pointer_cast<::arrow::Int64Array>(page->column(0)->chunk(0));
  auto srcs = std::static_pointer_cast<::arrow::Int64Array>(page->column(1)->chunk(0));
  auto dsts = std::static_pointer_cast<::arrow::Int64Array>(page->column(2)->chunk(0));
  auto types = std::static_pointer_cast<::arrow::StringArray>(page->column(3)->chunk(0));
  auto props = std::static_pointer_cast<::arrow::StringArray>(page->column(4)->chunk(0));
  auto ts_arr = std::static_pointer_cast<::arrow::TimestampArray>(page->column(5)->chunk(0));

  for (int64_t row = 0; row < page->num_rows(); ++row) {
    spdlog::trace("row {}: {}, {} -> {} [{}], {}, {}", row, ids->Value(row), srcs->Value(row), dsts->Value(row),
                  types->GetString(row), props->GetString(row), ts_arr->Value(row));
  }
  return ::arrow::Status::OK();
}

inline ::arrow::Status ReadAndPrint(const std::string &path) {
  ARROW_ASSIGN_OR_RAISE(auto infile, ::arrow::io::ReadableFile::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto reader, ::arrow::ipc::RecordBatchFileReader::Open(infile));

  auto schema = reader->schema();

  spdlog::trace("== Schema metadata ==");
  if (schema->metadata()) {
    for (int i = 0; i < schema->metadata()->size(); ++i) {
      spdlog::trace("  {} = {}", schema->metadata()->key(i), schema->metadata()->value(i));
    }
  }

  for (int i = 0; i < reader->num_record_batches(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(i));

    auto ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(0));
    auto srcs = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
    auto dsts = std::static_pointer_cast<::arrow::Int64Array>(batch->column(2));
    auto types = std::static_pointer_cast<::arrow::StringArray>(batch->column(3));
    auto props = std::static_pointer_cast<::arrow::StringArray>(batch->column(4));
    auto ts_arr = std::static_pointer_cast<::arrow::TimestampArray>(batch->column(5));

    for (int64_t row = 0; row < batch->num_rows(); ++row) {
      spdlog::trace("row {}: {}, {} -> {} [{}], {}, {}", row, ids->Value(row), srcs->Value(row), dsts->Value(row),
                    types->GetString(row), props->GetString(row), ts_arr->Value(row));
    }
  }
  return ::arrow::Status::OK();
}

} // namespace memgraph::epage_arrow