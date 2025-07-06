#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <spdlog/spdlog.h>

#include "edge.hpp"
#include "edge_schema.hpp"

namespace memgraph::epage_parquet {

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

    // Create a Parquet writer
    std::shared_ptr<::arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, ::arrow::io::FileOutputStream::Open(filepath_));

    // Configure Parquet writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    builder.encoding(parquet::Encoding::PLAIN);
    builder.disable_dictionary();
    auto writer_properties = builder.build();

    // Configure Arrow writer properties
    parquet::ArrowWriterProperties::Builder arrow_builder;
    arrow_builder.store_schema();
    auto arrow_properties = arrow_builder.build();

    // Write the table to Parquet
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(), outfile, 64 * 1024,
                                                   writer_properties, arrow_properties));

    // Ensure the file is properly closed
    ARROW_RETURN_NOT_OK(outfile->Close());

    return ::arrow::Status::OK();
  }

  static ::arrow::Result<Page> Deserialize(const std::filesystem::path &filepath, int64_t batch_size) {
    // Open the Parquet file
    std::shared_ptr<::arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, ::arrow::io::ReadableFile::Open(filepath));

    // Create a Parquet reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::FileReader::Make(::arrow::default_memory_pool(),
                                                         parquet::ParquetFileReader::Open(infile), &reader));

    // Read the table
    std::shared_ptr<::arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

    // Convert to edges
    std::vector<Edge> edges;
    edges.reserve(batch_size);
    auto id_array = std::static_pointer_cast<::arrow::Int64Array>(table->column(0)->chunk(0));
    auto src_array = std::static_pointer_cast<::arrow::Int64Array>(table->column(1)->chunk(0));
    auto dst_array = std::static_pointer_cast<::arrow::Int64Array>(table->column(2)->chunk(0));
    auto type_array = std::static_pointer_cast<::arrow::StringArray>(table->column(3)->chunk(0));
    auto props_array = std::static_pointer_cast<::arrow::StringArray>(table->column(4)->chunk(0));
    auto ts_array = std::static_pointer_cast<::arrow::TimestampArray>(table->column(5)->chunk(0));

    for (int64_t i = 0; i < table->num_rows(); ++i) {
      edges.emplace_back(id_array->Value(i), src_array->Value(i), dst_array->Value(i), type_array->GetString(i),
                         props_array->GetString(i), ts_array->Value(i));
    }

    return Page(std::move(edges), filepath);
  }

private:
  int64_t batch_size_;
  std::vector<Edge> edges_;
  std::filesystem::path filepath_;
};

::arrow::Status PrintPage(const std::shared_ptr<::arrow::Table> &page) {
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

::arrow::Status ReadAndPrint(const std::string &path) {
  // Open the Parquet file
  std::shared_ptr<::arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(infile, ::arrow::io::ReadableFile::Open(path));

  // Create a Parquet reader
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::FileReader::Make(::arrow::default_memory_pool(),
                                                       parquet::ParquetFileReader::Open(infile), &reader));

  // Read the table
  std::shared_ptr<::arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  auto schema = table->schema();
  spdlog::trace("== Schema metadata ==");
  if (schema->metadata()) {
    for (int i = 0; i < schema->metadata()->size(); ++i) {
      spdlog::trace("  {} = {}", schema->metadata()->key(i), schema->metadata()->value(i));
    }
  }

  auto ids = std::static_pointer_cast<::arrow::Int64Array>(table->column(0)->chunk(0));
  auto srcs = std::static_pointer_cast<::arrow::Int64Array>(table->column(1)->chunk(0));
  auto dsts = std::static_pointer_cast<::arrow::Int64Array>(table->column(2)->chunk(0));
  auto types = std::static_pointer_cast<::arrow::StringArray>(table->column(3)->chunk(0));
  auto props = std::static_pointer_cast<::arrow::StringArray>(table->column(4)->chunk(0));
  auto ts_arr = std::static_pointer_cast<::arrow::TimestampArray>(table->column(5)->chunk(0));

  for (int64_t row = 0; row < table->num_rows(); ++row) {
    spdlog::trace("row {}: {}, {} -> {} [{}], {}, {}", row, ids->Value(row), srcs->Value(row), dsts->Value(row),
                  types->GetString(row), props->GetString(row), ts_arr->Value(row));
  }
  return ::arrow::Status::OK();
}

} // namespace memgraph::epage_parquet