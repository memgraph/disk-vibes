#pragma once

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <vector>

namespace memgraph::experimental::rocks {

constexpr char kDefaultColumnFamily[] = "default";

::arrow::Status PutArrowBuffer(const std::string &db_name, const std::string &key,
                               const std::shared_ptr<::arrow::Buffer> &buf) {
  ::rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.create_missing_column_families = true;

  // List of column families
  std::vector<::rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.push_back(::rocksdb::ColumnFamilyDescriptor(kDefaultColumnFamily, ::rocksdb::ColumnFamilyOptions()));

  // List of column family handles
  std::vector<::rocksdb::ColumnFamilyHandle *> handles_raw;
  ::rocksdb::DB *db_raw;

  ::rocksdb::Status s = ::rocksdb::DB::Open(opts, db_name, column_families, &handles_raw, &db_raw);
  if (!s.ok())
    return ::arrow::Status::IOError(s.ToString());

  // Wrap the raw DB pointer and handles in unique_ptrs
  std::unique_ptr<::rocksdb::DB> uptr_db(db_raw);
  std::vector<std::unique_ptr<::rocksdb::ColumnFamilyHandle>> handles;
  for (auto handle : handles_raw) {
    handles.emplace_back(handle);
  }

  s = uptr_db->Put(::rocksdb::WriteOptions{}, handles[0].get(), key,
                   ::rocksdb::Slice(reinterpret_cast<const char *>(buf->data()), buf->size()));
  if (!s.ok()) {
    return ::arrow::Status::IOError(s.ToString());
  }
  return ::arrow::Status::OK();
}

::arrow::Result<std::string> GetValue(const std::string &db_name, const std::string &key) {
  ::rocksdb::Options opts;
  opts.create_if_missing = false;
  opts.create_missing_column_families = true;

  // List of column families
  std::vector<::rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.push_back(::rocksdb::ColumnFamilyDescriptor(kDefaultColumnFamily, ::rocksdb::ColumnFamilyOptions()));

  // List of column family handles
  std::vector<::rocksdb::ColumnFamilyHandle *> handles_raw;
  ::rocksdb::DB *db_raw;

  ::rocksdb::Status s = ::rocksdb::DB::Open(opts, db_name, column_families, &handles_raw, &db_raw);
  if (!s.ok())
    return ::arrow::Status::IOError(s.ToString());

  // Wrap the raw DB pointer and handles in unique_ptrs
  std::unique_ptr<::rocksdb::DB> uptr_db(db_raw);
  std::vector<std::unique_ptr<::rocksdb::ColumnFamilyHandle>> handles;
  for (auto handle : handles_raw) {
    handles.emplace_back(handle);
  }

  std::string value;
  s = uptr_db->Get(::rocksdb::ReadOptions{}, handles[0].get(), key, &value);
  if (!s.ok()) {
    return ::arrow::Status::IOError(s.ToString());
  }
  return value;
}

::arrow::Result<std::shared_ptr<::arrow::Table>> GetArrowTable(const std::string &db_name, const std::string &key) {
  ARROW_ASSIGN_OR_RAISE(auto value, GetValue(db_name, key));
  auto buf = std::make_shared<::arrow::Buffer>(reinterpret_cast<const uint8_t *>(value.data()),
                                               static_cast<int64_t>(value.size()));
  auto source = std::make_shared<::arrow::io::BufferReader>(buf);
  ARROW_ASSIGN_OR_RAISE(auto reader, ::arrow::ipc::RecordBatchFileReader::Open(source));
  std::vector<std::shared_ptr<::arrow::RecordBatch>> batches;
  for (int b = 0; b < reader->num_record_batches(); ++b) {
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(b));
    batches.push_back(batch);
  }
  return ::arrow::Table::FromRecordBatches(batches);
}

} // namespace memgraph::experimental::rocks
