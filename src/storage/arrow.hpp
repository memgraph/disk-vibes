#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <memory>
#include <string>

namespace memgraph::arrow {

inline auto GPageMeta() {
  auto meta = ::arrow::key_value_metadata({"version"}, {"0.0.1"});
  return meta;
};

inline ::arrow::Result<std::shared_ptr<::arrow::Buffer>> TableToBuffer(const std::shared_ptr<::arrow::Table> &table) {
  ARROW_ASSIGN_OR_RAISE(auto sink, ::arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer, ::arrow::ipc::MakeFileWriter(sink, table->schema()));
  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());
  return sink->Finish();
}

::arrow::Status WriteTableToFile(const std::string &path, const std::shared_ptr<::arrow::Table> &table) {
  ARROW_ASSIGN_OR_RAISE(auto outfile, ::arrow::io::FileOutputStream::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto writer, ::arrow::ipc::MakeFileWriter(outfile.get(), table->schema()));
  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());
  return outfile->Close();
}

::arrow::Result<std::shared_ptr<::arrow::Table>> ReadTableFromFile(const std::string &path) {
  ARROW_ASSIGN_OR_RAISE(auto infile, ::arrow::io::ReadableFile::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto reader, ::arrow::ipc::RecordBatchFileReader::Open(infile));
  std::vector<std::shared_ptr<::arrow::RecordBatch>> batches;
  for (int i = 0; i < reader->num_record_batches(); i++) {
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(i));
    batches.push_back(batch);
  }
  return ::arrow::Table::FromRecordBatches(batches);
}

::arrow::Result<std::shared_ptr<::arrow::Buffer>> SerializeTable(const std::shared_ptr<::arrow::Table> &table) {
  ARROW_ASSIGN_OR_RAISE(auto outstream, ::arrow::io::BufferOutputStream::Create(0, ::arrow::default_memory_pool()));
  ARROW_ASSIGN_OR_RAISE(auto writer, ::arrow::ipc::MakeFileWriter(outstream.get(), table->schema()));
  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());
  return outstream->Finish();
}

} // namespace memgraph::arrow