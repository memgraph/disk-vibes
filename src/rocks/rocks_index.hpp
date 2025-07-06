#pragma once

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

namespace memgraph::experimental::rocks {

// Helper to format the reverse index key
inline std::string FormatReverseKey(char chunk_type, int64_t entity_id, int64_t txid) {
  std::stringstream ss;
  ss << "R" << chunk_type << "__" << entity_id << "__" << txid;
  return ss.str();
}

// Helper to parse the reverse index key
inline bool ParseReverseKey(const std::string &key, char &chunk_type, int64_t &entity_id, int64_t &txid) {
  if (key.size() < 3 || key[0] != 'R')
    return false;

  chunk_type = key[1];
  if (chunk_type != 'N' && chunk_type != 'E')
    return false;

  size_t pos1 = key.find("__", 2);
  if (pos1 == std::string::npos)
    return false;

  size_t pos2 = key.find("__", pos1 + 2);
  if (pos2 == std::string::npos)
    return false;

  try {
    entity_id = std::stoll(key.substr(pos1 + 2, pos2 - pos1 - 2));
    txid = std::stoll(key.substr(pos2 + 2));
    return true;
  } catch (...) {
    return false;
  }
}

// Helper to serialize chunk IDs into a comma-separated string
inline std::string SerializeChunkIds(const std::vector<int64_t> &chunk_ids) {
  if (chunk_ids.empty())
    return "";

  std::stringstream ss;
  ss << chunk_ids[0];
  for (size_t i = 1; i < chunk_ids.size(); ++i) {
    ss << "," << chunk_ids[i];
  }
  return ss.str();
}

// Helper to deserialize chunk IDs from a comma-separated string
inline std::vector<int64_t> DeserializeChunkIds(const std::string &value) {
  std::vector<int64_t> chunk_ids;
  std::stringstream ss(value);
  std::string item;

  while (std::getline(ss, item, ',')) {
    try {
      chunk_ids.push_back(std::stoll(item));
    } catch (...) {
      // Skip invalid entries
      continue;
    }
  }

  return chunk_ids;
}

class ReverseIndex {
public:
  explicit ReverseIndex(const std::string &db_path) : db_path_(db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path_, &db_);
    if (!status.ok()) {
      throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }
  }
  ~ReverseIndex() { delete db_; }

  // Add a chunk ID to the reverse index
  bool AddChunkId(char chunk_type, int64_t entity_id, int64_t txid, int64_t chunk_id) {
    std::string key = FormatReverseKey(chunk_type, entity_id, txid);
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);

    std::vector<int64_t> chunk_ids;
    if (status.ok()) {
      chunk_ids = DeserializeChunkIds(value);
    }

    // Check if chunk_id already exists
    if (std::find(chunk_ids.begin(), chunk_ids.end(), chunk_id) != chunk_ids.end()) {
      return true; // Already exists
    }

    chunk_ids.push_back(chunk_id);
    std::sort(chunk_ids.begin(),
              chunk_ids.end()); // Keep sorted for consistency

    return db_->Put(rocksdb::WriteOptions(), key, SerializeChunkIds(chunk_ids)).ok();
  }

  // Add multiple chunk IDs to the reverse index
  bool AddChunkIds(char chunk_type, int64_t entity_id, int64_t txid, const std::vector<int64_t> &chunk_ids) {
    if (chunk_ids.empty()) {
      return true; // Nothing to add
    }

    std::string key = FormatReverseKey(chunk_type, entity_id, txid);
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);

    std::vector<int64_t> existing_chunk_ids;
    if (status.ok()) {
      existing_chunk_ids = DeserializeChunkIds(value);
    }

    // Add new chunk IDs that don't already exist
    for (int64_t chunk_id : chunk_ids) {
      if (std::find(existing_chunk_ids.begin(), existing_chunk_ids.end(), chunk_id) == existing_chunk_ids.end()) {
        existing_chunk_ids.push_back(chunk_id);
      }
    }

    std::sort(existing_chunk_ids.begin(),
              existing_chunk_ids.end()); // Keep sorted for consistency

    return db_->Put(rocksdb::WriteOptions(), key, SerializeChunkIds(existing_chunk_ids)).ok();
  }

  // Get all chunk IDs for an entity
  std::vector<int64_t> GetChunkIds(char chunk_type, int64_t entity_id, int64_t txid) {
    std::string key = FormatReverseKey(chunk_type, entity_id, txid);
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);

    if (!status.ok()) {
      return {};
    }

    return DeserializeChunkIds(value);
  }

  // Remove a chunk ID from the reverse index
  bool RemoveChunkId(char chunk_type, int64_t entity_id, int64_t txid, int64_t chunk_id) {
    std::string key = FormatReverseKey(chunk_type, entity_id, txid);
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);

    if (!status.ok()) {
      return false;
    }

    std::vector<int64_t> chunk_ids = DeserializeChunkIds(value);
    auto it = std::find(chunk_ids.begin(), chunk_ids.end(), chunk_id);
    if (it == chunk_ids.end()) {
      return true; // Already removed
    }

    chunk_ids.erase(it);

    if (chunk_ids.empty()) {
      return db_->Delete(rocksdb::WriteOptions(), key).ok();
    } else {
      return db_->Put(rocksdb::WriteOptions(), key, SerializeChunkIds(chunk_ids)).ok();
    }
  }

  // Get all entity IDs of a specific type
  std::vector<int64_t> GetAllEntityIds(char chunk_type) {
    std::vector<int64_t> entity_ids;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions());

    std::string prefix = "R" + std::string(1, chunk_type) + "__";
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
      char type;
      int64_t entity_id, txid;
      if (ParseReverseKey(it->key().ToString(), type, entity_id, txid)) {
        entity_ids.push_back(entity_id);
      }
    }

    delete it;
    return entity_ids;
  }

  // Get all transaction IDs for an entity
  std::vector<int64_t> GetTransactionIds(char chunk_type, int64_t entity_id) {
    std::vector<int64_t> txids;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions());

    std::string prefix = "R" + std::string(1, chunk_type) + "__" + std::to_string(entity_id) + "__";
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
      char type;
      int64_t eid, txid;
      if (ParseReverseKey(it->key().ToString(), type, eid, txid)) {
        txids.push_back(txid);
      }
    }

    delete it;
    return txids;
  }

private:
  std::string db_path_;
  rocksdb::DB *db_;
};

} // namespace memgraph::experimental::rocks