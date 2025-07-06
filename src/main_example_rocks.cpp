#include <memory>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

#include "storage/common.hpp"

int main() {
  init_spdlog();

  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;

  // List of column families
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Default column family
  column_families.push_back(
      rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
  // Custom column family
  column_families.push_back(rocksdb::ColumnFamilyDescriptor("new_cf", rocksdb::ColumnFamilyOptions()));

  // List of column family handles
  std::vector<rocksdb::ColumnFamilyHandle *> handles_raw;
  rocksdb::TransactionDB *db_raw;

  // Open DB with column families
  rocksdb::Status s = rocksdb::TransactionDB::Open(options, rocksdb::TransactionDBOptions(), "my_cf_database",
                                                   column_families, &handles_raw, &db_raw);
  if (!s.ok()) {
    spdlog::error("Error opening database: {}", s.ToString());
    return 1;
  }

  // Wrap the raw DB pointer in a unique_ptr
  std::unique_ptr<rocksdb::TransactionDB> db(db_raw);

  // Wrap the raw handles in unique_ptrs
  std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> handles;
  for (auto handle : handles_raw) {
    handles.emplace_back(handle);
  }

  // Example 1: Successful transaction with commit
  {
    spdlog::info("Example 1: Successful transaction with commit");
    rocksdb::Transaction *txn = db->BeginTransaction(rocksdb::WriteOptions());

    // Put some values in the transaction
    txn->Put(handles[0].get(), "txn_key1", "value1");
    txn->Put(handles[1].get(), "txn_key2", "value2");

    // Commit the transaction
    s = txn->Commit();
    if (!s.ok()) {
      spdlog::error("Error committing transaction: {}", s.ToString());
    }
    delete txn;

    // Verify the committed values
    std::string value1, value2;
    db->Get(rocksdb::ReadOptions(), handles[0].get(), "txn_key1", &value1);
    db->Get(rocksdb::ReadOptions(), handles[1].get(), "txn_key2", &value2);
    spdlog::info("After commit - Default CF - txn_key1: {}", value1);
    spdlog::info("After commit - Custom CF - txn_key2: {}", value2);
  }

  // Example 2: Failed transaction with rollback
  {
    spdlog::info("Example 2: Failed transaction with rollback");
    rocksdb::Transaction *txn = db->BeginTransaction(rocksdb::WriteOptions());

    // Put some values in the transaction
    txn->Put(handles[0].get(), "rollback_key1", "value1");
    txn->Put(handles[1].get(), "rollback_key2", "value2");

    // Simulate a failure and rollback
    spdlog::info("Rolling back transaction...");
    txn->Rollback();
    delete txn;

    // Verify the values are not present (rollback worked)
    std::string value1, value2;
    rocksdb::Status s1 = db->Get(rocksdb::ReadOptions(), handles[0].get(), "rollback_key1", &value1);
    rocksdb::Status s2 = db->Get(rocksdb::ReadOptions(), handles[1].get(), "rollback_key2", &value2);
    spdlog::info("After rollback - Default CF - rollback_key1: {}", s1.ok() ? value1 : "not found");
    spdlog::info("After rollback - Custom CF - rollback_key2: {}", s2.ok() ? value2 : "not found");
  }

  // Example 3: Transaction with conflict detection
  {
    spdlog::info("Example 3: Transaction with conflict detection");
    rocksdb::Transaction *txn1 = db->BeginTransaction(rocksdb::WriteOptions());
    rocksdb::Transaction *txn2 = db->BeginTransaction(rocksdb::WriteOptions());

    txn1->Put(handles[0].get(), "conflict_key", "value1");
    txn2->Put(handles[0].get(), "conflict_key", "value2");

    auto s2 = txn2->Commit();
    spdlog::info("Second transaction commit status: {}", s2.ToString());

    auto s1 = txn1->Commit();
    spdlog::info("First transaction commit status: {}", s1.ToString());

    std::string value;
    db->Get(rocksdb::ReadOptions(), handles[0].get(), "conflict_key", &value);
    spdlog::info("Final value of conflict_key: {}", value);

    delete txn1;
    delete txn2;
  }

  return 0;
}
