#pragma once

#include <atomic>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/writer.h>
#include <parquet/arrow/writer.h>
#include <spdlog/spdlog.h>

#include "edge.hpp"
#include "graph.hpp"
#include "isolation_level.hpp"
#include "node.hpp"
#include "schema_log_entry.hpp"
#include "transaction_types.hpp"

namespace memgraph {

using Node = memgraph::Node;

// TODO(gitbuda): Implemnt GC with the actual cleanup polich to choose (1. cleanup 2. archive).
class TransactionalGraph {
public:
  class CommitLog {
  public:
    explicit CommitLog(const std::filesystem::path &log_dir, size_t batch_size = 1)
        : log_dir_(log_dir), next_transaction_id_(1), next_entry_id_(1), batch_size_(batch_size) {
      std::filesystem::create_directories(log_dir_);
      spdlog::trace("Created commit log directory: {}", log_dir_.string());
    }

    int64_t BeginTransaction() {
      std::lock_guard<std::mutex> lock(log_mutex_);
      int64_t tx_id = next_transaction_id_++;
      active_transactions_[tx_id] =
          transaction::Transaction{.id = tx_id, .state = transaction::TransactionState::ACTIVE, .entries = {}};
      spdlog::trace("Started transaction {}", tx_id);
      return tx_id;
    }

    ::arrow::Status Append(int64_t tx_id, const transaction::LogEntry &entry) {
      std::lock_guard<std::mutex> lock(log_mutex_);
      auto it = active_transactions_.find(tx_id);
      if (it == active_transactions_.end()) {
        return ::arrow::Status::Invalid("Transaction not found");
      }
      if (it->second.state != transaction::TransactionState::ACTIVE) {
        return ::arrow::Status::Invalid("Transaction is not active");
      }
      it->second.entries.push_back(entry);
      spdlog::trace("Appended entry {} to transaction {} (type: {})", entry.entry_id, tx_id,
                    (entry.type == transaction::LogEntry::Type::NEW_NODES      ? "ADD_NODES"
                     : entry.type == transaction::LogEntry::Type::DELETE_NODES ? "DELETE_NODES"
                     : entry.type == transaction::LogEntry::Type::NEW_EDGES    ? "NEW_EDGES"
                                                                               : "DELETE_EDGES"));
      return ::arrow::Status::OK();
    }

    ::arrow::Status Commit(int64_t tx_id) {
      std::lock_guard<std::mutex> lock(log_mutex_);
      auto it = active_transactions_.find(tx_id);
      if (it == active_transactions_.end()) {
        return ::arrow::Status::Invalid("Transaction not found");
      }
      if (it->second.state != transaction::TransactionState::ACTIVE) {
        return ::arrow::Status::Invalid("Transaction is not active");
      }
      it->second.state = transaction::TransactionState::COMMITTED;
      committed_transactions_[tx_id] = std::move(it->second);
      active_transactions_.erase(it);
      // TODO(gitbuda): Also add regular write policy, combine data and time policies.
      // Check if we need to write a new batch file
      if (committed_transactions_.size() >= batch_size_) {
        ARROW_RETURN_NOT_OK(WriteBatchFile());
      }
      return ::arrow::Status::OK();
    }

    ::arrow::Status Abort(int64_t tx_id) {
      // TODO(gitbuda): Implement ATOMICITY (Abort should revert any changes done by transaction).
      std::lock_guard<std::mutex> lock(log_mutex_);
      auto it = active_transactions_.find(tx_id);
      if (it == active_transactions_.end()) {
        spdlog::error("Failed to abort transaction {}: Transaction not found", tx_id);
        return ::arrow::Status::Invalid("Transaction not found");
      }
      if (it->second.state != transaction::TransactionState::ACTIVE) {
        spdlog::error("Failed to abort transaction {}: Transaction is not active", tx_id);
        return ::arrow::Status::Invalid("Transaction is not active");
      }
      it->second.state = transaction::TransactionState::ABORTED;
      aborted_transactions_[tx_id] = std::move(it->second);
      active_transactions_.erase(it);
      spdlog::trace("Aborted transaction {} with {} entries", tx_id, aborted_transactions_[tx_id].entries.size());
      return ::arrow::Status::OK();
    }

    ::arrow::Result<std::vector<transaction::LogEntry>> GetTransactionEntries(int64_t tx_id) {
      std::lock_guard<std::mutex> lock(log_mutex_);
      // Check active transactions
      auto active_it = active_transactions_.find(tx_id);
      if (active_it != active_transactions_.end()) {
        spdlog::trace("Retrieved {} entries from active transaction {}", active_it->second.entries.size(), tx_id);
        return active_it->second.entries;
      }
      // Check committed transactions
      auto committed_it = committed_transactions_.find(tx_id);
      if (committed_it != committed_transactions_.end()) {
        spdlog::trace("Retrieved {} entries from committed transaction {}", committed_it->second.entries.size(), tx_id);
        return committed_it->second.entries;
      }
      // Check aborted transactions
      auto aborted_it = aborted_transactions_.find(tx_id);
      if (aborted_it != aborted_transactions_.end()) {
        spdlog::trace("Retrieved {} entries from aborted transaction {}", aborted_it->second.entries.size(), tx_id);
        return aborted_it->second.entries;
      }
      spdlog::error("Failed to get entries for transaction {}: Transaction not found", tx_id);
      return ::arrow::Status::Invalid("Transaction not found");
    }

    ::arrow::Result<transaction::TransactionState> GetTransactionState(int64_t tx_id) {
      std::lock_guard<std::mutex> lock(log_mutex_);
      if (active_transactions_.contains(tx_id)) {
        spdlog::trace("Transaction {} is ACTIVE", tx_id);
        return transaction::TransactionState::ACTIVE;
      }
      if (committed_transactions_.contains(tx_id)) {
        spdlog::trace("Transaction {} is COMMITTED", tx_id);
        return transaction::TransactionState::COMMITTED;
      }
      if (aborted_transactions_.contains(tx_id)) {
        spdlog::trace("Transaction {} is ABORTED", tx_id);
        return transaction::TransactionState::ABORTED;
      }
      spdlog::error("Failed to get state for transaction {}: Transaction not found", tx_id);
      return ::arrow::Status::Invalid("Transaction not found");
    }

    std::vector<int64_t> GetActiveTransactionIds() {
      std::lock_guard<std::mutex> lock(log_mutex_);
      std::vector<int64_t> ids;
      ids.reserve(active_transactions_.size());
      for (const auto &[id, _] : active_transactions_) {
        ids.push_back(id);
      }
      return ids;
    }

    int64_t GetNextEntryId() { return next_entry_id_++; }

    ::arrow::Result<int64_t> GetLastCommittedTransactionId(int64_t visible_from_tx) {
      std::lock_guard<std::mutex> lock(log_mutex_);
      int64_t last_committed = 0;
      for (const auto &[tx_id, _] : committed_transactions_) {
        if (tx_id > last_committed) {
          last_committed = tx_id;
        }
      }
      return last_committed;
    }

    ::arrow::Status LockPages(int64_t tx_id, const std::unordered_set<int64_t> &page_ids) {
      std::lock_guard<std::mutex> lock(page_locks_mutex_);
      for (int64_t page_id : page_ids) {
        if (IsPageLocked(page_id, tx_id)) {
          spdlog::trace("Page {} is locked by another transaction", page_id);
          return ::arrow::Status::Invalid("Page ", page_id, " is locked by another transaction");
        }
        active_page_locks_[tx_id].insert(page_id);
        spdlog::trace("Transaction {} locked page {}", tx_id, page_id);
      }
      return ::arrow::Status::OK();
    }

    void UnlockPages(int64_t tx_id) {
      std::lock_guard<std::mutex> lock(page_locks_mutex_);
      if (active_page_locks_.contains(tx_id)) {
        spdlog::trace("Transaction {} unlocked all pages", tx_id);
        active_page_locks_.erase(tx_id);
      }
    }

  private:
    bool IsPageLocked(int64_t page_id, int64_t current_tx_id) {
      for (const auto &[tx_id, pages] : active_page_locks_) {
        if (tx_id != current_tx_id && pages.contains(page_id)) {
          spdlog::trace("Page {} is locked by transaction {}", page_id, tx_id);
          return true;
        }
      }
      return false;
    }

    ::arrow::Status WriteBatchFile() {
      if (committed_transactions_.empty()) {
        return ::arrow::Status::OK();
      }

      // Find the min and max transaction IDs in this batch
      int64_t min_tx_id = std::numeric_limits<int64_t>::max();
      int64_t max_tx_id = std::numeric_limits<int64_t>::min();
      for (const auto &[tx_id, _] : committed_transactions_) {
        min_tx_id = std::min(min_tx_id, tx_id);
        max_tx_id = std::max(max_tx_id, tx_id);
      }

      // Create a new batch file with transaction ID range
      std::stringstream filename;
      filename << "tx_" << min_tx_id << "_" << max_tx_id << ".parquet";
      auto filepath = log_dir_ / filename.str();

      // Convert committed transactions to vector
      std::vector<transaction::Transaction> transactions;
      transactions.reserve(committed_transactions_.size());
      for (const auto &[tx_id, tx] : committed_transactions_) {
        if (!written_transactions_.contains(tx_id)) {
          transactions.push_back(tx);
          written_transactions_.insert(tx_id);
        }
      }

      // Create table using log schema
      auto table_result = log_schema::Make(transactions, ::arrow::default_memory_pool());
      if (!table_result.ok()) {
        return table_result.status();
      }
      auto table = table_result.ValueOrDie();

      // Write to Parquet file
      std::shared_ptr<::arrow::io::FileOutputStream> outfile;
      ARROW_ASSIGN_OR_RAISE(outfile, ::arrow::io::FileOutputStream::Open(filepath.string()));
      ARROW_RETURN_NOT_OK(::parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(), outfile));
      ARROW_RETURN_NOT_OK(outfile->Close());

      spdlog::trace("Wrote batch file: {}", filepath.string());
      return ::arrow::Status::OK();
    }

    const std::filesystem::path log_dir_;
    std::mutex log_mutex_;
    std::atomic<int64_t> next_transaction_id_;
    std::atomic<int64_t> next_entry_id_;
    size_t batch_size_;
    // TODO(gitbuda): Copies of Transaction is super error prone (even during
    // construction stuff can go wrong) -> FIXME
    std::unordered_map<int64_t, transaction::Transaction> active_transactions_;
    std::unordered_map<int64_t, transaction::Transaction> committed_transactions_;
    std::unordered_map<int64_t, transaction::Transaction> aborted_transactions_;
    std::unordered_set<int64_t> written_transactions_;
    std::mutex page_locks_mutex_;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> active_page_locks_;
  };

  class Transaction {
  public:
    Transaction(Graph &graph, CommitLog &log, int64_t transaction_id, IsolationLevel isolation)
        : graph_(graph), log_(log), transaction_id_(transaction_id), isolation_(isolation),
          state_(transaction::TransactionState::ACTIVE) {}

    ::arrow::Status AddNodes(const std::vector<Node> &nodes) {
      if (isolation_ == IsolationLevel::NO_ISOLATION) {
        return graph_.AddNodes(nodes);
      }

      // Get page IDs for the nodes
      auto page_ids = graph_.GetPageIdsSetForNodes(nodes);
      // Try to lock the pages
      auto status = log_.LockPages(transaction_id_, page_ids);
      if (!status.ok()) {
        spdlog::trace("Failed to lock pages for transaction {}: {}", transaction_id_, status.ToString());
        return status;
      }
      // Add nodes to the graph
      status = graph_.AddNodes(nodes, transaction_id_);
      // Mark all pages as dirty
      for (const auto &page_id : page_ids) {
        MarkPageAsDirty(page_id);
      }
      if (!status.ok()) {
        spdlog::trace("Failed to add nodes for transaction {}: {}", transaction_id_, status.ToString());
        log_.UnlockPages(transaction_id_);
        return status;
      }
      // Add nodes to the log
      transaction::LogEntry entry{
          .entry_id = log_.GetNextEntryId(),
          .type = transaction::LogEntry::Type::NEW_NODES,
          .data = nodes,
      };
      status = log_.Append(transaction_id_, entry);
      if (!status.ok()) {
        spdlog::trace("Failed to append to log for transaction {}: {}", transaction_id_, status.ToString());
        log_.UnlockPages(transaction_id_);
        return status;
      }

      return ::arrow::Status::OK();
    }

    ::arrow::Status AddEdges(const std::vector<memgraph::Edge> &edges) {
      if (isolation_ == IsolationLevel::NO_ISOLATION) {
        return graph_.AddEdges(edges);
      }

      // Get page IDs for the edges
      auto page_ids = graph_.GetPageIdsSetForEdges(edges);
      // Try to lock the pages
      auto status = log_.LockPages(transaction_id_, page_ids);
      if (!status.ok()) {
        spdlog::trace("Failed to lock pages for transaction {}: {}", transaction_id_, status.ToString());
        return status;
      }

      // Add edges to the graph
      status = graph_.AddEdges(edges, transaction_id_);
      // Mark all pages as dirty
      for (const auto &page_id : page_ids) {
        MarkPageAsDirty(page_id);
      }

      if (!status.ok()) {
        spdlog::trace("Failed to add edges for transaction {}: {}", transaction_id_, status.ToString());
        log_.UnlockPages(transaction_id_);
        return status;
      }

      // Add edges to the log
      transaction::LogEntry entry{
          .entry_id = log_.GetNextEntryId(),
          .type = transaction::LogEntry::Type::NEW_EDGES,
          .data = edges,
      };
      status = log_.Append(transaction_id_, entry);
      if (!status.ok()) {
        spdlog::trace("Failed to append to log for transaction {}: {}", transaction_id_, status.ToString());
        log_.UnlockPages(transaction_id_);
        return status;
      }

      return ::arrow::Status::OK();
    }

    ::arrow::Status DeleteNodes(const std::vector<int64_t> &node_ids) {
      if (isolation_ == IsolationLevel::NO_ISOLATION) {
        return graph_.DeleteNodes(node_ids);
      }
      ARROW_RETURN_NOT_OK(graph_.DeleteNodes(node_ids, transaction_id_));
      // Create individual entries for each node to be deleted
      for (int64_t node_id : node_ids) {
        transaction::LogEntry entry{
            .entry_id = log_.GetNextEntryId(),
            .type = transaction::LogEntry::Type::DELETE_NODES,
            .data = std::vector<Node>{}, // Empty vector since we're deleting
        };
        ARROW_RETURN_NOT_OK(log_.Append(transaction_id_, entry));
      }
      return ::arrow::Status::OK();
    }

    ::arrow::Status DeleteEdges(const std::vector<int64_t> &edge_ids) {
      if (isolation_ == IsolationLevel::NO_ISOLATION) {
        return graph_.DeleteEdges(edge_ids);
      }
      ARROW_RETURN_NOT_OK(graph_.DeleteEdges(edge_ids, transaction_id_));
      // Create individual entries for each edge to be deleted
      for (int64_t edge_id : edge_ids) {
        transaction::LogEntry entry{
            .entry_id = log_.GetNextEntryId(),
            .type = transaction::LogEntry::Type::DELETE_EDGES,
            .data = std::vector<memgraph::Edge>{}, // Empty vector since we're deleting
        };
        ARROW_RETURN_NOT_OK(log_.Append(transaction_id_, entry));
      }
      return ::arrow::Status::OK();
    }

    ::arrow::Result<std::vector<Node>> GetNodes(int64_t start_id, int64_t end_id) {
      if (isolation_ == IsolationLevel::NO_ISOLATION) {
        return graph_.GetNodes(start_id, end_id);
      }

      // If transaction is aborted, return error because any query against aborted state is invalid
      if (state_ == transaction::TransactionState::ABORTED) {
        return ::arrow::Status::Invalid("Transaction has been aborted");
      }

      // Get the last committed transaction ID that this transaction can see
      auto last_committed_result = log_.GetLastCommittedTransactionId(transaction_id_);
      if (!last_committed_result.ok()) {
        return last_committed_result.status();
      }
      auto last_committed = last_committed_result.ValueOrDie();
      // Calculate page filenames based on the last committed transaction ID
      // TODO(gitbuda): The logic here and under the Graph::GetNodes is error prone -> IMPROVE.
      int64_t start_page = start_id / graph_.GetPageSize();
      int64_t end_page = (end_id - 1) / graph_.GetPageSize();
      std::vector<std::string> page_filenames;
      page_filenames.reserve(end_page - start_page + 1);
      for (int64_t page_id = start_page; page_id <= end_page; ++page_id) {
        if (IsPageDirty(page_id)) {
          page_filenames.push_back(graph_.GetNodeFilePath(page_id, transaction_id_).string());
        } else {
          page_filenames.push_back(graph_.GetNodeFilePath(page_id, last_committed).string());
        }
      }
      return graph_.GetNodes(start_id, end_id, last_committed, page_filenames);
    }

    ::arrow::Result<std::vector<memgraph::Edge>> GetEdges(int64_t start_id, int64_t end_id) {
      if (isolation_ == IsolationLevel::NO_ISOLATION) {
        return graph_.GetEdges(start_id, end_id);
      }

      // If transaction is aborted, return error because any query against aborted state is invalid
      if (state_ == transaction::TransactionState::ABORTED) {
        return ::arrow::Status::Invalid("Transaction has been aborted");
      }

      // Get the last committed transaction ID that this transaction can see
      auto last_committed_result = log_.GetLastCommittedTransactionId(transaction_id_);
      if (!last_committed_result.ok()) {
        return last_committed_result.status();
      }
      auto last_committed = last_committed_result.ValueOrDie();

      // Calculate page filenames based on the last committed transaction ID
      int64_t start_page = start_id / graph_.GetPageSize();
      int64_t end_page = (end_id - 1) / graph_.GetPageSize();
      std::vector<std::string> page_filenames;
      page_filenames.reserve(end_page - start_page + 1);

      for (int64_t page_id = start_page; page_id <= end_page; ++page_id) {
        if (IsPageDirty(page_id)) {
          page_filenames.push_back(graph_.GetEdgeFilePath(page_id, transaction_id_).string());
        } else {
          page_filenames.push_back(graph_.GetEdgeFilePath(page_id, last_committed).string());
        }
      }

      return graph_.GetEdges(start_id, end_id, last_committed, page_filenames);
    }

    ::arrow::Status Commit() {
      if (state_ == transaction::TransactionState::COMMITTED) {
        spdlog::trace("Transaction {} already committed", transaction_id_);
        return ::arrow::Status::OK();
      }

      spdlog::trace("COMMITTING: {} started", transaction_id_);
      if (isolation_ == IsolationLevel::NO_ISOLATION) {
        return ::arrow::Status::OK();
      }

      // Commit the transaction
      auto status = log_.Commit(transaction_id_);
      if (!status.ok()) {
        log_.UnlockPages(transaction_id_);
        return status;
      }

      // Unlock all pages
      log_.UnlockPages(transaction_id_);

      state_ = transaction::TransactionState::COMMITTED;
      spdlog::trace("COMMITTING: {} ended", transaction_id_);
      return ::arrow::Status::OK();
    }

    ::arrow::Status Abort() {
      if (isolation_ == IsolationLevel::NO_ISOLATION) {
        return ::arrow::Status::OK();
      }

      // Abort the transaction
      auto status = log_.Abort(transaction_id_);
      if (!status.ok()) {
        log_.UnlockPages(transaction_id_);
        return status;
      }

      // Unlock all pages
      log_.UnlockPages(transaction_id_);

      state_ = transaction::TransactionState::ABORTED;
      return ::arrow::Status::OK();
    }

    int64_t GetTransactionId() const { return transaction_id_; }
    IsolationLevel GetIsolationLevel() const { return isolation_; }

  private:
    Graph &graph_;
    CommitLog &log_;
    int64_t transaction_id_;
    IsolationLevel isolation_;
    transaction::TransactionState state_;

    std::unordered_set<int64_t> dirty_pages_; // Set of page IDs that have been modified in this transaction
    void MarkPageAsDirty(int64_t page_id) { dirty_pages_.insert(page_id); }
    bool IsPageDirty(int64_t page_id) const { return dirty_pages_.contains(page_id); }
    const std::unordered_set<int64_t> &GetDirtyPages() const { return dirty_pages_; }
    void ClearDirtyPages() { dirty_pages_.clear(); }
  };

  explicit TransactionalGraph(Graph &graph, size_t batch_size = 1)
      : graph_(graph), log_(graph.GetDirectory() / "logs", batch_size) {}

  ~TransactionalGraph() {
    // Abort any active transactions
    for (const auto &tx_id : log_.GetActiveTransactionIds()) {
      auto status = log_.Abort(tx_id);
      if (!status.ok()) {
        spdlog::error("Failed to abort transaction during cleanup: {}", status.ToString());
      }
    }
  }

  ::arrow::Result<Transaction> BeginTransaction(IsolationLevel isolation) {
    if (isolation == IsolationLevel::NO_ISOLATION) {
      return Transaction(graph_, log_, 0, isolation);
    }
    int64_t transaction_id = log_.BeginTransaction();
    return Transaction(graph_, log_, transaction_id, isolation);
  }

private:
  Graph &graph_;
  CommitLog log_;
};

} // namespace memgraph