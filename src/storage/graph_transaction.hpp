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
    explicit CommitLog(const std::filesystem::path &log_dir, size_t commit_batch_size = 1)
        : log_dir_(log_dir), next_transaction_id_(1), next_entry_id_(1), commit_batch_size_(commit_batch_size) {
      std::filesystem::create_directories(log_dir_);
      spdlog::trace("Created commit log directory: {}", log_dir_.string());
    }

    int64_t BeginTransaction() {
      std::lock_guard<std::mutex> lock(log_mutex_);
      int64_t tx_id = next_transaction_id_++;
      transactions_[tx_id] =
          transaction::Transaction{.id = tx_id, .state = transaction::TransactionState::ACTIVE, .entries = {}};
      spdlog::trace("Started transaction {}", tx_id);
      return tx_id;
    }

    ::arrow::Status Append(int64_t tx_id, const transaction::LogEntry &entry) {
      std::lock_guard<std::mutex> lock(log_mutex_);
      auto it = transactions_.find(tx_id);
      if (it == transactions_.end()) {
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
      auto it = transactions_.find(tx_id);
      if (it == transactions_.end()) {
        return ::arrow::Status::Invalid("Transaction not found");
      }
      if (it->second.state != transaction::TransactionState::ACTIVE) {
        return ::arrow::Status::Invalid("Transaction is not active");
      }
      it->second.state = transaction::TransactionState::COMMITTED;
      // TODO(gitbuda): Also add regular write policy, combine data and time policies.
      // Check if we need to write a new batch file
      size_t committed_count = 0;
      for (const auto &[id, tx] : transactions_) {
        if (tx.state == transaction::TransactionState::COMMITTED) {
          committed_count++;
        }
      }
      if (committed_count >= commit_batch_size_) {
        ARROW_RETURN_NOT_OK(WriteBatchFile());
      }
      return ::arrow::Status::OK();
    }

    ::arrow::Status Abort(int64_t tx_id) {
      // TODO(gitbuda): Implement ATOMICITY (Abort should revert any changes done by transaction).
      std::lock_guard<std::mutex> lock(log_mutex_);
      auto it = transactions_.find(tx_id);
      if (it == transactions_.end()) {
        spdlog::error("Failed to abort transaction {}: Transaction not found", tx_id);
        return ::arrow::Status::Invalid("Transaction not found");
      }
      if (it->second.state != transaction::TransactionState::ACTIVE) {
        spdlog::error("Failed to abort transaction {}: Transaction is not active", tx_id);
        return ::arrow::Status::Invalid("Transaction is not active");
      }
      it->second.state = transaction::TransactionState::ABORTED;
      spdlog::trace("Aborted transaction {} with {} entries", tx_id, it->second.entries.size());
      return ::arrow::Status::OK();
    }

    ::arrow::Result<std::vector<transaction::LogEntry>> GetTransactionEntries(int64_t tx_id) {
      std::lock_guard<std::mutex> lock(log_mutex_);
      auto it = transactions_.find(tx_id);
      if (it == transactions_.end()) {
        spdlog::error("Failed to get entries for transaction {}: Transaction not found", tx_id);
        return ::arrow::Status::Invalid("Transaction not found");
      }
      spdlog::trace("Retrieved {} entries from transaction {} (state: {})", it->second.entries.size(), tx_id,
                    it->second.state == transaction::TransactionState::ACTIVE      ? "ACTIVE"
                    : it->second.state == transaction::TransactionState::COMMITTED ? "COMMITTED"
                                                                                   : "ABORTED");
      return it->second.entries;
    }

    ::arrow::Result<transaction::TransactionState> GetTransactionState(int64_t tx_id) {
      std::lock_guard<std::mutex> lock(log_mutex_);
      auto it = transactions_.find(tx_id);
      if (it == transactions_.end()) {
        spdlog::error("Failed to get state for transaction {}: Transaction not found", tx_id);
        return ::arrow::Status::Invalid("Transaction not found");
      }
      spdlog::trace("Transaction {} is {}", tx_id,
                    it->second.state == transaction::TransactionState::ACTIVE      ? "ACTIVE"
                    : it->second.state == transaction::TransactionState::COMMITTED ? "COMMITTED"
                                                                                   : "ABORTED");
      return it->second.state;
    }

    std::vector<int64_t> GetActiveTransactionIds() {
      std::lock_guard<std::mutex> lock(log_mutex_);
      std::vector<int64_t> ids;
      for (const auto &[id, tx] : transactions_) {
        if (tx.state == transaction::TransactionState::ACTIVE) {
          ids.push_back(id);
        }
      }
      return ids;
    }

    int64_t GetNextEntryId() { return next_entry_id_++; }

    ::arrow::Result<int64_t> GetLastCommittedTransactionId(int64_t visible_from_tx) {
      std::lock_guard<std::mutex> lock(log_mutex_);
      int64_t last_committed = 0;
      for (const auto &[id, tx] : transactions_) {
        if (tx.state == transaction::TransactionState::COMMITTED && id > last_committed) {
          last_committed = id;
        }
      }
      return last_committed;
    }

    ::arrow::Status LockPages(int64_t tx_id, const std::unordered_set<int64_t> &page_ids, bool is_node_pages) {
      std::lock_guard<std::mutex> lock(page_locks_mutex_);
      auto &active_locks = is_node_pages ? active_node_page_locks_ : active_edge_page_locks_;

      for (int64_t page_id : page_ids) {
        if (IsPageLocked(page_id, tx_id, is_node_pages)) {
          spdlog::trace("Page {} is locked by another transaction", page_id);
          return ::arrow::Status::Invalid("Page ", page_id, " is locked by another transaction");
        }
        active_locks[tx_id].insert(page_id);
        spdlog::trace("Transaction {} locked {} page {}", tx_id, is_node_pages ? "node" : "edge", page_id);
      }
      return ::arrow::Status::OK();
    }

    void UnlockPages(int64_t tx_id) {
      std::lock_guard<std::mutex> lock(page_locks_mutex_);
      if (active_node_page_locks_.contains(tx_id)) {
        spdlog::trace("Transaction {} unlocked all node pages", tx_id);
        active_node_page_locks_.erase(tx_id);
      }
      if (active_edge_page_locks_.contains(tx_id)) {
        spdlog::trace("Transaction {} unlocked all edge pages", tx_id);
        active_edge_page_locks_.erase(tx_id);
      }
    }

  private:
    bool IsPageLocked(int64_t page_id, int64_t current_tx_id, bool is_node_pages) {
      const auto &active_locks = is_node_pages ? active_node_page_locks_ : active_edge_page_locks_;
      for (const auto &[tx_id, pages] : active_locks) {
        if (tx_id != current_tx_id && pages.contains(page_id)) {
          spdlog::trace("Page {} is locked by transaction {}", page_id, tx_id);
          return true;
        }
      }
      return false;
    }

    ::arrow::Status WriteBatchFile() {
      // Find the min and max transaction IDs in this batch
      int64_t min_tx_id = std::numeric_limits<int64_t>::max();
      int64_t max_tx_id = std::numeric_limits<int64_t>::min();
      std::vector<transaction::Transaction> committed_txs;
      committed_txs.reserve(commit_batch_size_);

      for (const auto &[tx_id, tx] : transactions_) {
        if (tx.state == transaction::TransactionState::COMMITTED && !written_transactions_.contains(tx_id)) {
          min_tx_id = std::min(min_tx_id, tx_id);
          max_tx_id = std::max(max_tx_id, tx_id);
          committed_txs.push_back(tx);
          written_transactions_.insert(tx_id);
        }
      }

      if (committed_txs.empty()) {
        return ::arrow::Status::OK();
      }

      // Create a new batch file with transaction ID range
      std::stringstream filename;
      filename << "tx_" << min_tx_id << "_" << max_tx_id << ".parquet";
      auto filepath = log_dir_ / filename.str();

      // Create table using log schema
      auto table_result = log_schema::Make(committed_txs, ::arrow::default_memory_pool());
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
    size_t commit_batch_size_;
    std::unordered_map<int64_t, transaction::Transaction> transactions_;
    std::unordered_set<int64_t> written_transactions_;
    std::mutex page_locks_mutex_;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> active_node_page_locks_;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> active_edge_page_locks_;
  };

  class Transaction {
  public:
    Transaction(Graph &graph, CommitLog &log, int64_t transaction_id, IsolationLevel isolation)
        : graph_(graph), log_(log), transaction_id_(transaction_id), isolation_(isolation),
          state_(transaction::TransactionState::ACTIVE) {}

    // Delete copy constructor and assignment operator
    Transaction(const Transaction &) = delete;
    Transaction &operator=(const Transaction &) = delete;

    // Allow move construction but delete move assignment since we have reference members
    Transaction(Transaction &&) = default;
    Transaction &operator=(Transaction &&) = delete;

    ~Transaction() {
      if (transaction_id_ != 0) {
        // Only abort if not already committed/aborted
        if (state_ == transaction::TransactionState::ACTIVE) {
          auto status = Abort();
          if (!status.ok()) {
            spdlog::warn("Failed to abort transaction {} in destructor: {}", transaction_id_, status.ToString());
          }
        }
      }
    }

    ::arrow::Status AddNodes(const std::vector<Node> &nodes) {
      if (isolation_ == IsolationLevel::NO_ISOLATION) {
        return graph_.AddNodes(nodes);
      }

      // Get page IDs for the nodes
      auto page_ids = graph_.GetPageIdsSetForNodes(nodes);
      // Try to lock the pages
      auto status = log_.LockPages(transaction_id_, page_ids, true);
      if (!status.ok()) {
        spdlog::trace("Failed to lock pages for transaction {}: {}", transaction_id_, status.ToString());
        return status;
      }
      // Add nodes to the graph
      status = graph_.AddNodes(nodes, transaction_id_);
      if (!status.ok()) {
        spdlog::trace("Failed to add nodes for transaction {}: {}", transaction_id_, status.ToString());
        log_.UnlockPages(transaction_id_);
        return status;
      }
      // Mark all pages as dirty
      for (const auto &page_id : page_ids) {
        MarkPageAsDirty(page_id);
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
      auto status = log_.LockPages(transaction_id_, page_ids, false);
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

  ::arrow::Result<std::unique_ptr<Transaction>> BeginTransaction(IsolationLevel isolation) {
    // Get a new transaction ID
    int64_t tx_id = log_.BeginTransaction();

    // Create new transaction
    return std::make_unique<Transaction>(graph_, log_, tx_id, isolation);
  }

  size_t GetPageSize() const { return graph_.GetPageSize(); }

private:
  Graph &graph_;
  CommitLog log_;
};

} // namespace memgraph