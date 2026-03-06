#pragma once
/*
 * log_manager.h – Write-Ahead Log (WAL)
 *
 * Guarantees (ARIES WAL rules)
 * ────────────────────────────
 *  1. WAL rule: a dirty page must NOT be flushed to disk until the log
 *     record for the modification has been appended to stable storage.
 *  2. Commit rule: a transaction is committed only after its COMMIT log
 *     record has been flushed.
 *
 * LogManager
 * ──────────
 * • Maintains an in-memory log buffer.
 * • Appends log records (assign LSN = next available byte offset in log file).
 * • Flushes buffer to disk on demand (Flush), or when buffer is nearly full.
 * • Provides a read interface for the RecoveryManager.
 *
 * Thread safety: append_latch_ serialises appends; flush_latch_ for flushes.
 */

#include "recovery/log_record.h"
#include "storage/disk_manager.h"
#include <mutex>
#include <vector>

namespace ydb {

class LogManager {
public:
    explicit LogManager(DiskManager* disk);
    ~LogManager();

    // ── Append helpers ──────────────────────────────────────────────────────
    lsn_t AppendBegin(txn_id_t txn_id, lsn_t prev_lsn);
    lsn_t AppendCommit(txn_id_t txn_id, lsn_t prev_lsn);
    lsn_t AppendAbort(txn_id_t txn_id, lsn_t prev_lsn);
    lsn_t AppendInsert(txn_id_t txn_id, lsn_t prev_lsn,
                        const RID& rid, const char* data, uint32_t size);
    lsn_t AppendDelete(txn_id_t txn_id, lsn_t prev_lsn,
                        const RID& rid, const char* old_data, uint32_t old_size);
    lsn_t AppendUpdate(txn_id_t txn_id, lsn_t prev_lsn,
                        const RID& rid,
                        const char* old_data, uint32_t old_size,
                        const char* new_data, uint32_t new_size);
    lsn_t AppendCLR(txn_id_t txn_id, lsn_t prev_lsn,
                     const RID& rid, lsn_t undo_next_lsn);
    lsn_t AppendCheckpoint(txn_id_t txn_id);

    // Force all buffered log records to stable storage.
    void Flush();

    // Current persistent LSN (everything < this is on disk).
    lsn_t GetPersistentLSN() const { return persistent_lsn_; }

    // ── Read interface for recovery ─────────────────────────────────────────
    // Read the log record that starts at `lsn`.
    // Returns false if lsn is past the end of the log.
    bool ReadRecord(lsn_t lsn, LogRecord* out);

    // Total bytes written to the log file (= next LSN to assign).
    lsn_t GetNextLSN() const { return next_lsn_; }

private:
    DiskManager* disk_;
    std::mutex   append_latch_;

    lsn_t        next_lsn_       = 0;   // byte offset in log file
    lsn_t        persistent_lsn_ = -1;  // last LSN written to disk

    std::vector<char> log_buf_;  // in-memory write buffer
    size_t            buf_used_  = 0;

    // Low-level: serialise `rec` into the buffer and return its LSN.
    lsn_t Append(LogRecord& rec);

    // Flush if buffer is > 75% full.
    void MaybeFlush();

    static constexpr size_t BUF_SIZE = LOG_BUFFER_SIZE;
};

} // namespace ydb
