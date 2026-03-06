#pragma once
/*
 * recovery_manager.h – ARIES crash-recovery algorithm
 *
 * ARIES (Algorithms for Recovery and Isolation Exploiting Semantics)
 * Three phases executed in order after a crash / startup:
 *
 * 1. Analysis
 * ───────────
 *  Scan the log from the last checkpoint LSN to the end.
 *  Rebuild two tables:
 *    ATT  (Active Transaction Table)  – transactions not yet committed/aborted
 *    DPT  (Dirty Page Table)          – pages modified but possibly not flushed
 *                                       maps page_id → recLSN (first LSN that
 *                                       dirtied this page after last checkpoint)
 *
 * 2. Redo
 * ───────
 *  Start from min(recLSN) across all entries in DPT.
 *  Replay every UPDATE / INSERT / DELETE in the log.
 *  Skip a record if:
 *    • The page is not in the DPT  (already flushed before crash), OR
 *    • page.pageLSN >= record.lsn   (already on disk)
 *
 * 3. Undo
 * ───────
 *  Undo all loser transactions (those in ATT with state != COMMITTED).
 *  Process in reverse LSN order (priority queue ordered by LSN descending).
 *  For each undo:
 *    • Write a CLR (Compensation Log Record) with undo_next_lsn = prevLSN.
 *    • Apply the undo operation to the heap.
 *    • Advance to prevLSN.
 *  CLRs are never undone themselves (redo-only log records).
 */

#include "recovery/log_manager.h"
#include "storage/table_heap.h"
#include <unordered_map>
#include <set>

namespace ydb {

// Summary of an active transaction as seen in the log.
struct ATTEntry {
    txn_id_t txn_id;
    lsn_t    last_lsn;   // most-recent LSN written by this txn
    bool     committed;  // true if a COMMIT record was seen
};

// Dirty-Page Table entry.
struct DPTEntry {
    page_id_t page_id;
    lsn_t     rec_lsn;  // first LSN that dirtied this page
};

class RecoveryManager {
public:
    RecoveryManager(LogManager*        log_mgr,
                    BufferPoolManager* bpm,
                    TableHeap*         heap);  // primary table (simplification)

    // Run full ARIES recovery.
    // `checkpoint_lsn` = LSN of the most-recent CHECKPOINT record (0 if none).
    void Recover(lsn_t checkpoint_lsn = 0);

private:
    LogManager*        log_;
    BufferPoolManager* bpm_;
    TableHeap*         heap_;

    std::unordered_map<txn_id_t, ATTEntry> att_;
    std::unordered_map<page_id_t, DPTEntry> dpt_;

    void Analysis(lsn_t start_lsn);
    void Redo();
    void Undo();

    // Apply a redo operation directly to the heap page.
    void RedoRecord(const LogRecord& rec);

    // Apply an undo operation and write a CLR.
    void UndoRecord(const LogRecord& rec, txn_id_t txn_id, lsn_t last_lsn);
};

} // namespace ydb
