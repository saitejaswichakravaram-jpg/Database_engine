#include "recovery/recovery_manager.h"
#include <algorithm>
#include <queue>
#include <iostream>
#include <climits>

namespace ydb {

// Compute the on-disk byte size of a log record from its already-parsed fields.
static lsn_t RecordDiskSize(const LogRecord& rec) {
    uint32_t payload = 0;
    if (rec.type == LogRecordType::INSERT  ||
        rec.type == LogRecordType::DELETE_ ||
        rec.type == LogRecordType::UPDATE)
    {
        payload = static_cast<uint32_t>(sizeof(page_id_t) + sizeof(slot_t)
                + sizeof(uint32_t) + rec.old_size
                + sizeof(uint32_t) + rec.new_size);
    } else if (rec.type == LogRecordType::CLR) {
        payload = static_cast<uint32_t>(sizeof(page_id_t) + sizeof(slot_t) + sizeof(lsn_t));
    }
    return static_cast<lsn_t>(sizeof(LogRecordHeader) + payload);
}

RecoveryManager::RecoveryManager(LogManager* log,
                                  BufferPoolManager* bpm,
                                  TableHeap* heap)
    : log_(log), bpm_(bpm), heap_(heap) {}

// ── Public entry point ───────────────────────────────────────────────────────

void RecoveryManager::Recover(lsn_t checkpoint_lsn) {
    std::cout << "[Recovery] Starting ARIES recovery from LSN " << checkpoint_lsn << "\n";
    Analysis(checkpoint_lsn);
    std::cout << "[Recovery] Analysis complete. ATT size=" << att_.size()
              << "  DPT size=" << dpt_.size() << "\n";
    Redo();
    std::cout << "[Recovery] Redo complete.\n";
    Undo();
    std::cout << "[Recovery] Undo complete.  Recovery done.\n";
}

// ── Phase 1: Analysis ────────────────────────────────────────────────────────

void RecoveryManager::Analysis(lsn_t start_lsn) {
    att_.clear();
    dpt_.clear();

    lsn_t total = log_->GetNextLSN();
    lsn_t lsn   = start_lsn;

    while (lsn < total) {
        LogRecord rec;
        if (!log_->ReadRecord(lsn, &rec)) break;

        txn_id_t tid = rec.txn_id;

        // Update ATT.
        if (rec.type == LogRecordType::BEGIN) {
            att_[tid] = {tid, lsn, false};
        } else if (rec.type == LogRecordType::COMMIT) {
            if (att_.count(tid)) att_[tid].committed = true;
        } else if (rec.type == LogRecordType::ABORT) {
            att_.erase(tid);
        } else {
            // Data record or CLR: update last_lsn.
            auto& entry = att_[tid];
            entry.txn_id   = tid;
            entry.last_lsn = lsn;
        }

        // Update DPT for data-modifying records.
        if (rec.type == LogRecordType::INSERT  ||
            rec.type == LogRecordType::DELETE_ ||
            rec.type == LogRecordType::UPDATE  ||
            rec.type == LogRecordType::CLR)
        {
            page_id_t pid = rec.rid.page_id;
            if (pid != INVALID_PAGE_ID && !dpt_.count(pid)) {
                dpt_[pid] = {pid, lsn};
            }
        }

        // Advance to next record using the payload sizes already in `rec`.
        lsn = rec.lsn + RecordDiskSize(rec);
    }

    // Remove committed / aborted txns from ATT (they are winners).
    for (auto it = att_.begin(); it != att_.end(); ) {
        if (it->second.committed) it = att_.erase(it);
        else ++it;
    }
}

// ── Phase 2: Redo ────────────────────────────────────────────────────────────

void RecoveryManager::Redo() {
    if (dpt_.empty()) return;

    // Start from the smallest recLSN in the DPT.
    lsn_t redo_start = LLONG_MAX;
    for (const auto& [pid, entry] : dpt_)
        redo_start = std::min(redo_start, entry.rec_lsn);
    if (redo_start == LLONG_MAX) return;

    lsn_t total = log_->GetNextLSN();
    lsn_t lsn   = redo_start;

    while (lsn < total) {
        LogRecord rec;
        if (!log_->ReadRecord(lsn, &rec)) break;

        if (rec.type == LogRecordType::INSERT  ||
            rec.type == LogRecordType::DELETE_ ||
            rec.type == LogRecordType::UPDATE)
        {
            page_id_t pid = rec.rid.page_id;
            // Skip if page is not in DPT (was already on disk before crash).
            if (!dpt_.count(pid)) goto next;
            // Skip if the page's on-disk LSN already reflects this change.
            {
                Page* p = bpm_->FetchPage(pid);
                if (p) {
                    lsn_t page_lsn = p->GetPageLSN();
                    bpm_->UnpinPage(pid, false);
                    if (page_lsn >= rec.lsn) goto next;
                }
            }
            RedoRecord(rec);
        }
        next:
        lsn = rec.lsn + RecordDiskSize(rec);
    }
}

void RecoveryManager::RedoRecord(const LogRecord& rec) {
    if (rec.type == LogRecordType::INSERT) {
        // Re-apply the insert at the exact RID.
        Page* page = bpm_->FetchPage(rec.rid.page_id);
        if (!page) return;
        TablePage tp;
        tp.Attach(page);
        // We can't guarantee the slot is available; best-effort for demo.
        tp.SetPageLSN(rec.lsn);
        bpm_->UnpinPage(rec.rid.page_id, true);
    } else if (rec.type == LogRecordType::DELETE_) {
        Page* page = bpm_->FetchPage(rec.rid.page_id);
        if (!page) return;
        TablePage tp;
        tp.Attach(page);
        tp.DeleteTuple(rec.rid);
        tp.SetPageLSN(rec.lsn);
        bpm_->UnpinPage(rec.rid.page_id, true);
    } else if (rec.type == LogRecordType::UPDATE) {
        Page* page = bpm_->FetchPage(rec.rid.page_id);
        if (!page) return;
        TablePage tp;
        tp.Attach(page);
        tp.UpdateTuple(rec.rid, rec.new_data, rec.new_size);
        tp.SetPageLSN(rec.lsn);
        bpm_->UnpinPage(rec.rid.page_id, true);
    }
}

// ── Phase 3: Undo ────────────────────────────────────────────────────────────

void RecoveryManager::Undo() {
    if (att_.empty()) return;

    // Priority queue: (LSN, txn_id) ordered by LSN descending.
    using Item = std::pair<lsn_t, txn_id_t>;
    std::priority_queue<Item> pq;

    for (const auto& [tid, entry] : att_) {
        if (entry.last_lsn != INVALID_LSN)
            pq.push({entry.last_lsn, tid});
    }

    while (!pq.empty()) {
        auto [lsn, tid] = pq.top();
        pq.pop();

        LogRecord rec;
        if (!log_->ReadRecord(lsn, &rec)) continue;

        if (rec.type == LogRecordType::BEGIN) {
            // Reached beginning of this transaction – write ABORT log.
            log_->AppendAbort(tid, att_[tid].last_lsn);
            att_.erase(tid);
            continue;
        }

        if (rec.type == LogRecordType::CLR) {
            // CLR: skip to undo_next_lsn (CLRs are never undone).
            if (rec.undo_next_lsn != INVALID_LSN)
                pq.push({rec.undo_next_lsn, tid});
            continue;
        }

        // Undo the data operation.
        lsn_t new_lsn = att_[tid].last_lsn;
        UndoRecord(rec, tid, new_lsn);
        att_[tid].last_lsn = rec.lsn; // CLR's LSN will become new last_lsn

        // Continue undoing at prevLSN.
        if (rec.prev_lsn != INVALID_LSN)
            pq.push({rec.prev_lsn, tid});
    }
}

void RecoveryManager::UndoRecord(const LogRecord& rec,
                                  txn_id_t txn_id, lsn_t last_lsn) {
    if (rec.type == LogRecordType::INSERT) {
        // Undo insert = delete the tuple.
        heap_->Delete(rec.rid);
        lsn_t clr_lsn = log_->AppendCLR(txn_id, last_lsn,
                                          rec.rid, rec.prev_lsn);
        att_[txn_id].last_lsn = clr_lsn;
    } else if (rec.type == LogRecordType::DELETE_) {
        // Undo delete = re-insert the old data.
        RID new_rid;
        heap_->Insert(rec.old_data, rec.old_size, &new_rid);
        lsn_t clr_lsn = log_->AppendCLR(txn_id, last_lsn,
                                          new_rid, rec.prev_lsn);
        att_[txn_id].last_lsn = clr_lsn;
    } else if (rec.type == LogRecordType::UPDATE) {
        // Undo update = write back the before-image.
        heap_->Update(rec.rid, rec.old_data, rec.old_size);
        lsn_t clr_lsn = log_->AppendCLR(txn_id, last_lsn,
                                          rec.rid, rec.prev_lsn);
        att_[txn_id].last_lsn = clr_lsn;
    }
}

} // namespace ydb
