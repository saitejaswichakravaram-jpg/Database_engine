#include "recovery/log_manager.h"
#include <cstring>
#include <stdexcept>

namespace ydb {

LogManager::LogManager(DiskManager* disk) : disk_(disk) {
    log_buf_.resize(BUF_SIZE);
    // next_lsn starts where the existing log file ends.
    next_lsn_ = static_cast<lsn_t>(disk_->GetLogSize());
    persistent_lsn_ = next_lsn_ - 1;
}

LogManager::~LogManager() {
    Flush();
}

// ── Low-level serialisation ──────────────────────────────────────────────────
//
// Binary layout written for each record:
//   [LogRecordHeader (40 bytes)]
//   [payload bytes]
//
// Payload for data records:
//   [rid.page_id (4)] [rid.slot_num (4)]
//   [old_size (4)] [old_data (old_size)]
//   [new_size (4)] [new_data (new_size)]
//
// Payload for CLR:
//   [rid.page_id (4)] [rid.slot_num (4)] [undo_next_lsn (8)]
//
// BEGIN / COMMIT / ABORT / CHECKPOINT: no payload.

lsn_t LogManager::Append(LogRecord& rec) {
    std::lock_guard<std::mutex> lk(append_latch_);

    // Calculate payload size.
    uint32_t payload = 0;
    bool has_data = (rec.type == LogRecordType::INSERT  ||
                     rec.type == LogRecordType::DELETE_ ||
                     rec.type == LogRecordType::UPDATE);
    bool is_clr  = (rec.type == LogRecordType::CLR);

    if (has_data) {
        payload = sizeof(page_id_t) + sizeof(slot_t)
                + sizeof(uint32_t) + rec.old_size
                + sizeof(uint32_t) + rec.new_size;
    } else if (is_clr) {
        payload = sizeof(page_id_t) + sizeof(slot_t) + sizeof(lsn_t);
    }

    size_t total = sizeof(LogRecordHeader) + payload;
    if (buf_used_ + total > BUF_SIZE) {
        // Flush buffer to make room.
        disk_->WriteLog(log_buf_.data(), buf_used_);
        persistent_lsn_ = next_lsn_ - 1;
        buf_used_ = 0;
    }

    // Assign LSN.
    lsn_t assigned_lsn = next_lsn_;
    rec.lsn = assigned_lsn;
    next_lsn_ += static_cast<lsn_t>(total);

    // Build header.
    LogRecordHeader hdr{};
    hdr.lsn         = assigned_lsn;
    hdr.prev_lsn    = rec.prev_lsn;
    hdr.txn_id      = rec.txn_id;
    hdr.type        = rec.type;
    hdr.payload_len = payload;

    char* dst = log_buf_.data() + buf_used_;
    memcpy(dst, &hdr, sizeof(LogRecordHeader));
    dst += sizeof(LogRecordHeader);

    if (has_data) {
        memcpy(dst, &rec.rid.page_id,  sizeof(page_id_t)); dst += sizeof(page_id_t);
        memcpy(dst, &rec.rid.slot_num, sizeof(slot_t));    dst += sizeof(slot_t);
        memcpy(dst, &rec.old_size,     sizeof(uint32_t));  dst += sizeof(uint32_t);
        memcpy(dst, rec.old_data,      rec.old_size);      dst += rec.old_size;
        memcpy(dst, &rec.new_size,     sizeof(uint32_t));  dst += sizeof(uint32_t);
        memcpy(dst, rec.new_data,      rec.new_size);      dst += rec.new_size;
    } else if (is_clr) {
        memcpy(dst, &rec.rid.page_id,     sizeof(page_id_t)); dst += sizeof(page_id_t);
        memcpy(dst, &rec.rid.slot_num,    sizeof(slot_t));    dst += sizeof(slot_t);
        memcpy(dst, &rec.undo_next_lsn,   sizeof(lsn_t));
    }

    buf_used_ += total;
    return assigned_lsn;
}

void LogManager::Flush() {
    std::lock_guard<std::mutex> lk(append_latch_);
    if (buf_used_ == 0) return;
    disk_->WriteLog(log_buf_.data(), buf_used_);
    persistent_lsn_ = next_lsn_ - 1;
    buf_used_ = 0;
}

void LogManager::MaybeFlush() {
    if (buf_used_ >= BUF_SIZE * 3 / 4) Flush();
}

// ── Public append methods ────────────────────────────────────────────────────

lsn_t LogManager::AppendBegin(txn_id_t txn_id, lsn_t prev_lsn) {
    LogRecord r;
    r.txn_id   = txn_id;
    r.prev_lsn = prev_lsn;
    r.type     = LogRecordType::BEGIN;
    return Append(r);
}

lsn_t LogManager::AppendCommit(txn_id_t txn_id, lsn_t prev_lsn) {
    LogRecord r;
    r.txn_id   = txn_id;
    r.prev_lsn = prev_lsn;
    r.type     = LogRecordType::COMMIT;
    lsn_t l = Append(r);
    Flush(); // commit rule: must be on stable storage before returning
    return l;
}

lsn_t LogManager::AppendAbort(txn_id_t txn_id, lsn_t prev_lsn) {
    LogRecord r;
    r.txn_id   = txn_id;
    r.prev_lsn = prev_lsn;
    r.type     = LogRecordType::ABORT;
    lsn_t l = Append(r);
    Flush();
    return l;
}

lsn_t LogManager::AppendInsert(txn_id_t txn_id, lsn_t prev_lsn,
                                 const RID& rid,
                                 const char* data, uint32_t size) {
    LogRecord r;
    r.txn_id   = txn_id;
    r.prev_lsn = prev_lsn;
    r.type     = LogRecordType::INSERT;
    r.rid      = rid;
    r.old_size = 0;
    r.new_size = size;
    memcpy(r.new_data, data, size);
    return Append(r);
}

lsn_t LogManager::AppendDelete(txn_id_t txn_id, lsn_t prev_lsn,
                                 const RID& rid,
                                 const char* old_data, uint32_t old_size) {
    LogRecord r;
    r.txn_id   = txn_id;
    r.prev_lsn = prev_lsn;
    r.type     = LogRecordType::DELETE_;
    r.rid      = rid;
    r.old_size = old_size;
    r.new_size = 0;
    memcpy(r.old_data, old_data, old_size);
    return Append(r);
}

lsn_t LogManager::AppendUpdate(txn_id_t txn_id, lsn_t prev_lsn,
                                 const RID& rid,
                                 const char* old_data, uint32_t old_size,
                                 const char* new_data, uint32_t new_size) {
    LogRecord r;
    r.txn_id   = txn_id;
    r.prev_lsn = prev_lsn;
    r.type     = LogRecordType::UPDATE;
    r.rid      = rid;
    r.old_size = old_size;
    r.new_size = new_size;
    memcpy(r.old_data, old_data, old_size);
    memcpy(r.new_data, new_data, new_size);
    return Append(r);
}

lsn_t LogManager::AppendCLR(txn_id_t txn_id, lsn_t prev_lsn,
                              const RID& rid, lsn_t undo_next_lsn) {
    LogRecord r;
    r.txn_id        = txn_id;
    r.prev_lsn      = prev_lsn;
    r.type          = LogRecordType::CLR;
    r.rid           = rid;
    r.undo_next_lsn = undo_next_lsn;
    return Append(r);
}

lsn_t LogManager::AppendCheckpoint(txn_id_t txn_id) {
    LogRecord r;
    r.txn_id   = txn_id;
    r.prev_lsn = INVALID_LSN;
    r.type     = LogRecordType::CHECKPOINT;
    lsn_t l    = Append(r);
    Flush();
    return l;
}

// ── Read interface ───────────────────────────────────────────────────────────

bool LogManager::ReadRecord(lsn_t lsn, LogRecord* out) {
    if (lsn < 0) return false;

    // First flush buffer so disk is up to date.
    {
        std::lock_guard<std::mutex> lk(append_latch_);
        if (buf_used_ > 0) {
            disk_->WriteLog(log_buf_.data(), buf_used_);
            persistent_lsn_ = next_lsn_ - 1;
            buf_used_ = 0;
        }
    }

    LogRecordHeader hdr{};
    if (!disk_->ReadLog(reinterpret_cast<char*>(&hdr),
                        sizeof(hdr), static_cast<size_t>(lsn)))
        return false;

    out->lsn      = hdr.lsn;
    out->prev_lsn = hdr.prev_lsn;
    out->txn_id   = hdr.txn_id;
    out->type     = hdr.type;

    if (hdr.payload_len == 0) return true;

    size_t off = static_cast<size_t>(lsn) + sizeof(LogRecordHeader);
    char   payload[2048]{};
    if (!disk_->ReadLog(payload, hdr.payload_len, off)) return false;

    const char* p = payload;
    if (out->type == LogRecordType::CLR) {
        memcpy(&out->rid.page_id,    p, sizeof(page_id_t)); p += sizeof(page_id_t);
        memcpy(&out->rid.slot_num,   p, sizeof(slot_t));    p += sizeof(slot_t);
        memcpy(&out->undo_next_lsn,  p, sizeof(lsn_t));
    } else {
        memcpy(&out->rid.page_id,  p, sizeof(page_id_t)); p += sizeof(page_id_t);
        memcpy(&out->rid.slot_num, p, sizeof(slot_t));    p += sizeof(slot_t);
        memcpy(&out->old_size,     p, sizeof(uint32_t));  p += sizeof(uint32_t);
        memcpy(out->old_data,      p, out->old_size);     p += out->old_size;
        memcpy(&out->new_size,     p, sizeof(uint32_t));  p += sizeof(uint32_t);
        memcpy(out->new_data,      p, out->new_size);
    }
    return true;
}

} // namespace ydb
