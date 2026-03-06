#pragma once
/*
 * log_record.h – binary WAL record format
 *
 * Every log record starts with a fixed-size LogRecordHeader.
 * Variable-length payload follows immediately for UPDATE records.
 *
 * On-disk binary layout per record
 * ──────────────────────────────────
 *  [LogRecordHeader]
 *  for BEGIN / COMMIT / ABORT:  header only (size = sizeof(LogRecordHeader))
 *  for INSERT / DELETE / UPDATE:
 *     [header]
 *     [rid: page_id_t (4) + slot_t (4)]
 *     [old_size: uint32_t]  [old_data: old_size bytes]   ← before-image
 *     [new_size: uint32_t]  [new_data: new_size bytes]   ← after-image
 *  for CLR (Compensation Log Record during undo):
 *     [header]
 *     [rid: 8 bytes]
 *     [undo_next_lsn: lsn_t]   ← LSN of the next record to undo for this txn
 *  for CHECKPOINT:
 *     header only (checkpoints written before flush)
 *
 * LSN = byte offset in the log file at which this record begins.
 *       The LogManager fills this in when appending.
 */

#include "common/types.h"
#include <cstdint>

namespace ydb {

enum class LogRecordType : uint8_t {
    INVALID    = 0,
    BEGIN      = 1,
    COMMIT     = 2,
    ABORT      = 3,
    INSERT     = 4,
    DELETE_    = 5,   // underscore to avoid keyword clash
    UPDATE     = 6,
    CLR        = 7,   // Compensation Log Record (undo-of-undo)
    CHECKPOINT = 8,
};

// Fixed-size header present at the start of EVERY log record.
struct LogRecordHeader {
    lsn_t           lsn;         //  8 B – assigned by LogManager
    lsn_t           prev_lsn;    //  8 B – previous LSN for same txn (undo chain)
    txn_id_t        txn_id;      //  8 B
    LogRecordType   type;        //  1 B
    uint8_t         _pad[7];     //  7 B – alignment
    uint32_t        payload_len; //  4 B – bytes after this header
    uint8_t         _pad2[4];    //  4 B
};
// Total: 40 bytes; keep aligned on 8.
static_assert(sizeof(LogRecordHeader) == 40);

// In-memory representation (unpacked for easy manipulation).
struct LogRecord {
    // Header fields.
    lsn_t           lsn       = INVALID_LSN;
    lsn_t           prev_lsn  = INVALID_LSN;
    txn_id_t        txn_id    = INVALID_TXN_ID;
    LogRecordType   type      = LogRecordType::INVALID;

    // For INSERT / DELETE / UPDATE / CLR.
    RID     rid;
    // Before / after images (UPDATE; DELETE uses old_data; INSERT uses new_data).
    uint32_t old_size = 0;
    char     old_data[512] {};  // simplification: max tuple 512 B
    uint32_t new_size = 0;
    char     new_data[512] {};

    // For CLR only.
    lsn_t undo_next_lsn = INVALID_LSN;
};

} // namespace ydb
