#pragma once
#include <cstdint>
#include <climits>

namespace ydb {

// ─── Fundamental scalar types ──────────────────────────────────────────────
using page_id_t  = int32_t;   // on-disk page number
using frame_id_t = int32_t;   // in-memory buffer-pool frame index
using lsn_t      = int64_t;   // log sequence number (byte offset in log file)
using txn_id_t   = int64_t;   // transaction identifier
using slot_t     = uint32_t;  // slot index inside a data page

// ─── Sentinel / invalid values ─────────────────────────────────────────────
static constexpr page_id_t  INVALID_PAGE_ID  = -1;
static constexpr frame_id_t INVALID_FRAME_ID = -1;
static constexpr lsn_t      INVALID_LSN      = -1;
static constexpr txn_id_t   INVALID_TXN_ID   = -1;
static constexpr slot_t     INVALID_SLOT      = UINT32_MAX;

// ─── Page geometry ─────────────────────────────────────────────────────────
static constexpr uint32_t PAGE_SIZE        = 4096;   // 4 KiB
static constexpr uint32_t LOG_BUFFER_SIZE  = 4 * 1024 * 1024; // 4 MiB log buffer
static constexpr uint32_t DEFAULT_POOL_SZ  = 64;     // default buffer-pool frames

// ─── Record identifier ─────────────────────────────────────────────────────
// Uniquely identifies a tuple: (page it lives on, slot index within that page)
struct RID {
    page_id_t page_id  = INVALID_PAGE_ID;
    slot_t    slot_num = INVALID_SLOT;

    bool IsValid()                 const { return page_id != INVALID_PAGE_ID; }
    bool operator==(const RID& o)  const { return page_id == o.page_id && slot_num == o.slot_num; }
    bool operator!=(const RID& o)  const { return !(*this == o); }
    bool operator<(const RID& o)   const {
        if (page_id != o.page_id) return page_id < o.page_id;
        return slot_num < o.slot_num;
    }
};

} // namespace ydb
