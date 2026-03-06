#pragma once
/*
 * page.h – raw page abstraction
 *
 * Every page on disk / in the buffer pool is PAGE_SIZE (4096) bytes.
 *
 * On-disk layout
 * ───────────────
 *  Byte 0 … 31  : PageHeader  (32 bytes, fixed for every page type)
 *  Byte 32 …    : type-specific payload (interpreted by upper layers)
 *
 * PageHeader fields
 * ─────────────────
 *  page_id        – assigned by DiskManager at allocation time
 *  page_type      – DATA | BPLUS_INTERNAL | BPLUS_LEAF | META | FREE
 *  flags          – bit-field: currently unused (reserved for hints)
 *  num_slots      – number of slot-directory entries (data pages only)
 *  free_space_ptr – byte offset where free space starts (data pages)
 *  page_lsn       – LSN of the most-recent WAL record that modified this page
 *  checksum       – simple XOR checksum over the payload bytes
 *  reserved       – padding to keep the header exactly 32 bytes
 *
 * Buffer-pool metadata (NOT written to disk)
 * ──────────────────────────────────────────
 *  pin_count  – how many threads currently hold this page
 *  is_dirty   – true if the in-memory copy differs from disk
 */

#include "common/types.h"
#include <cstring>

namespace ydb {

// ─── Page type tag ──────────────────────────────────────────────────────────
enum class PageType : uint8_t {
    INVALID        = 0,
    META           = 1,  // first page: database-level metadata
    DATA           = 2,  // heap/table data (slotted-page layout)
    BPLUS_INTERNAL = 3,  // B+ tree internal node
    BPLUS_LEAF     = 4,  // B+ tree leaf node
    FREE           = 5,  // on free-list, awaiting reuse
};

// ─── PageHeader ─────────────────────────────────────────────────────────────
// Exactly 32 bytes.  Fields ordered largest-to-smallest to avoid implicit
// compiler padding.  Verified by static_assert below.
struct PageHeader {
    lsn_t      page_lsn;        //  8 B – WAL LSN of last update   (offset 0)
    page_id_t  page_id;         //  4 B – unique page identifier    (offset 8)
    uint32_t   checksum;        //  4 B – payload checksum          (offset 12)
    uint16_t   num_slots;       //  2 B – slot count (data pages)   (offset 16)
    uint16_t   free_space_ptr;  //  2 B – start of free area        (offset 18)
    PageType   page_type;       //  1 B – see enum above            (offset 20)
    uint8_t    flags;           //  1 B – reserved bit-field        (offset 21)
    uint8_t    reserved[10];    // 10 B – future use                (offset 22)
};
static_assert(sizeof(PageHeader) == 32, "PageHeader must be exactly 32 bytes");

// ─── Page ───────────────────────────────────────────────────────────────────
class Page {
public:
    Page() { memset(data_, 0, PAGE_SIZE); }

    // ── Header accessors ────────────────────────────────────────────────────
    PageHeader*       GetHeader()       { return reinterpret_cast<PageHeader*>(data_); }
    const PageHeader* GetHeader() const { return reinterpret_cast<const PageHeader*>(data_); }

    page_id_t  GetPageId()   const { return GetHeader()->page_id; }
    void       SetPageId(page_id_t id) { GetHeader()->page_id = id; }

    PageType   GetPageType() const { return GetHeader()->page_type; }
    void       SetPageType(PageType t) { GetHeader()->page_type = t; }

    lsn_t      GetPageLSN()  const { return GetHeader()->page_lsn; }
    void       SetPageLSN(lsn_t l) { GetHeader()->page_lsn = l; }

    // ── Buffer-pool metadata (NOT persisted to disk) ─────────────────────
    bool IsDirty()    const { return is_dirty_; }
    void SetDirty(bool d)   { is_dirty_ = d; }

    int  GetPinCount() const { return pin_count_; }
    void Pin()               { ++pin_count_; }
    void Unpin()             { if (pin_count_ > 0) --pin_count_; }

    // ── Raw bytes ────────────────────────────────────────────────────────
    char*       GetRaw()       { return data_; }          // full 4096 bytes
    const char* GetRaw() const { return data_; }

    // Payload area (after the fixed header)
    char*       GetPayload()       { return data_ + sizeof(PageHeader); }
    const char* GetPayload() const { return data_ + sizeof(PageHeader); }

    static constexpr uint32_t PAYLOAD_SIZE = PAGE_SIZE - sizeof(PageHeader);

private:
    char data_[PAGE_SIZE];

    // Buffer-pool bookkeeping (lives only in RAM)
    bool is_dirty_  = false;
    int  pin_count_ = 0;

    friend class BufferPoolManager;
};

} // namespace ydb
