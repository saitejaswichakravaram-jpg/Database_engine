#pragma once
/*
 * table_page.h – slotted-page layout for heap-file (table) data
 *
 * Slotted-page memory map (all within one PAGE_SIZE block)
 * ─────────────────────────────────────────────────────────
 *
 *  ┌─────────────────────────────────────────────┐  offset 0
 *  │  PageHeader  (32 bytes, from page.h)        │
 *  ├─────────────────────────────────────────────┤  offset 32
 *  │  Slot[0]  { uint16_t offset, uint16_t len } │
 *  │  Slot[1]                                    │
 *  │  Slot[2]                                    │
 *  │  …                                          │
 *  │         ← slot array grows downward →       │
 *  │                 (free space)                │
 *  │         ← tuple data grows upward  →        │
 *  │  Tuple data packed at high offsets          │
 *  └─────────────────────────────────────────────┘  offset PAGE_SIZE
 *
 * A slot has offset = INVALID_OFFSET  when the tuple was deleted.
 * Compaction (vacuum) is done by ReOrganize().
 *
 * Tuple format (simple, fixed for now):
 *   [uint32_t data_len][data_len bytes of raw data]
 *
 * This class operates directly on a Page's raw bytes; it does NOT own
 * the page – callers fetch/unpin through BufferPoolManager.
 */

#include "storage/page.h"
#include <string>
#include <cstring>

namespace ydb {

static constexpr uint16_t INVALID_OFFSET = 0;  // slot is tombstoned

struct Slot {
    uint16_t offset;  // byte offset from start of page (0 = deleted)
    uint16_t length;  // total bytes of the stored tuple record
};

class TablePage {
public:
    // Wrap an existing raw Page* (caller already fetched + pinned it).
    // Call Init() only when constructing a brand-new page.
    void Init(Page* page, page_id_t page_id);

    // Attach to an already-initialised page.
    void Attach(Page* page);

    // ── Tuple operations ───────────────────────────────────────────────────

    // Insert `data` (size bytes) and write the assigned RID into `*rid`.
    // Returns false if there is not enough free space.
    bool InsertTuple(const char* data, uint32_t size, RID* rid);

    // Copy tuple at `rid` into `dst` (caller allocates buffer of >= size).
    // Returns 0 if slot is deleted or rid is invalid.
    uint32_t GetTuple(const RID& rid, char* dst) const;

    // Mark slot as deleted (logical delete; space reclaimed by Reorganize).
    // Returns false if slot is already deleted.
    bool DeleteTuple(const RID& rid);

    // Update tuple in-place. Returns false if new size doesn't fit.
    bool UpdateTuple(const RID& rid, const char* data, uint32_t size);

    // ── Iteration ──────────────────────────────────────────────────────────
    // Fill `*rid` with the first valid (non-deleted) slot.
    // Returns false if no valid tuple exists.
    bool GetFirstTuple(RID* rid) const;

    // Advance to the next valid slot after `*rid`; modifies `*rid` in place.
    // Returns false when past the last slot.
    bool GetNextTuple(RID* rid) const;

    // ── Metadata ───────────────────────────────────────────────────────────
    uint16_t NumSlots()      const;
    uint16_t FreeSpace()     const;
    page_id_t GetPageId()    const { return page_ ? page_->GetPageId() : INVALID_PAGE_ID; }
    lsn_t     GetPageLSN()   const { return page_ ? page_->GetPageLSN() : INVALID_LSN; }
    void      SetPageLSN(lsn_t l)  { if (page_) page_->SetPageLSN(l); }

private:
    Page* page_ = nullptr;

    // Pointer arithmetic helpers.
    PageHeader* Header()       { return page_->GetHeader(); }
    const PageHeader* Header() const { return page_->GetHeader(); }

    Slot* SlotAt(uint16_t idx) {
        // Slot array starts right after PageHeader.
        return reinterpret_cast<Slot*>(page_->GetRaw() + sizeof(PageHeader)) + idx;
    }
    const Slot* SlotAt(uint16_t idx) const {
        return reinterpret_cast<const Slot*>(page_->GetRaw() + sizeof(PageHeader)) + idx;
    }

    char* TupleAt(uint16_t off) { return page_->GetRaw() + off; }
    const char* TupleAt(uint16_t off) const { return page_->GetRaw() + off; }

    // Byte offset where free space begins (just after the last slot entry).
    uint16_t SlotArrayEnd() const {
        return static_cast<uint16_t>(sizeof(PageHeader)
                                   + Header()->num_slots * sizeof(Slot));
    }
};

} // namespace ydb
