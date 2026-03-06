#pragma once
/*
 * table_heap.h – heap file: an ordered chain of TablePages
 *
 * A table's data lives in a linked list of DATA pages on disk.
 * The first page is at `first_page_id_`.
 *
 * Chaining
 * ────────
 * The last 4 bytes of each page's payload are reserved for
 * `next_page_id` (int32_t, INVALID_PAGE_ID if last page).
 *
 * Insertion
 * ─────────
 * Try to insert into the last page; if no room, append a new page.
 *
 * Iteration
 * ─────────
 * Sequential scan through all pages.
 */

#include "storage/table_page.h"
#include "buffer/buffer_pool_manager.h"
#include <string>

namespace ydb {

// Offset inside the raw page where we store the next-page pointer.
static constexpr size_t NEXT_PAGE_ID_OFFSET = PAGE_SIZE - sizeof(page_id_t);

class TableHeap {
public:
    // Create a new (empty) heap.
    TableHeap(BufferPoolManager* bpm, page_id_t* first_page_id_out);

    // Open an existing heap whose first page is `first_page_id`.
    TableHeap(BufferPoolManager* bpm, page_id_t first_page_id);

    ~TableHeap() = default;

    // ── Mutation ────────────────────────────────────────────────────────────
    bool   Insert(const char* data, uint32_t size, RID* rid);
    bool   Delete(const RID& rid);
    bool   Update(const RID& rid, const char* data, uint32_t size);

    // ── Access ──────────────────────────────────────────────────────────────
    // Read tuple at `rid` into `dst`.  Returns number of bytes copied.
    uint32_t Get(const RID& rid, char* dst) const;

    // ── Sequential scan ─────────────────────────────────────────────────────
    // Find the very first valid tuple across all pages.
    bool GetFirst(RID* rid) const;
    // Advance past `*rid` to the next valid tuple; returns false at end.
    bool GetNext(RID* rid)  const;

    page_id_t GetFirstPageId() const { return first_page_id_; }

private:
    BufferPoolManager* bpm_;
    page_id_t          first_page_id_;
    page_id_t          last_page_id_;   // cached to speed up inserts

    // Read the next-page pointer stored at the end of a raw page.
    static page_id_t GetNextPageId(const Page* page);
    static void      SetNextPageId(Page* page, page_id_t next);

    // Append a fresh page to the chain; returns its id.
    page_id_t AppendPage();
};

} // namespace ydb
