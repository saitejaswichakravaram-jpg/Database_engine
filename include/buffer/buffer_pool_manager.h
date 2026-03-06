#pragma once
/*
 * buffer_pool_manager.h – page cache sitting between upper layers and disk
 *
 * Design
 * ──────
 * Fixed pool of `pool_size` frames (Page objects) in memory.
 *
 *  page_table_  : page_id  → frame_id   (which frame holds this page?)
 *  free_list_   : frame_ids that have never been used
 *  replacer_    : LRU policy for evicting unpinned frames
 *
 * Fetch a page
 * ────────────
 *  1. If already in page_table_, pin it and return.
 *  2. Else find a victim frame (free list or LRU eviction).
 *  3. If victim frame is dirty, flush it to disk first.
 *  4. Load requested page from disk into the frame.
 *  5. Pin the frame, update page_table_, return pointer.
 *
 * Pin / Unpin
 * ───────────
 *  A caller must Unpin() every page it fetched, setting `is_dirty`
 *  to true if it modified the page.  The BPM never evicts pinned pages.
 *
 * New pages
 * ─────────
 *  NewPage() asks DiskManager for a fresh page_id, loads it into a frame,
 *  and returns the pinned Page*.
 *
 * Flush
 * ─────
 *  FlushPage()  – write one specific page to disk unconditionally.
 *  FlushAll()   – flush every dirty page.
 *
 * Thread safety
 * ─────────────
 *  latch_ protects the page table, free list, and replacer.
 */

#include "storage/page.h"
#include "storage/disk_manager.h"
#include "buffer/lru_replacer.h"
#include <memory>
#include <list>
#include <unordered_map>
#include <mutex>

namespace ydb {

class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
    ~BufferPoolManager();

    // Fetch page `page_id` into the pool (pinned, pin_count += 1).
    // Returns nullptr if no frame is available.
    Page* FetchPage(page_id_t page_id);

    // Allocate a brand-new page on disk and bring it into the pool.
    // `page_id_out` is set to the newly allocated page's id.
    // Returns nullptr if no frame is available.
    Page* NewPage(page_id_t* page_id_out);

    // Decrement pin count; if is_dirty == true, mark the frame dirty.
    // Returns false if the page is not in the pool or pin_count is already 0.
    bool UnpinPage(page_id_t page_id, bool is_dirty);

    // Write the page to disk right now (even if not dirty).
    bool FlushPage(page_id_t page_id);

    // Flush every dirty page.
    void FlushAll();

    // Delete a page from the pool and from disk (pin_count must be 0).
    bool DeletePage(page_id_t page_id);

    size_t GetPoolSize() const { return pool_size_; }

private:
    size_t              pool_size_;
    DiskManager*        disk_;
    Page*               pages_;           // array of frames
    LRUReplacer         replacer_;
    std::list<frame_id_t>                      free_list_;
    std::unordered_map<page_id_t, frame_id_t>  page_table_;
    std::mutex          latch_;

    // Internal helpers (called with latch_ held).
    frame_id_t FindVictimFrame();
    void       EvictFrame(frame_id_t fid);
};

} // namespace ydb
