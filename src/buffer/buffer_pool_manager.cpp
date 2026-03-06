#include "buffer/buffer_pool_manager.h"
#include <stdexcept>
#include <cstring>

namespace ydb {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk)
    : pool_size_(pool_size), disk_(disk),
      pages_(new Page[pool_size]),
      replacer_(pool_size)
{
    // All frames start on the free list.
    for (frame_id_t i = 0; i < static_cast<frame_id_t>(pool_size_); ++i)
        free_list_.push_back(i);
}

BufferPoolManager::~BufferPoolManager() {
    FlushAll();
    delete[] pages_;
}

// ── Internal helpers ────────────────────────────────────────────────────────

frame_id_t BufferPoolManager::FindVictimFrame() {
    // Try free list first.
    if (!free_list_.empty()) {
        frame_id_t fid = free_list_.front();
        free_list_.pop_front();
        return fid;
    }
    // Ask replacer for an LRU victim.
    frame_id_t fid = INVALID_FRAME_ID;
    if (!replacer_.Victim(&fid)) return INVALID_FRAME_ID;
    return fid;
}

void BufferPoolManager::EvictFrame(frame_id_t fid) {
    Page& pg = pages_[fid];
    if (pg.IsDirty() && pg.GetPageId() != INVALID_PAGE_ID)
        disk_->WritePage(pg.GetPageId(), pg.GetRaw());
    if (pg.GetPageId() != INVALID_PAGE_ID)
        page_table_.erase(pg.GetPageId());
    // Reset the frame.
    memset(pg.GetRaw(), 0, PAGE_SIZE);
    pg.SetDirty(false);
    pg.pin_count_ = 0;
}

// ── Public API ──────────────────────────────────────────────────────────────

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lk(latch_);

    // 1. Already in pool?
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t fid = it->second;
        pages_[fid].Pin();
        replacer_.Pin(fid);
        return &pages_[fid];
    }

    // 2. Need a victim frame.
    frame_id_t fid = FindVictimFrame();
    if (fid == INVALID_FRAME_ID) return nullptr;

    // 3. Evict whatever was there.
    EvictFrame(fid);

    // 4. Load from disk.
    disk_->ReadPage(page_id, pages_[fid].GetRaw());
    pages_[fid].SetDirty(false);
    pages_[fid].pin_count_ = 1;
    replacer_.Pin(fid);

    // 5. Update page table.
    page_table_[page_id] = fid;
    return &pages_[fid];
}

Page* BufferPoolManager::NewPage(page_id_t* page_id_out) {
    std::lock_guard<std::mutex> lk(latch_);

    frame_id_t fid = FindVictimFrame();
    if (fid == INVALID_FRAME_ID) return nullptr;

    EvictFrame(fid);

    page_id_t pid = disk_->AllocatePage();
    *page_id_out  = pid;

    // Fresh page: zeroed and marked dirty so it gets flushed on eviction.
    memset(pages_[fid].GetRaw(), 0, PAGE_SIZE);
    pages_[fid].SetPageId(pid);
    pages_[fid].SetDirty(true);
    pages_[fid].pin_count_ = 1;
    replacer_.Pin(fid);

    page_table_[pid] = fid;
    return &pages_[fid];
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lk(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;
    frame_id_t fid = it->second;
    Page& pg = pages_[fid];
    if (pg.GetPinCount() == 0) return false;
    pg.Unpin();
    if (is_dirty) pg.SetDirty(true);
    if (pg.GetPinCount() == 0)
        replacer_.Unpin(fid); // now eligible for eviction
    return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lk(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;
    frame_id_t fid = it->second;
    disk_->WritePage(page_id, pages_[fid].GetRaw());
    pages_[fid].SetDirty(false);
    return true;
}

void BufferPoolManager::FlushAll() {
    std::lock_guard<std::mutex> lk(latch_);
    for (auto& [pid, fid] : page_table_) {
        if (pages_[fid].IsDirty())
            disk_->WritePage(pid, pages_[fid].GetRaw());
        pages_[fid].SetDirty(false);
    }
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::lock_guard<std::mutex> lk(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return true; // already gone
    frame_id_t fid = it->second;
    if (pages_[fid].GetPinCount() > 0) return false; // still in use
    replacer_.Pin(fid); // remove from eviction list
    EvictFrame(fid);
    free_list_.push_back(fid);
    disk_->DeallocatePage(page_id);
    return true;
}

} // namespace ydb
