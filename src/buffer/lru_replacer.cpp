#include "buffer/lru_replacer.h"

namespace ydb {

LRUReplacer::LRUReplacer(size_t num_frames) : capacity_(num_frames) {}

bool LRUReplacer::Victim(frame_id_t* frame_id) {
    std::lock_guard<std::mutex> lk(latch_);
    if (lru_list_.empty()) return false;
    // Evict from the LRU (front) end.
    *frame_id = lru_list_.front();
    map_.erase(*frame_id);
    lru_list_.pop_front();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lk(latch_);
    auto it = map_.find(frame_id);
    if (it == map_.end()) return; // already pinned or unknown
    lru_list_.erase(it->second);
    map_.erase(it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lk(latch_);
    if (map_.count(frame_id)) return; // already in list
    if (lru_list_.size() >= capacity_) return; // shouldn't happen
    lru_list_.push_back(frame_id);
    map_[frame_id] = std::prev(lru_list_.end());
}

size_t LRUReplacer::Size() {
    std::lock_guard<std::mutex> lk(latch_);
    return lru_list_.size();
}

} // namespace ydb
