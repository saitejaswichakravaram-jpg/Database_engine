#pragma once
/*
 * lru_replacer.h – LRU eviction policy
 *
 * Keeps an ordered list of unpinned frames.
 * Victim() returns the Least-Recently-Used unpinned frame.
 * Unpin() places a frame at the MRU (most-recent) end.
 * Pin() removes a frame from the eviction list.
 */

#include "buffer/replacer.h"
#include <list>
#include <unordered_map>
#include <mutex>

namespace ydb {

class LRUReplacer : public Replacer {
public:
    explicit LRUReplacer(size_t num_frames);
    ~LRUReplacer() override = default;

    bool   Victim(frame_id_t* frame_id) override;
    void   Pin(frame_id_t frame_id)     override;
    void   Unpin(frame_id_t frame_id)   override;
    size_t Size()                       override;

private:
    size_t capacity_;
    std::mutex latch_;

    // LRU list: front = least-recently-used (next victim),
    //           back  = most-recently-used
    std::list<frame_id_t> lru_list_;

    // Frame → iterator into lru_list_ (O(1) removal).
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> map_;
};

} // namespace ydb
