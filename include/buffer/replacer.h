#pragma once
#include <cstddef>
/*
 * replacer.h – abstract eviction policy
 *
 * The buffer pool calls Victim() when it needs a free frame.
 * Pin() marks a frame as currently in use (must not be evicted).
 * Unpin() makes a frame evictable again once all holders release it.
 */

#include "common/types.h"

namespace ydb {

class Replacer {
public:
    virtual ~Replacer() = default;

    // Select the best victim frame and write its id into `frame_id`.
    // Returns false if no frame is available for eviction.
    virtual bool Victim(frame_id_t* frame_id) = 0;

    // Mark `frame_id` as currently pinned (must not be evicted).
    virtual void Pin(frame_id_t frame_id) = 0;

    // Mark `frame_id` as evictable.
    virtual void Unpin(frame_id_t frame_id) = 0;

    // Number of frames currently eligible for eviction.
    virtual size_t Size() = 0;
};

} // namespace ydb
