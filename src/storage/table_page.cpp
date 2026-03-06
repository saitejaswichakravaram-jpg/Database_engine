#include "storage/table_page.h"
#include <cassert>
#include <cstring>

namespace ydb {

void TablePage::Init(Page* page, page_id_t page_id) {
    page_ = page;
    page_->SetPageId(page_id);
    page_->SetPageType(PageType::DATA);
    Header()->num_slots      = 0;
    // Reserve the last sizeof(page_id_t) bytes for the TableHeap next-page pointer.
    // Tuple data grows downward from just above those reserved bytes.
    Header()->free_space_ptr = static_cast<uint16_t>(PAGE_SIZE - sizeof(page_id_t));
    page_->SetDirty(true);
}

void TablePage::Attach(Page* page) {
    page_ = page;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

uint16_t TablePage::NumSlots() const {
    return Header()->num_slots;
}

uint16_t TablePage::FreeSpace() const {
    // free area = [SlotArrayEnd .. free_space_ptr)
    uint16_t top = SlotArrayEnd();
    uint16_t bot = Header()->free_space_ptr;
    return (bot > top) ? (bot - top) : 0;
}

// ── Tuple operations ────────────────────────────────────────────────────────

bool TablePage::InsertTuple(const char* data, uint32_t size, RID* rid) {
    // Total bytes needed: 4-byte length prefix + data
    uint32_t total = sizeof(uint32_t) + size;

    // Also need room for a new Slot entry.
    uint16_t needed = static_cast<uint16_t>(total + sizeof(Slot));
    if (FreeSpace() < needed) return false;

    // Place tuple at high end, growing downward.
    uint16_t tuple_offset = static_cast<uint16_t>(Header()->free_space_ptr - total);
    Header()->free_space_ptr = tuple_offset;

    // Write [length][data].
    char* dest = TupleAt(tuple_offset);
    memcpy(dest, &size, sizeof(uint32_t));
    memcpy(dest + sizeof(uint32_t), data, size);

    // Add slot entry.
    uint16_t slot_idx = Header()->num_slots++;
    SlotAt(slot_idx)->offset = tuple_offset;
    SlotAt(slot_idx)->length = static_cast<uint16_t>(total);

    rid->page_id  = GetPageId();
    rid->slot_num = slot_idx;

    page_->SetDirty(true);
    return true;
}

uint32_t TablePage::GetTuple(const RID& rid, char* dst) const {
    if (rid.slot_num >= Header()->num_slots) return 0;
    const Slot* s = SlotAt(rid.slot_num);
    if (s->offset == INVALID_OFFSET) return 0; // deleted
    const char* src = TupleAt(s->offset);
    uint32_t len = 0;
    memcpy(&len, src, sizeof(uint32_t));
    memcpy(dst, src + sizeof(uint32_t), len);
    return len;
}

bool TablePage::DeleteTuple(const RID& rid) {
    if (rid.slot_num >= Header()->num_slots) return false;
    Slot* s = SlotAt(rid.slot_num);
    if (s->offset == INVALID_OFFSET) return false;
    s->offset = INVALID_OFFSET; // logical tombstone
    page_->SetDirty(true);
    return true;
}

bool TablePage::UpdateTuple(const RID& rid, const char* data, uint32_t size) {
    if (rid.slot_num >= Header()->num_slots) return false;
    Slot* s = SlotAt(rid.slot_num);
    if (s->offset == INVALID_OFFSET) return false;
    uint32_t old_payload = 0;
    memcpy(&old_payload, TupleAt(s->offset), sizeof(uint32_t));
    if (size > old_payload) return false; // no room for in-place update
    // Write new data in-place (may leave a small gap, handled by vacuum).
    memcpy(TupleAt(s->offset), &size, sizeof(uint32_t));
    memcpy(TupleAt(s->offset) + sizeof(uint32_t), data, size);
    page_->SetDirty(true);
    return true;
}

// ── Iteration ───────────────────────────────────────────────────────────────

bool TablePage::GetFirstTuple(RID* rid) const {
    rid->page_id  = GetPageId();
    rid->slot_num = 0;
    return GetNextTuple(rid); // reuse: finds first non-deleted starting at 0
}

bool TablePage::GetNextTuple(RID* rid) const {
    // Start from rid->slot_num; find first non-deleted.
    for (uint16_t i = rid->slot_num; i < Header()->num_slots; ++i) {
        if (SlotAt(i)->offset != INVALID_OFFSET) {
            rid->slot_num = i;
            return true;
        }
    }
    return false;
}

} // namespace ydb
