#pragma once
/*
 * bplus_tree.h – B+ tree index backed by the buffer pool
 *
 * Properties
 * ──────────
 *  • Every tree node == one PAGE_SIZE page in the buffer pool.
 *  • Internal nodes hold keys + child page_ids.
 *  • Leaf nodes hold key-value pairs and are linked via next_leaf_page_id.
 *  • Keys are int32_t; values are RID (page_id + slot_num).
 *  • Order is determined at runtime from PAGE_SIZE / sizeof(entry).
 *
 * Page payload layout (after the 32-byte PageHeader)
 * ───────────────────────────────────────────────────
 *
 *  BPlusNodeHeader (12 bytes)
 *  ┌──────────────┬──────────────┬────────────────────────┐
 *  │  is_leaf (1) │ num_keys (4) │  max_keys (4) + pad(3) │
 *  └──────────────┴──────────────┴────────────────────────┘
 *
 *  Internal node after the header:
 *    children[0], keys[0], children[1], keys[1], ... children[n]
 *    (num_keys keys, num_keys+1 children)
 *
 *  Leaf node after the header:
 *    next_leaf_page_id (4 bytes)
 *    keys[0], values[0], keys[1], values[1], ...
 *
 * All values are stored sorted by key.
 */

#include "storage/page.h"
#include "buffer/buffer_pool_manager.h"
#include <optional>
#include <vector>
#include <functional>

namespace ydb {

// ── Key / Value types used by this tree ─────────────────────────────────────
using BKey   = int32_t;
using BValue = RID;

// ── Node header ─────────────────────────────────────────────────────────────
struct BPlusNodeHeader {
    uint8_t  is_leaf;    // 1 = leaf, 0 = internal
    uint8_t  _pad[3];
    int32_t  num_keys;
    int32_t  max_keys;   // max keys this page can hold
};
static_assert(sizeof(BPlusNodeHeader) == 12);

// Helper: max keys an INTERNAL page can hold
//   payload = PAYLOAD_SIZE - sizeof(BPlusNodeHeader)
//   layout:  (max_keys+1)*sizeof(page_id_t) + max_keys*sizeof(BKey)
//   solve:   max_keys = (payload - sizeof(page_id_t))
//                     / (sizeof(page_id_t) + sizeof(BKey))
inline constexpr int InternalMaxKeys() {
    return static_cast<int>(
        (Page::PAYLOAD_SIZE - sizeof(BPlusNodeHeader) - sizeof(page_id_t))
        / (sizeof(page_id_t) + sizeof(BKey)));
}

// Max entries a LEAF page can hold
//   payload after header and next_page pointer:
//   (PAYLOAD_SIZE - sizeof(BPlusNodeHeader) - sizeof(page_id_t))
//   / (sizeof(BKey) + sizeof(BValue))
inline constexpr int LeafMaxKeys() {
    return static_cast<int>(
        (Page::PAYLOAD_SIZE - sizeof(BPlusNodeHeader) - sizeof(page_id_t))
        / (sizeof(BKey) + sizeof(BValue)));
}

// ── BPlusTree ────────────────────────────────────────────────────────────────
class BPlusTree {
public:
    // Create a new (empty) B+ tree.
    BPlusTree(BufferPoolManager* bpm, page_id_t* root_page_id_out);

    // Open an existing tree whose root is at `root_page_id`.
    BPlusTree(BufferPoolManager* bpm, page_id_t root_page_id);

    // ── Operations ─────────────────────────────────────────────────────────
    bool Insert(BKey key, const BValue& value);
    bool Delete(BKey key);
    std::optional<BValue> Search(BKey key) const;

    // Range scan: call `cb` for every (key, value) in [low, high] order.
    void RangeScan(BKey low, BKey high,
                   const std::function<void(BKey, BValue)>& cb) const;

    page_id_t GetRootPageId() const { return root_page_id_; }

private:
    BufferPoolManager* bpm_;
    page_id_t          root_page_id_;

    // ── Internal helpers ───────────────────────────────────────────────────

    // Read the node header without pin-leaking.
    bool IsLeaf(page_id_t pid) const;

    // -- Accessors into raw page bytes --
    BPlusNodeHeader* NodeHeader(Page* p) const {
        return reinterpret_cast<BPlusNodeHeader*>(p->GetPayload());
    }
    const BPlusNodeHeader* NodeHeader(const Page* p) const {
        return reinterpret_cast<const BPlusNodeHeader*>(p->GetPayload());
    }

    // Internal node: children array starts after header.
    page_id_t* InternalChildren(Page* p) const {
        return reinterpret_cast<page_id_t*>(p->GetPayload() + sizeof(BPlusNodeHeader));
    }
    const page_id_t* InternalChildren(const Page* p) const {
        return reinterpret_cast<const page_id_t*>(p->GetPayload() + sizeof(BPlusNodeHeader));
    }
    // Internal node: keys array starts after children array.
    BKey* InternalKeys(Page* p) const {
        int max_keys = NodeHeader(p)->max_keys;
        return reinterpret_cast<BKey*>(InternalChildren(p) + max_keys + 1);
    }
    const BKey* InternalKeys(const Page* p) const {
        int max_keys = NodeHeader(p)->max_keys;
        return reinterpret_cast<const BKey*>(InternalChildren(p) + max_keys + 1);
    }

    // Leaf node: next_leaf_page_id starts after header.
    page_id_t* LeafNextPtr(Page* p) const {
        return reinterpret_cast<page_id_t*>(p->GetPayload() + sizeof(BPlusNodeHeader));
    }
    // Leaf node: key-value pairs after next pointer.
    BKey* LeafKeys(Page* p) const {
        return reinterpret_cast<BKey*>(LeafNextPtr(p) + 1);
    }
    const BKey* LeafKeys(const Page* p) const {
        return reinterpret_cast<const BKey*>(
            p->GetPayload() + sizeof(BPlusNodeHeader) + sizeof(page_id_t));
    }
    BValue* LeafValues(Page* p) const {
        return reinterpret_cast<BValue*>(LeafKeys(p) + NodeHeader(p)->max_keys);
    }
    const BValue* LeafValues(const Page* p) const {
        const BKey* keys = LeafKeys(p);
        int max_keys = NodeHeader(p)->max_keys;
        return reinterpret_cast<const BValue*>(keys + max_keys);
    }

    // -- Node creation --
    Page* NewInternalPage(page_id_t* pid);
    Page* NewLeafPage(page_id_t* pid);

    // -- Search helpers --
    page_id_t FindLeaf(BKey key) const;
    int  LowerBound(const BKey* keys, int n, BKey key) const;

    // -- Insert helpers --
    // Returns true and sets `push_key` / `push_right` if a split occurred.
    struct SplitResult { bool split; BKey push_key; page_id_t right_pid; };
    SplitResult InsertIntoLeaf(page_id_t leaf_pid, BKey key, const BValue& val);
    SplitResult InsertIntoInternal(page_id_t node_pid, BKey key, const BValue& val,
                                   int depth);
    SplitResult InsertRecursive(page_id_t node_pid, BKey key, const BValue& val,
                                int depth);
    void SplitLeaf(Page* left, page_id_t left_pid,
                   Page* right, page_id_t right_pid,
                   BKey* push_key);
    void SplitInternal(Page* left, page_id_t left_pid,
                       Page* right, page_id_t right_pid,
                       BKey insert_key, page_id_t insert_child,
                       BKey* push_key);
    void CreateNewRoot(BKey key, page_id_t left_pid, page_id_t right_pid);

    // -- Delete helpers --
    bool DeleteFromLeaf(page_id_t pid, BKey key);

    // Height of tree (root = depth 0).
    int TreeHeight() const;
};

} // namespace ydb
