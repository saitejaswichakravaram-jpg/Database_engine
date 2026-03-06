#include "index/bplus_tree.h"
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace ydb {

// ── Constructor helpers ──────────────────────────────────────────────────────

Page* BPlusTree::NewLeafPage(page_id_t* pid) {
    Page* p = bpm_->NewPage(pid);
    if (!p) throw std::runtime_error("BPlusTree: out of buffer frames");
    p->SetPageType(PageType::BPLUS_LEAF);
    BPlusNodeHeader* h = NodeHeader(p);
    h->is_leaf   = 1;
    h->num_keys  = 0;
    h->max_keys  = LeafMaxKeys();
    *LeafNextPtr(p) = INVALID_PAGE_ID;
    p->SetDirty(true);
    return p;
}

Page* BPlusTree::NewInternalPage(page_id_t* pid) {
    Page* p = bpm_->NewPage(pid);
    if (!p) throw std::runtime_error("BPlusTree: out of buffer frames");
    p->SetPageType(PageType::BPLUS_INTERNAL);
    BPlusNodeHeader* h = NodeHeader(p);
    h->is_leaf   = 0;
    h->num_keys  = 0;
    h->max_keys  = InternalMaxKeys();
    p->SetDirty(true);
    return p;
}

// ── Tree creation / opening ──────────────────────────────────────────────────

BPlusTree::BPlusTree(BufferPoolManager* bpm, page_id_t* root_page_id_out)
    : bpm_(bpm)
{
    Page* root = NewLeafPage(&root_page_id_);
    *root_page_id_out = root_page_id_;
    bpm_->UnpinPage(root_page_id_, true);
}

BPlusTree::BPlusTree(BufferPoolManager* bpm, page_id_t root_page_id)
    : bpm_(bpm), root_page_id_(root_page_id) {}

// ── Utilities ────────────────────────────────────────────────────────────────

bool BPlusTree::IsLeaf(page_id_t pid) const {
    Page* p = bpm_->FetchPage(pid);
    if (!p) return false;
    bool leaf = NodeHeader(p)->is_leaf != 0;
    bpm_->UnpinPage(pid, false);
    return leaf;
}

// Binary search: first index i where keys[i] >= key.
int BPlusTree::LowerBound(const BKey* keys, int n, BKey key) const {
    int lo = 0, hi = n;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (keys[mid] < key) lo = mid + 1;
        else                 hi = mid;
    }
    return lo;
}

int BPlusTree::TreeHeight() const {
    int h = 0;
    page_id_t cur = root_page_id_;
    while (true) {
        Page* p = bpm_->FetchPage(cur);
        if (!p) break;
        bool leaf = NodeHeader(p)->is_leaf != 0;
        page_id_t child = leaf ? INVALID_PAGE_ID : InternalChildren(p)[0];
        bpm_->UnpinPage(cur, false);
        if (leaf) break;
        cur = child;
        ++h;
    }
    return h;
}

// ── Search ───────────────────────────────────────────────────────────────────

page_id_t BPlusTree::FindLeaf(BKey key) const {
    page_id_t cur = root_page_id_;
    while (true) {
        Page* p = bpm_->FetchPage(cur);
        if (!p) return INVALID_PAGE_ID;
        bool leaf = NodeHeader(p)->is_leaf != 0;
        if (leaf) { bpm_->UnpinPage(cur, false); return cur; }
        // Internal: descend into correct child.
        int n = NodeHeader(p)->num_keys;
        const BKey* keys = InternalKeys(p);
        int idx = LowerBound(keys, n, key);
        // children[idx] is the subtree for keys < keys[idx]
        // if key == keys[idx], we go right (idx+1) because we use strict <
        if (idx < n && keys[idx] == key) idx++;
        // Actually standard B+ tree: go to children[idx] where idx = first i with key < keys[i]
        // Re-derive: we want child for range [keys[idx-1], keys[idx])
        // LowerBound gives first i where keys[i] >= key → that's idx
        // So we take child[idx]
        page_id_t child = InternalChildren(p)[idx];
        bpm_->UnpinPage(cur, false);
        cur = child;
    }
}

std::optional<BValue> BPlusTree::Search(BKey key) const {
    page_id_t leaf_pid = FindLeaf(key);
    if (leaf_pid == INVALID_PAGE_ID) return std::nullopt;
    Page* p = bpm_->FetchPage(leaf_pid);
    if (!p) return std::nullopt;
    int n = NodeHeader(p)->num_keys;
    const BKey* keys = LeafKeys(p);
    int idx = LowerBound(keys, n, key);
    std::optional<BValue> result;
    if (idx < n && keys[idx] == key)
        result = LeafValues(p)[idx];
    bpm_->UnpinPage(leaf_pid, false);
    return result;
}

void BPlusTree::RangeScan(BKey low, BKey high,
                           const std::function<void(BKey, BValue)>& cb) const {
    page_id_t cur = FindLeaf(low);
    while (cur != INVALID_PAGE_ID) {
        Page* p = bpm_->FetchPage(cur);
        if (!p) break;
        int n = NodeHeader(p)->num_keys;
        const BKey*   keys = LeafKeys(p);
        const BValue* vals = LeafValues(p);
        page_id_t nxt = *LeafNextPtr(p);
        bool done = false;
        for (int i = 0; i < n; ++i) {
            if (keys[i] > high) { done = true; break; }
            if (keys[i] >= low) cb(keys[i], vals[i]);
        }
        bpm_->UnpinPage(cur, false);
        if (done) break;
        cur = nxt;
    }
}

// ── Insert ───────────────────────────────────────────────────────────────────

void BPlusTree::SplitLeaf(Page* left, page_id_t left_pid,
                           Page* right, page_id_t right_pid,
                           BKey* push_key)
{
    BPlusNodeHeader* lh = NodeHeader(left);
    BPlusNodeHeader* rh = NodeHeader(right);
    int total    = lh->num_keys;
    int half     = total / 2;
    int max_keys = lh->max_keys;

    BKey*   lkeys = LeafKeys(left);
    BValue* lvals = LeafValues(left);
    BKey*   rkeys = LeafKeys(right);
    BValue* rvals = LeafValues(right);

    // Copy upper half to right leaf.
    int r_count = total - half;
    memcpy(rkeys, lkeys + half, r_count * sizeof(BKey));
    memcpy(rvals, lvals + half, r_count * sizeof(BValue));
    rh->num_keys = r_count;
    rh->max_keys = max_keys;

    // Chain right after left.
    *LeafNextPtr(right) = *LeafNextPtr(left);
    *LeafNextPtr(left)  = right_pid;

    lh->num_keys = half;
    *push_key = rkeys[0]; // duplicate: smallest key in right also stays in left

    left->SetDirty(true);
    right->SetDirty(true);
    (void)left_pid;
}

void BPlusTree::SplitInternal(Page* left, page_id_t left_pid,
                               Page* right, page_id_t right_pid,
                               BKey insert_key, page_id_t insert_child,
                               BKey* push_key)
{
    BPlusNodeHeader* lh = NodeHeader(left);
    int max_keys  = lh->max_keys;
    int total     = max_keys + 1;  // temporarily one over capacity

    // Build temporary arrays.
    std::vector<BKey>      tmp_keys(total);
    std::vector<page_id_t> tmp_children(total + 1);

    BKey*      lkeys = InternalKeys(left);
    page_id_t* lch   = InternalChildren(left);

    // Find where insert_key fits.
    int pos = LowerBound(lkeys, lh->num_keys, insert_key);

    memcpy(tmp_keys.data(),    lkeys, pos * sizeof(BKey));
    tmp_keys[pos] = insert_key;
    memcpy(tmp_keys.data() + pos + 1, lkeys + pos,
           (lh->num_keys - pos) * sizeof(BKey));

    memcpy(tmp_children.data(), lch, (pos + 1) * sizeof(page_id_t));
    tmp_children[pos + 1] = insert_child;
    memcpy(tmp_children.data() + pos + 2, lch + pos + 1,
           (lh->num_keys - pos) * sizeof(page_id_t));

    // Split: left gets first half, right gets second half.
    int left_count  = (total - 1) / 2;  // keys in left after split
    *push_key       = tmp_keys[left_count]; // middle key pushed up (not stored)
    int right_count = total - 1 - left_count; // keys in right

    // Populate left.
    memcpy(lkeys, tmp_keys.data(), left_count * sizeof(BKey));
    memcpy(lch,   tmp_children.data(), (left_count + 1) * sizeof(page_id_t));
    lh->num_keys  = left_count;
    lh->max_keys  = max_keys;

    // Populate right.
    BPlusNodeHeader* rh = NodeHeader(right);
    BKey*      rkeys = InternalKeys(right);
    page_id_t* rch   = InternalChildren(right);
    memcpy(rkeys, tmp_keys.data() + left_count + 1, right_count * sizeof(BKey));
    memcpy(rch,   tmp_children.data() + left_count + 1,
           (right_count + 1) * sizeof(page_id_t));
    rh->num_keys = right_count;
    rh->max_keys = max_keys;
    rh->is_leaf  = 0;

    left->SetDirty(true);
    right->SetDirty(true);
    (void)left_pid; (void)right_pid;
}

void BPlusTree::CreateNewRoot(BKey key, page_id_t left_pid, page_id_t right_pid) {
    page_id_t new_root_pid;
    Page* root = NewInternalPage(&new_root_pid);
    BPlusNodeHeader* h = NodeHeader(root);
    InternalKeys(root)[0]     = key;
    InternalChildren(root)[0] = left_pid;
    InternalChildren(root)[1] = right_pid;
    h->num_keys = 1;
    bpm_->UnpinPage(new_root_pid, true);
    root_page_id_ = new_root_pid;
}

BPlusTree::SplitResult BPlusTree::InsertRecursive(page_id_t pid, BKey key,
                                                    const BValue& val, int depth)
{
    Page* p = bpm_->FetchPage(pid);
    if (!p) return {false, 0, INVALID_PAGE_ID};
    bool leaf = NodeHeader(p)->is_leaf != 0;
    bpm_->UnpinPage(pid, false);

    if (leaf) {
        return InsertIntoLeaf(pid, key, val);
    } else {
        return InsertIntoInternal(pid, key, val, depth);
    }
}

BPlusTree::SplitResult BPlusTree::InsertIntoLeaf(page_id_t pid, BKey key,
                                                   const BValue& val)
{
    Page* p = bpm_->FetchPage(pid);
    if (!p) return {false, 0, INVALID_PAGE_ID};
    BPlusNodeHeader* h = NodeHeader(p);
    BKey*   keys = LeafKeys(p);
    BValue* vals = LeafValues(p);
    int n = h->num_keys;

    int pos = LowerBound(keys, n, key);
    if (pos < n && keys[pos] == key) {
        // Duplicate key: update value in place.
        vals[pos] = val;
        p->SetDirty(true);
        bpm_->UnpinPage(pid, true);
        return {false, 0, INVALID_PAGE_ID};
    }

    if (n < h->max_keys) {
        // Room available: shift and insert.
        memmove(keys + pos + 1, keys + pos, (n - pos) * sizeof(BKey));
        memmove(vals + pos + 1, vals + pos, (n - pos) * sizeof(BValue));
        keys[pos] = key;
        vals[pos] = val;
        h->num_keys++;
        p->SetDirty(true);
        bpm_->UnpinPage(pid, true);
        return {false, 0, INVALID_PAGE_ID};
    }

    // Leaf is full: insert then split.
    // Temporarily expand (we have the page exclusively – safe for in-memory op).
    memmove(keys + pos + 1, keys + pos, (n - pos) * sizeof(BKey));
    memmove(vals + pos + 1, vals + pos, (n - pos) * sizeof(BValue));
    keys[pos] = key;
    vals[pos] = val;
    h->num_keys = n + 1; // temporarily over-full

    page_id_t right_pid;
    Page* right = NewLeafPage(&right_pid);
    BKey push_key;
    SplitLeaf(p, pid, right, right_pid, &push_key);
    bpm_->UnpinPage(right_pid, true);
    bpm_->UnpinPage(pid, true);
    return {true, push_key, right_pid};
}

BPlusTree::SplitResult BPlusTree::InsertIntoInternal(page_id_t pid, BKey key,
                                                       const BValue& val, int depth)
{
    Page* p = bpm_->FetchPage(pid);
    if (!p) return {false, 0, INVALID_PAGE_ID};
    BPlusNodeHeader* h = NodeHeader(p);
    const BKey* keys   = InternalKeys(p);
    int n = h->num_keys;

    // Descend into correct child.
    int pos = LowerBound(keys, n, key);
    if (pos < n && keys[pos] == key) pos++;
    page_id_t child_pid = InternalChildren(p)[pos];
    bpm_->UnpinPage(pid, false);

    SplitResult child_result = InsertRecursive(child_pid, key, val, depth + 1);
    if (!child_result.split) return {false, 0, INVALID_PAGE_ID};

    // Child split: insert push_key + right child into this node.
    p = bpm_->FetchPage(pid);
    if (!p) return {false, 0, INVALID_PAGE_ID};
    h    = NodeHeader(p);
    n    = h->num_keys;
    keys = InternalKeys(p);

    if (n < h->max_keys) {
        // Room: insert in place.
        BKey*      ikeys = InternalKeys(p);
        page_id_t* ich   = InternalChildren(p);
        int ins = LowerBound(ikeys, n, child_result.push_key);
        memmove(ikeys + ins + 1, ikeys + ins, (n - ins) * sizeof(BKey));
        memmove(ich + ins + 2, ich + ins + 1, (n - ins) * sizeof(page_id_t));
        ikeys[ins]     = child_result.push_key;
        ich[ins + 1]   = child_result.right_pid;
        h->num_keys++;
        p->SetDirty(true);
        bpm_->UnpinPage(pid, true);
        return {false, 0, INVALID_PAGE_ID};
    }

    // Internal is full: split.
    page_id_t right_pid;
    Page* right = NewInternalPage(&right_pid);
    BKey push_key;
    SplitInternal(p, pid, right, right_pid,
                  child_result.push_key, child_result.right_pid, &push_key);
    bpm_->UnpinPage(right_pid, true);
    bpm_->UnpinPage(pid, true);
    return {true, push_key, right_pid};
}

bool BPlusTree::Insert(BKey key, const BValue& value) {
    SplitResult res = InsertRecursive(root_page_id_, key, value, 0);
    if (res.split) CreateNewRoot(res.push_key, root_page_id_, res.right_pid);
    // After creating new root, old root_page_id_ was moved to left child.
    // CreateNewRoot already updates root_page_id_.
    return true;
}

// ── Delete (lazy: just remove from leaf; no rebalancing for brevity) ────────

bool BPlusTree::DeleteFromLeaf(page_id_t pid, BKey key) {
    Page* p = bpm_->FetchPage(pid);
    if (!p) return false;
    BPlusNodeHeader* h = NodeHeader(p);
    BKey*   keys = LeafKeys(p);
    BValue* vals = LeafValues(p);
    int n = h->num_keys;
    int idx = LowerBound(keys, n, key);
    if (idx >= n || keys[idx] != key) {
        bpm_->UnpinPage(pid, false);
        return false; // not found
    }
    memmove(keys + idx, keys + idx + 1, (n - idx - 1) * sizeof(BKey));
    memmove(vals + idx, vals + idx + 1, (n - idx - 1) * sizeof(BValue));
    h->num_keys--;
    p->SetDirty(true);
    bpm_->UnpinPage(pid, true);
    return true;
}

bool BPlusTree::Delete(BKey key) {
    page_id_t leaf = FindLeaf(key);
    if (leaf == INVALID_PAGE_ID) return false;
    return DeleteFromLeaf(leaf, key);
}

} // namespace ydb
