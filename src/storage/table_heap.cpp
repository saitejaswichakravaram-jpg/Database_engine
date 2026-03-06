#include "storage/table_heap.h"
#include <cstring>
#include <stdexcept>

namespace ydb {

// ── Static helpers ───────────────────────────────────────────────────────────

page_id_t TableHeap::GetNextPageId(const Page* page) {
    page_id_t nxt;
    memcpy(&nxt, page->GetRaw() + NEXT_PAGE_ID_OFFSET, sizeof(page_id_t));
    return nxt;
}

void TableHeap::SetNextPageId(Page* page, page_id_t next) {
    memcpy(page->GetRaw() + NEXT_PAGE_ID_OFFSET, &next, sizeof(page_id_t));
    page->SetDirty(true);
}

// ── Constructors ────────────────────────────────────────────────────────────

TableHeap::TableHeap(BufferPoolManager* bpm, page_id_t* first_page_id_out)
    : bpm_(bpm)
{
    page_id_t pid;
    Page* page = bpm_->NewPage(&pid);
    if (!page) throw std::runtime_error("TableHeap: cannot allocate first page");

    TablePage tp;
    tp.Init(page, pid);
    SetNextPageId(page, INVALID_PAGE_ID);

    *first_page_id_out = pid;
    first_page_id_ = pid;
    last_page_id_  = pid;
    bpm_->UnpinPage(pid, true);
}

TableHeap::TableHeap(BufferPoolManager* bpm, page_id_t first_page_id)
    : bpm_(bpm), first_page_id_(first_page_id), last_page_id_(first_page_id)
{
    // Walk to find last page.
    page_id_t cur = first_page_id;
    while (true) {
        Page* page = bpm_->FetchPage(cur);
        if (!page) break;
        page_id_t nxt = GetNextPageId(page);
        bpm_->UnpinPage(cur, false);
        if (nxt == INVALID_PAGE_ID) { last_page_id_ = cur; break; }
        cur = nxt;
    }
}

// ── Private: append page ────────────────────────────────────────────────────

page_id_t TableHeap::AppendPage() {
    page_id_t new_pid;
    Page* new_page = bpm_->NewPage(&new_pid);
    if (!new_page) return INVALID_PAGE_ID;

    TablePage tp;
    tp.Init(new_page, new_pid);
    SetNextPageId(new_page, INVALID_PAGE_ID);
    bpm_->UnpinPage(new_pid, true);

    // Stitch old last page → new page.
    Page* last = bpm_->FetchPage(last_page_id_);
    if (last) {
        SetNextPageId(last, new_pid);
        bpm_->UnpinPage(last_page_id_, true);
    }
    last_page_id_ = new_pid;
    return new_pid;
}

// ── Mutation ────────────────────────────────────────────────────────────────

bool TableHeap::Insert(const char* data, uint32_t size, RID* rid) {
    page_id_t cur = last_page_id_;

    // Try the last page first; if full, append a new one.
    for (int attempts = 0; attempts < 2; ++attempts) {
        Page* page = bpm_->FetchPage(cur);
        if (!page) return false;
        TablePage tp;
        tp.Attach(page);
        bool ok = tp.InsertTuple(data, size, rid);
        bpm_->UnpinPage(cur, ok);
        if (ok) return true;
        // Not enough room – append a new page.
        cur = AppendPage();
        if (cur == INVALID_PAGE_ID) return false;
    }
    return false;
}

bool TableHeap::Delete(const RID& rid) {
    Page* page = bpm_->FetchPage(rid.page_id);
    if (!page) return false;
    TablePage tp;
    tp.Attach(page);
    bool ok = tp.DeleteTuple(rid);
    bpm_->UnpinPage(rid.page_id, ok);
    return ok;
}

bool TableHeap::Update(const RID& rid, const char* data, uint32_t size) {
    Page* page = bpm_->FetchPage(rid.page_id);
    if (!page) return false;
    TablePage tp;
    tp.Attach(page);
    bool ok = tp.UpdateTuple(rid, data, size);
    bpm_->UnpinPage(rid.page_id, ok);
    return ok;
}

// ── Access ───────────────────────────────────────────────────────────────────

uint32_t TableHeap::Get(const RID& rid, char* dst) const {
    Page* page = bpm_->FetchPage(rid.page_id);
    if (!page) return 0;
    TablePage tp;
    tp.Attach(page);
    uint32_t n = tp.GetTuple(rid, dst);
    bpm_->UnpinPage(rid.page_id, false);
    return n;
}

// ── Scan ─────────────────────────────────────────────────────────────────────

bool TableHeap::GetFirst(RID* rid) const {
    page_id_t cur = first_page_id_;
    while (cur != INVALID_PAGE_ID) {
        Page* page = bpm_->FetchPage(cur);
        if (!page) return false;
        page_id_t nxt = GetNextPageId(page);
        TablePage tp;
        tp.Attach(page);
        rid->page_id  = cur;
        rid->slot_num = 0;
        bool found = tp.GetNextTuple(rid);
        bpm_->UnpinPage(cur, false);
        if (found) return true;
        cur = nxt;
    }
    return false;
}

bool TableHeap::GetNext(RID* rid) const {
    // Try next slot on same page.
    page_id_t cur = rid->page_id;
    Page* page = bpm_->FetchPage(cur);
    if (!page) return false;
    page_id_t nxt = GetNextPageId(page);
    TablePage tp;
    tp.Attach(page);
    rid->slot_num++;
    bool found = tp.GetNextTuple(rid);
    bpm_->UnpinPage(cur, false);
    if (found) return true;

    // Move to subsequent pages.
    cur = nxt;
    while (cur != INVALID_PAGE_ID) {
        page = bpm_->FetchPage(cur);
        if (!page) return false;
        nxt = GetNextPageId(page);
        tp.Attach(page);
        rid->page_id  = cur;
        rid->slot_num = 0;
        found = tp.GetNextTuple(rid);
        bpm_->UnpinPage(cur, false);
        if (found) return true;
        cur = nxt;
    }
    return false;
}

} // namespace ydb
