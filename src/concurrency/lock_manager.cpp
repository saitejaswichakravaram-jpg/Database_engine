#include "concurrency/lock_manager.h"
#include <algorithm>
#include <stdexcept>

namespace ydb {

// ── Internal helpers ──────────────────────────────────────────────────────────

LockRequestQueue& LockManager::GetQueue(const RID& rid) {
    return lock_table_[rid.page_id][rid.slot_num];
}

// Compatible if no EXCLUSIVE grant exists (and incoming is not X conflicting with S).
bool LockManager::IsCompatible(const LockRequestQueue& q, LockMode incoming) {
    for (const auto& req : q.requests) {
        if (!req.granted) continue;
        if (req.mode == LockMode::EXCLUSIVE) return false;
        if (incoming == LockMode::EXCLUSIVE) return false;
    }
    return true;
}

// Try to grant the next compatible waiting requests (FIFO).
void LockManager::GrantWaiting(LockRequestQueue& q) {
    bool notified = false;
    for (auto& req : q.requests) {
        if (req.granted) continue;
        if (!IsCompatible(q, req.mode)) break;
        req.granted = true;
        notified    = true;
    }
    if (notified) q.cv.notify_all();
}

// ── Public API ────────────────────────────────────────────────────────────────

bool LockManager::LockShared(Transaction* txn, const RID& rid) {
    if (txn->GetState() == TxnState::SHRINKING)
        throw std::runtime_error("2PL violation: lock after shrinking phase");

    std::unique_lock<std::mutex> lk(latch_);
    auto& q = GetQueue(rid);

    if (txn->ExclusiveLockSet().count(rid) || txn->SharedLockSet().count(rid))
        return true;

    q.requests.push_back({txn->GetTxnId(), LockMode::SHARED, false});
    auto it = std::prev(q.requests.end());

    // Attempt immediate grant; cv.wait checks predicate before blocking.
    GrantWaiting(q);

    q.cv.wait(lk, [&] {
        return txn->GetState() == TxnState::ABORTED || it->granted;
    });

    if (txn->GetState() == TxnState::ABORTED) {
        q.requests.erase(it);
        return false;
    }

    waits_for_.erase(txn->GetTxnId());
    txn->SharedLockSet().insert(rid);
    return true;
}

bool LockManager::LockExclusive(Transaction* txn, const RID& rid) {
    if (txn->GetState() == TxnState::SHRINKING)
        throw std::runtime_error("2PL violation: lock after shrinking phase");

    std::unique_lock<std::mutex> lk(latch_);
    auto& q = GetQueue(rid);

    if (txn->ExclusiveLockSet().count(rid)) return true;

    q.requests.push_back({txn->GetTxnId(), LockMode::EXCLUSIVE, false});
    auto it = std::prev(q.requests.end());

    for (const auto& req : q.requests) {
        if (req.granted && req.txn_id != txn->GetTxnId())
            waits_for_[txn->GetTxnId()].insert(req.txn_id);
    }

    GrantWaiting(q);

    q.cv.wait(lk, [&] {
        return txn->GetState() == TxnState::ABORTED || it->granted;
    });

    if (txn->GetState() == TxnState::ABORTED) {
        q.requests.erase(it);
        return false;
    }

    waits_for_.erase(txn->GetTxnId());
    if (txn->SharedLockSet().count(rid)) txn->SharedLockSet().erase(rid);
    txn->ExclusiveLockSet().insert(rid);
    return true;
}

bool LockManager::LockUpgrade(Transaction* txn, const RID& rid) {
    if (!txn->SharedLockSet().count(rid)) return false;

    std::unique_lock<std::mutex> lk(latch_);
    auto& q = GetQueue(rid);

    for (auto& req : q.requests) {
        if (req.txn_id == txn->GetTxnId() && req.mode == LockMode::SHARED) {
            req.mode    = LockMode::EXCLUSIVE;
            req.granted = false;
            break;
        }
    }

    GrantWaiting(q);

    q.cv.wait(lk, [&] {
        for (const auto& req : q.requests)
            if (req.txn_id == txn->GetTxnId()) return req.granted;
        return true;
    });

    txn->SharedLockSet().erase(rid);
    txn->ExclusiveLockSet().insert(rid);
    return true;
}

bool LockManager::Unlock(Transaction* txn, const RID& rid) {
    std::lock_guard<std::mutex> lk(latch_);
    auto& q = GetQueue(rid);
    for (auto it = q.requests.begin(); it != q.requests.end(); ++it) {
        if (it->txn_id == txn->GetTxnId()) {
            q.requests.erase(it);
            if (txn->GetState() == TxnState::GROWING)
                txn->SetState(TxnState::SHRINKING);
            GrantWaiting(q);
            return true;
        }
    }
    return false;
}

void LockManager::UnlockAll(Transaction* txn) {
    std::vector<RID> to_unlock;
    for (const auto& r : txn->SharedLockSet())    to_unlock.push_back(r);
    for (const auto& r : txn->ExclusiveLockSet()) to_unlock.push_back(r);
    for (const auto& r : to_unlock) Unlock(txn, r);
    txn->SharedLockSet().clear();
    txn->ExclusiveLockSet().clear();
}

// ── Deadlock detection ────────────────────────────────────────────────────────

bool LockManager::HasCycle(txn_id_t start, std::set<txn_id_t>& visited,
                            std::set<txn_id_t>& rec_stack,
                            txn_id_t* victim) const {
    visited.insert(start);
    rec_stack.insert(start);

    auto it = waits_for_.find(start);
    if (it != waits_for_.end()) {
        for (txn_id_t nbr : it->second) {
            if (!visited.count(nbr)) {
                if (HasCycle(nbr, visited, rec_stack, victim)) return true;
            } else if (rec_stack.count(nbr)) {
                *victim = *rec_stack.rbegin();
                return true;
            }
        }
    }
    rec_stack.erase(start);
    return false;
}

txn_id_t LockManager::DetectDeadlock() {
    std::lock_guard<std::mutex> lk(latch_);
    std::set<txn_id_t> visited, rec_stack;
    txn_id_t victim = INVALID_TXN_ID;

    for (const auto& [tid, _] : waits_for_)
        if (!visited.count(tid))
            if (HasCycle(tid, visited, rec_stack, &victim)) break;

    return victim;
}

} // namespace ydb
