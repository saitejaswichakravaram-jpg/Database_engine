#pragma once
/*
 * lock_manager.h – row-level 2PL lock manager with deadlock detection
 *
 * Lock modes
 * ──────────
 *  SHARED    (S) – read-only; multiple holders allowed simultaneously.
 *  EXCLUSIVE (X) – read-write; only one holder; blocks all others.
 *
 * Compatibility matrix
 *         S    X
 *   S   [ ✓  | ✗ ]
 *   X   [ ✗  | ✗ ]
 *
 * Protocol (Strict 2PL)
 * ─────────────────────
 *  • Growing phase: transaction can acquire new locks.
 *  • Shrinking phase: ALL locks released at commit / abort (not individually).
 *  • This gives conflict-serializability and cascadeless aborts.
 *
 * Deadlock detection
 * ──────────────────
 *  A background-style detector builds a wait-for graph and runs DFS.
 *  If a cycle is found, the youngest transaction (highest txn_id) is
 *  chosen as the victim and aborted.
 *
 * Thread safety
 * ─────────────
 *  latch_ protects the request queues and the wait-for graph.
 */

#include "concurrency/transaction.h"
#include <unordered_map>
#include <list>
#include <condition_variable>
#include <mutex>
#include <vector>
#include <set>

namespace ydb {

enum class LockMode { SHARED, EXCLUSIVE };

struct LockRequest {
    txn_id_t txn_id;
    LockMode mode;
    bool     granted = false;
};

struct LockRequestQueue {
    std::list<LockRequest> requests;
    std::condition_variable cv; // woken when the head request may be granted
};

class LockManager {
public:
    LockManager() = default;
    ~LockManager() = default;

    // Acquire a shared lock on `rid` for `txn`.
    // Blocks until granted or the transaction is aborted (returns false).
    bool LockShared(Transaction* txn, const RID& rid);

    // Acquire an exclusive lock on `rid` for `txn`.
    bool LockExclusive(Transaction* txn, const RID& rid);

    // Upgrade a held shared lock to exclusive.
    bool LockUpgrade(Transaction* txn, const RID& rid);

    // Release ONE lock.  (Usually called only via UnlockAll.)
    bool Unlock(Transaction* txn, const RID& rid);

    // Release all locks held by `txn` (called at commit / abort).
    void UnlockAll(Transaction* txn);

    // Run cycle detection; abort the youngest waiter in any cycle.
    // Returns the aborted txn_id, or INVALID_TXN_ID if no cycle.
    txn_id_t DetectDeadlock();

private:
    std::mutex latch_;
    std::unordered_map<page_id_t,
        std::unordered_map<slot_t, LockRequestQueue>> lock_table_;
    // Who is waiting for whom.
    std::unordered_map<txn_id_t, std::set<txn_id_t>> waits_for_;

    LockRequestQueue& GetQueue(const RID& rid);

    // Grant the next compatible requests in a queue after a release.
    void GrantWaiting(LockRequestQueue& q);

    // Returns true if `incoming` is compatible with all currently-granted modes.
    static bool IsCompatible(const LockRequestQueue& q, LockMode incoming);

    // DFS cycle detection.
    bool HasCycle(txn_id_t start, std::set<txn_id_t>& visited,
                  std::set<txn_id_t>& rec_stack, txn_id_t* victim) const;
};

} // namespace ydb
