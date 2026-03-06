#pragma once
/*
 * transaction.h – transaction object
 *
 * Lifecycle
 * ─────────
 *  GROWING  → can acquire new locks (2PL phase 1)
 *  SHRINKING → can release locks; cannot acquire new ones (2PL phase 2)
 *  COMMITTED → successfully finished
 *  ABORTED   → rolled back (or killed by deadlock detector)
 *
 * Each Transaction tracks:
 *  • its own txn_id and state
 *  • prevLSN: LSN of the most-recent log record it wrote
 *    (used to build the per-transaction undo chain in recovery)
 *  • lock sets (shared / exclusive) for 2PL
 */

#include "common/types.h"
#include <set>
#include <atomic>

namespace ydb {

enum class TxnState { GROWING, SHRINKING, COMMITTED, ABORTED };
enum class IsolationLevel { READ_UNCOMMITTED, READ_COMMITTED,
                             REPEATABLE_READ, SERIALIZABLE };

class Transaction {
public:
    explicit Transaction(txn_id_t id,
                         IsolationLevel iso = IsolationLevel::SERIALIZABLE)
        : txn_id_(id), state_(TxnState::GROWING), iso_level_(iso),
          prev_lsn_(INVALID_LSN) {}

    txn_id_t       GetTxnId()       const { return txn_id_; }
    TxnState       GetState()        const { return state_; }
    IsolationLevel GetIsolation()    const { return iso_level_; }
    lsn_t          GetPrevLSN()      const { return prev_lsn_; }
    void           SetPrevLSN(lsn_t l)    { prev_lsn_ = l; }
    void           SetState(TxnState s)   { state_ = s; }

    // Lock tracking sets (maintained by LockManager).
    std::set<RID>& SharedLockSet()    { return shared_lock_set_; }
    std::set<RID>& ExclusiveLockSet() { return exclusive_lock_set_; }

private:
    txn_id_t        txn_id_;
    TxnState        state_;
    IsolationLevel  iso_level_;
    lsn_t           prev_lsn_;

    std::set<RID>   shared_lock_set_;
    std::set<RID>   exclusive_lock_set_;
};

} // namespace ydb
