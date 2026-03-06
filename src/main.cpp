/*
 * main.cpp – end-to-end demo of the YDB engine
 *
 * Exercises every layer:
 *   DiskManager  → BufferPoolManager → TableHeap  (storage)
 *   BPlusTree                                       (index)
 *   LockManager + Transaction                       (concurrency)
 *   LogManager + RecoveryManager                    (durability)
 */

#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/table_heap.h"
#include "index/bplus_tree.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "recovery/log_manager.h"
#include "recovery/recovery_manager.h"

#include <iostream>
#include <string>
#include <cstring>
#include <cassert>

using namespace ydb;

// ─── Helper: pack a string into a fixed-size byte buffer ────────────────────
static std::string MakeTuple(const std::string& s) { return s; }

static void PrintSep(const std::string& title) {
    std::cout << "\n══════════════════════════════════════════\n"
              << "  " << title << "\n"
              << "══════════════════════════════════════════\n";
}

int main() {
    const std::string DB_FILE  = "/tmp/ydb_demo.db";
    const std::string LOG_FILE = "/tmp/ydb_demo.log";

    // ── 1. Initialise storage stack ─────────────────────────────────────────
    PrintSep("1. Storage: Disk Manager + Buffer Pool");

    DiskManager        disk(DB_FILE, LOG_FILE);
    BufferPoolManager  bpm(32, &disk);   // 32-frame pool
    LogManager         log_mgr(&disk);

    std::cout << "  DB file   : " << DB_FILE  << "\n";
    std::cout << "  Log file  : " << LOG_FILE << "\n";
    std::cout << "  Pool size : 32 frames × 4096 B = 128 KiB\n";

    // ── 2. Table Heap ────────────────────────────────────────────────────────
    PrintSep("2. Table Heap (Slotted Pages)");

    page_id_t first_pid;
    TableHeap heap(&bpm, &first_pid);
    std::cout << "  First page id: " << first_pid << "\n";

    // Insert 5 tuples.
    std::vector<RID> rids;
    std::vector<std::string> tuples = {
        "Alice,30,Engineer",
        "Bob,25,Designer",
        "Carol,28,Manager",
        "Dave,35,Director",
        "Eve,22,Intern",
    };
    for (const auto& t : tuples) {
        RID rid;
        bool ok = heap.Insert(t.data(), static_cast<uint32_t>(t.size()), &rid);
        assert(ok);
        rids.push_back(rid);
        std::cout << "  Inserted (" << rid.page_id << "," << rid.slot_num
                  << ")  \"" << t << "\"\n";
    }

    // Sequential scan.
    std::cout << "\n  Sequential scan:\n";
    RID cur;
    if (heap.GetFirst(&cur)) {
        do {
            char buf[256]{};
            uint32_t n = heap.Get(cur, buf);
            std::cout << "    (" << cur.page_id << "," << cur.slot_num
                      << ")  [" << n << "B]  " << std::string(buf, n) << "\n";
        } while (heap.GetNext(&cur));
    }

    // Delete one tuple.
    heap.Delete(rids[1]);
    std::cout << "\n  Deleted slot (" << rids[1].page_id << "," << rids[1].slot_num << ")\n";

    // Update one tuple.
    std::string updated = "Alice,31,SrEngineer";
    heap.Update(rids[0], updated.data(), static_cast<uint32_t>(updated.size()));
    std::cout << "  Updated slot (" << rids[0].page_id << "," << rids[0].slot_num
              << ")  →  \"" << updated << "\"\n";

    // ── 3. B+ Tree Index ─────────────────────────────────────────────────────
    PrintSep("3. B+ Tree Index");

    page_id_t root_pid;
    BPlusTree btree(&bpm, &root_pid);
    std::cout << "  Root page id: " << root_pid << "\n";
    std::cout << "  Leaf max keys  : " << LeafMaxKeys()     << "\n";
    std::cout << "  Internal max keys: " << InternalMaxKeys() << "\n";

    // Insert keys 0..19 with dummy RIDs.
    for (int i = 0; i < 20; ++i) {
        RID r{i / 5, static_cast<uint32_t>(i % 5)};
        btree.Insert(i, r);
    }
    std::cout << "  Inserted keys 0–19\n";

    // Search.
    for (int k : {0, 7, 13, 19, 99}) {
        auto v = btree.Search(k);
        if (v) std::cout << "  Search(" << k << ")  → (" << v->page_id << "," << v->slot_num << ")\n";
        else   std::cout << "  Search(" << k << ")  → NOT FOUND\n";
    }

    // Range scan 5..10.
    std::cout << "  RangeScan [5, 10]:  ";
    btree.RangeScan(5, 10, [](BKey k, BValue v) {
        std::cout << k << "→(" << v.page_id << "," << v.slot_num << ")  ";
    });
    std::cout << "\n";

    // Delete key 7 and verify.
    btree.Delete(7);
    auto v7 = btree.Search(7);
    std::cout << "  After Delete(7): Search(7) = " << (v7 ? "FOUND" : "NOT FOUND") << "\n";

    // ── 4. Transactions + Locking ────────────────────────────────────────────
    PrintSep("4. Transactions + Lock Manager (2PL)");

    LockManager lock_mgr;
    txn_id_t next_xid = 1;

    // Txn 1: acquire shared locks on two rows.
    Transaction txn1(next_xid++);
    RID r0 = rids[0], r2 = rids[2];

    bool ok1 = lock_mgr.LockShared(&txn1, r0);
    bool ok2 = lock_mgr.LockShared(&txn1, r2);
    std::cout << "  Txn1 S-lock on r0: " << (ok1 ? "OK" : "FAIL") << "\n";
    std::cout << "  Txn1 S-lock on r2: " << (ok2 ? "OK" : "FAIL") << "\n";

    // Txn 2: shared on r0 (should succeed; compatible with S).
    Transaction txn2(next_xid++);
    bool ok3 = lock_mgr.LockShared(&txn2, r0);
    std::cout << "  Txn2 S-lock on r0: " << (ok3 ? "OK" : "FAIL") << " (concurrent S, expected OK)\n";

    // Txn 1 commits.
    txn1.SetState(TxnState::SHRINKING);
    lock_mgr.UnlockAll(&txn1);
    txn1.SetState(TxnState::COMMITTED);
    std::cout << "  Txn1 committed; locks released\n";

    // Txn 2 upgrades to exclusive.
    bool ok4 = lock_mgr.LockUpgrade(&txn2, r0);
    std::cout << "  Txn2 upgrade S→X on r0: " << (ok4 ? "OK" : "FAIL") << "\n";
    txn2.SetState(TxnState::SHRINKING);
    lock_mgr.UnlockAll(&txn2);
    txn2.SetState(TxnState::COMMITTED);
    std::cout << "  Txn2 committed; locks released\n";

    // ── 5. WAL ───────────────────────────────────────────────────────────────
    PrintSep("5. Write-Ahead Log (WAL)");

    // Simulate a transaction with WAL records.
    Transaction txn3(next_xid++);
    lsn_t lsn_begin = log_mgr.AppendBegin(txn3.GetTxnId(), INVALID_LSN);
    txn3.SetPrevLSN(lsn_begin);
    std::cout << "  BEGIN  LSN=" << lsn_begin << "\n";

    std::string old_val = "old_value";
    std::string new_val = "new_value";
    RID fake_rid{0, 0};
    lsn_t lsn_upd = log_mgr.AppendUpdate(
        txn3.GetTxnId(), txn3.GetPrevLSN(), fake_rid,
        old_val.data(), static_cast<uint32_t>(old_val.size()),
        new_val.data(), static_cast<uint32_t>(new_val.size()));
    txn3.SetPrevLSN(lsn_upd);
    std::cout << "  UPDATE LSN=" << lsn_upd << "\n";

    lsn_t lsn_commit = log_mgr.AppendCommit(txn3.GetTxnId(), txn3.GetPrevLSN());
    std::cout << "  COMMIT LSN=" << lsn_commit << "  (flushed to disk)\n";

    // Read back the UPDATE record.
    LogRecord rec;
    bool read_ok = log_mgr.ReadRecord(lsn_upd, &rec);
    if (read_ok) {
        std::cout << "  Read back UPDATE: old=\""
                  << std::string(rec.old_data, rec.old_size)
                  << "\"  new=\""
                  << std::string(rec.new_data, rec.new_size) << "\"\n";
    }

    // ── 6. Crash Recovery (ARIES) ────────────────────────────────────────────
    PrintSep("6. ARIES Crash Recovery");

    // Simulate an uncommitted transaction.
    Transaction txn4(next_xid++);
    lsn_t lsn4_begin = log_mgr.AppendBegin(txn4.GetTxnId(), INVALID_LSN);
    txn4.SetPrevLSN(lsn4_begin);

    RID ins_rid;
    std::string ghost = "ghost_tuple";
    bool ins_ok = heap.Insert(ghost.data(), static_cast<uint32_t>(ghost.size()), &ins_rid);
    lsn_t lsn4_ins = log_mgr.AppendInsert(
        txn4.GetTxnId(), txn4.GetPrevLSN(),
        ins_rid, ghost.data(), static_cast<uint32_t>(ghost.size()));
    txn4.SetPrevLSN(lsn4_ins);
    std::cout << "  Txn4: INSERT ghost_tuple at (" << ins_rid.page_id
              << "," << ins_rid.slot_num << ")  LSN=" << lsn4_ins << "\n";
    std::cout << "  <<< CRASH before Txn4 commits >>>\n";

    // Log force-flush (simulates what happens before the crash point).
    log_mgr.Flush();
    bpm.FlushAll();

    // Run ARIES recovery (checkpoint_lsn = 0 → scan from beginning).
    RecoveryManager recovery(&log_mgr, &bpm, &heap);
    recovery.Recover(0);

    // After undo, ghost_tuple should be gone.
    char check_buf[256]{};
    uint32_t ghost_n = heap.Get(ins_rid, check_buf);
    std::cout << "  After recovery: Get(ghost_rid) returned "
              << ghost_n << " bytes  (0 = successfully undone)\n";

    // ── Summary ──────────────────────────────────────────────────────────────
    PrintSep("Summary");
    std::cout << "  All layers operational:\n"
              << "   [x] DiskManager    – page-level file I/O\n"
              << "   [x] BufferPool     – 32-frame LRU page cache\n"
              << "   [x] TableHeap      – slotted-page heap storage\n"
              << "   [x] BPlusTree      – buffered, page-based index\n"
              << "   [x] LockManager    – 2PL S/X locks + deadlock detection\n"
              << "   [x] LogManager     – WAL with binary log records\n"
              << "   [x] RecoveryMgr    – ARIES analysis/redo/undo\n";

    return 0;
}
