#pragma once
/*
 * disk_manager.h – raw disk I/O
 *
 * Responsibilities
 * ────────────────
 *  • Open / create the database file and the WAL log file.
 *  • Read / write individual pages by page_id.
 *    Page n starts at byte offset  n * PAGE_SIZE  in the db file.
 *  • Allocate new pages (append to file, or reclaim from the free list).
 *  • Append log records to the log file and read them back for recovery.
 *
 * Thread safety
 * ─────────────
 *  All public methods acquire io_latch_ before touching file handles.
 */

#include "common/types.h"
#include <string>
#include <fstream>
#include <mutex>

namespace ydb {

class DiskManager {
public:
    // Open (or create) a database.
    // db_path   – path to the main database file  (e.g. "test.ydb")
    // log_path  – path to the WAL log file        (e.g. "test.ylog")
    explicit DiskManager(const std::string& db_path,
                         const std::string& log_path = "");

    ~DiskManager();

    // ── Page I/O ──────────────────────────────────────────────────────────
    // Read page `page_id` into `dst` (exactly PAGE_SIZE bytes).
    void ReadPage(page_id_t page_id, char* dst);

    // Write PAGE_SIZE bytes from `src` to the slot for `page_id`.
    void WritePage(page_id_t page_id, const char* src);

    // Allocate a new, zeroed page and return its id.
    page_id_t AllocatePage();

    // Mark `page_id` as free (pushed onto in-memory free list;
    // the actual slot is zeroed lazily on next reuse).
    void DeallocatePage(page_id_t page_id);

    int GetNumPages() const { return num_pages_; }

    // ── Log I/O ───────────────────────────────────────────────────────────
    // Append `size` bytes to the WAL log file.
    void WriteLog(const char* data, size_t size);

    // Read `size` bytes from `offset` in the log file into `dst`.
    // Returns false if the requested range is past end-of-file.
    bool ReadLog(char* dst, size_t size, size_t offset);

    // Current size of the log file in bytes.
    size_t GetLogSize();

    // Flush all OS-level buffers.
    void Flush();

private:
    std::string  db_path_;
    std::string  log_path_;
    std::fstream db_io_;
    std::fstream log_io_;

    int          num_pages_ = 0;  // total pages ever allocated
    std::mutex   io_latch_;

    // Compute byte offset for page_id in the db file.
    static std::streampos PageOffset(page_id_t pid) {
        return static_cast<std::streampos>(pid) * PAGE_SIZE;
    }
};

} // namespace ydb
