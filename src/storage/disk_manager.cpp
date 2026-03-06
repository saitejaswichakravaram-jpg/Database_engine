#include "storage/disk_manager.h"
#include <stdexcept>
#include <cstring>
#include <cassert>

namespace ydb {

DiskManager::DiskManager(const std::string& db_path,
                         const std::string& log_path)
    : db_path_(db_path),
      log_path_(log_path.empty() ? db_path + ".log" : log_path)
{
    // Open (or create) db file in binary read/write mode.
    db_io_.open(db_path_, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
        // File doesn't exist yet – create it.
        db_io_.open(db_path_, std::ios::binary | std::ios::trunc
                              | std::ios::in | std::ios::out);
        if (!db_io_.is_open())
            throw std::runtime_error("DiskManager: cannot open db file: " + db_path_);
        num_pages_ = 0;
    } else {
        // Determine how many pages are already in the file.
        db_io_.seekg(0, std::ios::end);
        auto sz = db_io_.tellg();
        num_pages_ = static_cast<int>(sz / PAGE_SIZE);
    }

    // Open (or create) log file.
    log_io_.open(log_path_, std::ios::binary | std::ios::in | std::ios::out
                            | std::ios::app);
    if (!log_io_.is_open()) {
        log_io_.open(log_path_, std::ios::binary | std::ios::trunc
                                | std::ios::in | std::ios::out);
        if (!log_io_.is_open())
            throw std::runtime_error("DiskManager: cannot open log file: " + log_path_);
    }
}

DiskManager::~DiskManager() {
    Flush();
    db_io_.close();
    log_io_.close();
}

// ── Page I/O ──────────────────────────────────────────────────────────────

void DiskManager::ReadPage(page_id_t page_id, char* dst) {
    assert(page_id >= 0);
    std::lock_guard<std::mutex> lk(io_latch_);
    db_io_.seekg(PageOffset(page_id));
    if (!db_io_) throw std::runtime_error("DiskManager::ReadPage seek failed");
    db_io_.read(dst, PAGE_SIZE);
    // A short read (e.g. freshly allocated page) → zero-fill the rest.
    auto got = db_io_.gcount();
    if (got < static_cast<std::streamsize>(PAGE_SIZE))
        memset(dst + got, 0, PAGE_SIZE - got);
    db_io_.clear(); // clear eofbit so future seeks work
}

void DiskManager::WritePage(page_id_t page_id, const char* src) {
    assert(page_id >= 0);
    std::lock_guard<std::mutex> lk(io_latch_);
    db_io_.seekp(PageOffset(page_id));
    if (!db_io_) throw std::runtime_error("DiskManager::WritePage seek failed");
    db_io_.write(src, PAGE_SIZE);
    db_io_.flush();
    if (page_id >= num_pages_) num_pages_ = page_id + 1;
}

page_id_t DiskManager::AllocatePage() {
    std::lock_guard<std::mutex> lk(io_latch_);
    page_id_t pid = num_pages_++;
    // Write a blank page so the file slot exists on disk.
    char blank[PAGE_SIZE] = {};
    db_io_.seekp(PageOffset(pid));
    db_io_.write(blank, PAGE_SIZE);
    db_io_.flush();
    return pid;
}

void DiskManager::DeallocatePage(page_id_t /*page_id*/) {
    // A production engine would maintain a free-page bitmap or linked list.
    // For this implementation we simply leave the slot in the file.
    // Upper layers (BufferPoolManager) track free frames separately.
}

// ── Log I/O ───────────────────────────────────────────────────────────────

void DiskManager::WriteLog(const char* data, size_t size) {
    std::lock_guard<std::mutex> lk(io_latch_);
    log_io_.seekp(0, std::ios::end);
    log_io_.write(data, static_cast<std::streamsize>(size));
    log_io_.flush();
}

bool DiskManager::ReadLog(char* dst, size_t size, size_t offset) {
    std::lock_guard<std::mutex> lk(io_latch_);
    log_io_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!log_io_) { log_io_.clear(); return false; }
    log_io_.read(dst, static_cast<std::streamsize>(size));
    auto got = log_io_.gcount();
    log_io_.clear();
    return got == static_cast<std::streamsize>(size);
}

size_t DiskManager::GetLogSize() {
    std::lock_guard<std::mutex> lk(io_latch_);
    log_io_.seekg(0, std::ios::end);
    auto sz = log_io_.tellg();
    log_io_.clear();
    return static_cast<size_t>(static_cast<std::streamoff>(sz) < 0 ? 0 : static_cast<std::streamoff>(sz));
}

void DiskManager::Flush() {
    if (db_io_.is_open())  db_io_.flush();
    if (log_io_.is_open()) log_io_.flush();
}

} // namespace ydb
