#include "buffer_pool.h"
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdexcept>

namespace pageforge {

/**
 * @brief Constructs a DiskManager and opens the database file.
 * @param db_path Path to the file on disk.
 */
DiskManager::DiskManager(const std::string& db_path) {
    // Open with O_RDWR for read/write access and O_CREAT to initialize the file if missing.
    fd_ = ::open(db_path.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("DiskManager: Failed to open " + db_path);
    }

    // Determine initial page count by dividing total file bytes by the 4 KiB page size.
    struct stat st{};
    ::fstat(fd_, &st);
    page_count_ = static_cast<int>(st.st_size / PAGE_SIZE);
}

DiskManager::~DiskManager() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

/**
 * @brief Extends the file by one page and fills it with zeros.
 * @return The new page's physical index.
 */
int DiskManager::alloc_page() {
    int new_page = page_count_++;
    std::byte zeros[PAGE_SIZE] = {};
    // Physically extend the file on the filesystem to reserve the block.
    write_page(new_page, zeros);
    return new_page;
}

/**
 * @brief Reads a 4 KiB block from a specific offset in the file.
 * @param page_num The index of the page to read.
 * @param dst Pointer to the memory buffer where data will be stored.
 */
void DiskManager::read_page(int page_num, std::byte* dst) {
    assert(page_num < page_count_);
    off_t offset = static_cast<off_t>(page_num) * PAGE_SIZE;
    
    // pread provides atomic, thread-safe I/O without modifying the global file offset pointer.
    ssize_t n = ::pread(fd_, dst, PAGE_SIZE, offset);
    if (n != static_cast<ssize_t>(PAGE_SIZE)) {
        throw std::runtime_error("DiskManager: Short read on page " + std::to_string(page_num));
    }
}

/**
 * @brief Writes a 4 KiB block to a specific offset in the file.
 * @param page_num The index of the page to write.
 * @param src Pointer to the data to be written.
 */
void DiskManager::write_page(int page_num, const std::byte* src) {
    off_t offset = static_cast<off_t>(page_num) * PAGE_SIZE;
    
    // pwrite ensures the write happens at the correct physical offset regardless of concurrent operations.
    ssize_t n = ::pwrite(fd_, src, PAGE_SIZE, offset);
    if (n != static_cast<ssize_t>(PAGE_SIZE)) {
        throw std::runtime_error("DiskManager: Short write on page " + std::to_string(page_num));
    }
}

/**
 * @brief Initializes the BufferPool with a specific capacity.
 * @param num_frames Total frames available in memory.
 * @param disk Pointer to the DiskManager for I/O operations.
 */
BufferPool::BufferPool(size_t num_frames, DiskManager* disk)
    : frames_(num_frames), disk_(disk) {
    if (num_frames == 0) {
        throw std::invalid_argument("BufferPool: Size must be greater than zero.");
    }
}

/**
 * @brief Pins a page in the buffer pool, loading it if not already resident.
 * @param pid Unique ID of the requested page.
 * @return Raw pointer to the page data in memory.
 */
std::byte* BufferPool::pin_page(PageId pid) {
    auto it = page_table_.find(pid);
    if (it != page_table_.end()) {
        // Cache hit: Increment pin count and update ref_bit for the Clock algorithm.
        Frame& f = frames_[it->second];
        f.pin_count++;
        f.ref_bit = true;
        return f.data;
    }

    // Cache miss: Identify an eviction candidate before loading the new page from disk.
    int idx = evict_frame();
    Frame& f = frames_[idx];

    disk_->read_page(pid.page_num, f.data);

    // Re-initialize the frame metadata for the newly loaded page.
    f.page_id   = pid;
    f.pin_count = 1;
    f.dirty     = false;
    f.valid     = true;
    f.ref_bit   = true;

    page_table_[pid] = idx;
    return f.data;
}

/**
 * @brief Releases a pin on a page, allowing it to be evicted if pin count hits zero.
 * @param pid The ID of the page to unpin.
 * @param is_dirty True if the page was modified while pinned.
 */
void BufferPool::unpin_page(PageId pid, bool is_dirty) {
    auto it = page_table_.find(pid);
    if (it == page_table_.end()) return;

    Frame& f = frames_[it->second];
    // Sanity check: Ensure pin/unpin symmetry is maintained by the caller.
    assert(f.pin_count > 0 && "BufferPool: Attempted to unpin a page with pin_count 0");

    f.pin_count--;
    if (is_dirty) f.dirty = true;
}

/**
 * @brief Forces a specific page to be written back to the disk.
 * @param pid The ID of the page to flush.
 */
void BufferPool::flush_page(PageId pid) {
    auto it = page_table_.find(pid);
    if (it != page_table_.end()) {
        Frame& f = frames_[it->second];
        // Only perform the expensive disk write if the in-memory content has diverged.
        if (f.dirty) {
            disk_->write_page(f.page_id.page_num, f.data);
            f.dirty = false;
        }
    }
}

/**
 * @brief Implements the Clock (Second Chance) algorithm to find an eviction candidate.
 * @return Index of the reclaimed frame.
 */
int BufferPool::evict_frame() {
    size_t n = frames_.size();

    // The two-pass sweep clears ref_bits in the first pass and guarantees eviction in the second.
    for (size_t i = 0; i < 2 * n; ++i) {
        Frame& f = frames_[clock_hand_];

        // Immediate win: reclaim an unallocated or previously invalidated frame.
        if (!f.valid) {
            int idx = clock_hand_;
            advance_clock();
            return idx;
        }

        // Pinned pages are actively being used and must stay in memory.
        if (f.pin_count > 0) {
            advance_clock();
            continue;
        }

        // Grant second chance: clear the ref_bit and move the clock hand.
        if (f.ref_bit) {
            f.ref_bit = false;
            advance_clock();
            continue;
        }

        // Victim found: perform a write-back if the page was modified (dirty).
        if (f.dirty) {
            disk_->write_page(f.page_id.page_num, f.data);
            f.dirty = false;
        }

        // Remove from the look-up table and return the frame index.
        page_table_.erase(f.page_id);
        f.valid = false;
        evictions_++;

        int idx = clock_hand_;
        advance_clock();
        return idx;
    }

    throw std::runtime_error("BufferPool: All frames are currently pinned.");
}

/**
 * @brief Persists all dirty pages in the pool to the disk manager.
 */
void BufferPool::flush_all() {
    for (auto& [pid, idx] : page_table_) {
        Frame& f = frames_[idx];
        if (f.dirty) {
            disk_->write_page(pid.page_num, f.data);
            f.dirty = false;
        }
    }
}

/**
 * @brief High-level helper to create and pin a new page in one call.
 * @return The PageId of the newly allocated page.
 */
PageId BufferPool::new_page() {
    int page_num = disk_->alloc_page();
    PageId pid{0, page_num};
    pin_page(pid);
    return pid;
}

/**
 * @brief Creates an RAII handle for a page to ensure automatic unpinning.
 * @param pid The ID of the page to fetch.
 * @param for_write Whether the page will be modified.
 * @return An RAII guard managing the page's pin state.
 */
PageGuard BufferPool::fetch(PageId pid, bool for_write) {
    return PageGuard(this, pid, for_write);
}

/**
 * @brief Guard constructor pins the page immediately and assumes lifecycle ownership.
 * @param pool Pointer to the managing buffer pool.
 * @param pid The ID of the page to pin.
 * @param for_write If true, the page will be marked dirty upon release.
 */
PageGuard::PageGuard(BufferPool* pool, PageId pid, bool for_write)
    : pool_(pool), pid_(pid), dirty_(for_write), moved_(false) {
    data_ = pool_->pin_page(pid_);
}

/**
 * @brief Guard destructor unpins the page unless ownership was safely moved.
 */
PageGuard::~PageGuard() {
    if (!moved_ && pool_) {
        pool_->unpin_page(pid_, dirty_);
    }
}

/**
 * @brief Move constructor transfers ownership of the pinned page to a new guard.
 * @param o The source guard to move from.
 */
PageGuard::PageGuard(PageGuard&& o) noexcept
    : pool_(o.pool_), pid_(o.pid_), data_(o.data_),
      dirty_(o.dirty_), moved_(false) {
    o.moved_ = true;
}

/**
 * @brief Move assignment operator unpins the current page before taking ownership of another.
 * @param o The source guard to move from.
 * @return Reference to the reassigned guard.
 */
PageGuard& PageGuard::operator=(PageGuard&& o) noexcept {
    if (this != &o) {
        // Enforce RAII safety: unpin the currently held page before accepting the new one.
        if (!moved_ && pool_) {
            pool_->unpin_page(pid_, dirty_);
        }
        pool_   = o.pool_;
        pid_    = o.pid_;
        data_   = o.data_;
        dirty_  = o.dirty_;
        moved_  = false;
        o.moved_ = true;
    }
    return *this;
}

} // namespace pageforge