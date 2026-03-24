#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>
#include <stdexcept>

namespace pageforge {

/**
 * @brief Standard physical page size for the storage engine.
 */
static constexpr size_t PAGE_SIZE = 4096;

/**
 * @brief Identifies a specific page on disk by its file and page number.
 */
struct PageId {
    int file_id  = -1; ///< Internal identifier for the database file.
    int page_num = -1; ///< Physical block index within the file.

    /**
     * @brief Checks if the PageId refers to a valid physical location.
     * @return True if both file and page identifiers are non-negative.
     */
    bool valid() const { return file_id >= 0 && page_num >= 0; }
    
    /**
     * @brief Equality operator for usage in hash-based containers.
     * @param o The other PageId to compare against.
     * @return True if both IDs match.
     */
    bool operator==(const PageId& o) const {
        return file_id == o.file_id && page_num == o.page_num;
    }
};

/**
 * @brief Custom hash functor to allow PageId to be used as a key in unordered_map.
 */
struct PageIdHash {
    size_t operator()(const PageId& p) const {
        return std::hash<int>()(p.file_id) ^ (std::hash<int>()(p.page_num) << 16);
    }
};

/**
 * @brief Represents a single slot in the buffer pool containing page data and metadata.
 */
struct Frame {
    PageId    page_id;               ///< The ID of the page currently occupying the frame.
    std::byte data[PAGE_SIZE] = {};  ///< Raw page data buffer.
    int       pin_count = 0;         ///< Number of active components referencing this page.
    bool      dirty     = false;     ///< True if data was modified and requires a disk flush.
    bool      ref_bit   = false;     ///< Second-chance bit utilized by the Clock algorithm.
    bool      valid     = false;     ///< True if the frame contains a successfully loaded page.
};

/**
 * @brief Low-level manager for reading and writing fixed-size blocks to a database file.
 */
class DiskManager {
public:
    /**
     * @brief Constructs a DiskManager and opens the database file.
     * @param db_path Path to the file on disk.
     */
    explicit DiskManager(const std::string& db_path);
    ~DiskManager();

    /**
     * @brief Extends the file by one page and fills it with zeros.
     * @return The new page's physical index.
     */
    int  alloc_page();
    
    /**
     * @brief Reads a 4 KiB block from a specific offset in the file.
     * @param page_num The index of the page to read.
     * @param dst Pointer to the memory buffer where data will be stored.
     */
    void read_page (int page_num, std::byte* dst);
    
    /**
     * @brief Writes a 4 KiB block to a specific offset in the file.
     * @param page_num The index of the page to write.
     * @param src Pointer to the data to be written.
     */
    void write_page(int page_num, const std::byte* src);
    
    /**
     * @brief Returns the current physical size of the database file.
     * @return Total number of allocated pages.
     */
    int  page_count() const { return page_count_; }

private:
    int fd_         = -1;
    int page_count_ = 0;
};

class BufferPool;

/**
 * @brief RAII scoped handle that automatically unpins a page when destroyed.
 */
class PageGuard {
public:
    /**
     * @brief Guard constructor pins the page immediately and assumes lifecycle ownership.
     * @param pool Pointer to the managing buffer pool.
     * @param pid The ID of the page to pin.
     * @param for_write If true, the page will be marked dirty upon release.
     */
    PageGuard(BufferPool* pool, PageId pid, bool for_write = false);

    /**
     * @brief Guard destructor unpins the page unless ownership was safely moved.
     */
    ~PageGuard();

    PageGuard(const PageGuard&)            = delete;
    PageGuard& operator=(const PageGuard&) = delete;
    
    /**
     * @brief Move constructor transfers ownership of the pinned page to a new guard.
     * @param o The source guard to move from.
     */
    PageGuard(PageGuard&& o) noexcept;

    /**
     * @brief Move assignment operator unpins the current page before taking ownership of another.
     * @param o The source guard to move from.
     * @return Reference to the reassigned guard.
     */
    PageGuard& operator=(PageGuard&& o) noexcept;

    std::byte* data() { return data_; }
    const std::byte* data() const { return data_; }

    /**
     * @brief Reinterprets the raw page data as a specific structured type.
     * @tparam T The struct/type to cast the data to.
     * @return Typed pointer to the memory.
     */
    template<typename T>
    T* as() { return reinterpret_cast<T*>(data_); }

private:
    BufferPool* pool_  = nullptr;
    PageId      pid_;
    std::byte* data_  = nullptr;
    bool        dirty_ = false;
    bool        moved_ = false;
};

/**
 * @brief Cache manager that keeps frequently accessed disk pages in memory.
 */
class BufferPool {
public:
    /**
     * @brief Initializes the BufferPool with a specific capacity.
     * @param num_frames Total frames available in memory.
     * @param disk Pointer to the DiskManager for I/O operations.
     */
    BufferPool(size_t num_frames, DiskManager* disk);
    ~BufferPool() { flush_all(); }

    /**
     * @brief Pins a page in the buffer pool, loading it if not already resident.
     * @param pid Unique ID of the requested page.
     * @return Raw pointer to the page data in memory.
     */
    std::byte* pin_page(PageId pid);
    
    /**
     * @brief Releases a pin on a page, allowing it to be evicted if pin count hits zero.
     * @param pid The ID of the page to unpin.
     * @param is_dirty True if the page was modified while pinned.
     */
    void unpin_page(PageId pid, bool is_dirty);
    
    /**
     * @brief Forces a specific page to be written back to the disk.
     * @param pid The ID of the page to flush.
     */
    void flush_page(PageId pid);
    
    /**
     * @brief Persists all dirty pages in the pool to the disk manager.
     */
    void flush_all();
    
    /**
     * @brief High-level helper to create and pin a new page in one call.
     * @return The PageId of the newly allocated page.
     */
    PageId new_page();
    
    /**
     * @brief Creates an RAII handle for a page to ensure automatic unpinning.
     * @param pid The ID of the page to fetch.
     * @param for_write Whether the page will be modified.
     * @return An RAII guard managing the page's pin state.
     */
    PageGuard fetch(PageId pid, bool for_write = false);

    size_t num_frames()     const { return frames_.size(); }
    size_t cached_pages()   const { return page_table_.size(); }
    size_t eviction_count() const { return evictions_; }

private:
    /**
     * @brief Implements the Clock (Second Chance) algorithm to find an eviction candidate.
     * @return Index of the reclaimed frame.
     */
    int  evict_frame();
    
    /**
     * @brief Cycles the clock hand to the next position in the circular frame buffer.
     */
    void advance_clock() { clock_hand_ = (clock_hand_ + 1) % frames_.size(); }

    std::vector<Frame>                          frames_;
    std::unordered_map<PageId, int, PageIdHash> page_table_;
    DiskManager* disk_;
    int                                         clock_hand_ = 0;
    size_t                                      evictions_  = 0;
};

} // namespace pageforge