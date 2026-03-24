#pragma once

#include "buffer_pool.h"
#include "slotted_page.h"
#include <vector>
#include <cstdint>
#include <optional>
#include <functional>

namespace pageforge {

/**
 * @brief Unique identifier for a record within a HeapFile.
 *
 * Combines physical page number with a slot index inside the page.
 */
struct RecordId {
    int page_num; ///< Physical page index in the database file.
    int slot_num; ///< Logical slot index within the page directory.

    /**
     * @brief Equality operator for comparing record locations.
     * @param o The other RecordId to compare.
     * @return True if both page and slot indices match.
     */
    bool operator==(const RecordId& o) const {
        return page_num == o.page_num && slot_num == o.slot_num;
    }
};

/**
 * @brief A manager for variable-length records stored across multiple pages.
 *
 * Provides insert, lookup, update, delete, and scan operations. Records are
 * packed using the slotted-page layout provided by SlottedPage.
 */
class HeapFile {
public:
    /**
     * @brief Constructs a HeapFile and identifies pages with available space.
     * @param pool Pointer to the buffer pool.
     * @param disk Pointer to the disk manager.
     */
    HeapFile(BufferPool* pool, DiskManager* disk);
    ~HeapFile() = default;

    /**
     * @brief Inserts a new record into the file.
     * @param data Pointer to the record data.
     * @param len Length of the data in bytes.
     * @return The RecordId of the newly stored record.
     */
    RecordId insert(const std::byte* data, size_t len);

    /**
     * @brief Retrieves a copy of a record's data.
     * @param rid The RecordId of the record.
     * @return A vector containing the data, or empty if the record does not exist.
     */
    std::vector<std::byte> lookup(RecordId rid);

    /**
     * @brief Updates a record, possibly relocating it if the size increases.
     * @param rid The original RecordId.
     * @param data New data content.
     * @param len New data length.
     * @return The RecordId of the updated record (may differ from the input).
     */
    RecordId update(RecordId rid, const std::byte* data, size_t len);

    /**
     * @brief Deletes a record, marking its slot as free (tombstone).
     * @param rid The RecordId of the record to delete.
     */
    void delete_record(RecordId rid);

    /**
     * @brief Scans all pages and calls a visitor for every active record.
     * @param visitor Callback invoked for each record.
     */
    void scan(std::function<void(RecordId, const std::byte*, size_t)> visitor);

    /**
     * @brief Returns the total number of pages currently managed by the heap file.
     * @return Physical page count.
     */
    size_t page_count() const { return page_count_; }

private:
    /**
     * @brief Maximum number of slots a page can accommodate.
     * @return Slot capacity per page.
     */
    static constexpr size_t max_slots() {
        return (PAGE_SIZE - sizeof(SlottedPageHeader)) / sizeof(Slot);
    }

    /**
     * @brief Searches for a page that can hold a record of given length.
     * @param record_len Required space in bytes.
     * @return The page number and its free space offset, if found.
     */
    std::optional<std::pair<int, uint16_t>> find_free_page(size_t record_len);

    /**
     * @brief Allocates a new physical page and updates internal tracking.
     * @return The new page number.
     */
    int allocate_page();

    BufferPool* pool_;                     ///< Buffer pool for page caching.
    DiskManager* disk_;                    ///< Disk manager for I/O.
    size_t page_count_;                    ///< Total number of pages in the file.
    std::vector<int> free_page_candidates_; ///< Pages with remaining free space.
};

} // namespace pageforge