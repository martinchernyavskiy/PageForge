#include "heap_file.h"
#include <cstring>
#include <cassert>

namespace pageforge {

/**
 * @brief Constructs a HeapFile and identifies pages with available space.
 * @param pool Pointer to the buffer pool.
 * @param disk Pointer to the disk manager.
 */
HeapFile::HeapFile(BufferPool* pool, DiskManager* disk)
    : pool_(pool), disk_(disk) {
    page_count_ = static_cast<size_t>(disk_->page_count());

    // Hydrate the candidate list by scanning existing page headers for slot capacity.
    for (size_t i = 0; i < page_count_; ++i) {
        PageId pid{0, static_cast<int>(i)};
        auto guard = pool_->fetch(pid);
        SlottedPage sp(guard.data());

        if (sp.get_slot_count() < max_slots()) {
            free_page_candidates_.push_back(static_cast<int>(i));
        }
    }
}

/**
 * @brief Inserts a new record into the file.
 * @param data Pointer to the record data.
 * @param len Length of the data in bytes.
 * @return The RecordId of the newly stored record.
 */
RecordId HeapFile::insert(const std::byte* data, size_t len) {
    auto page_info = find_free_page(len);
    int page_num;

    if (page_info) {
        page_num = page_info->first;
    } else {
        // Fallback: Extend the physical file if no existing page satisfies the contiguous space requirement.
        page_num = allocate_page();
        auto guard = pool_->fetch({0, page_num}, true);
        SlottedPage sp(guard.data());
        sp.init();
        free_page_candidates_.push_back(page_num);
    }

    auto guard = pool_->fetch({0, page_num}, true);
    SlottedPage sp(guard.data());

    auto slot_id = sp.insert_record(data, static_cast<uint16_t>(len));
    // The earlier free‑space check guarantees this will succeed.
    assert(slot_id.has_value() && "HeapFile: insert_record failed despite free space");

    return {page_num, static_cast<int>(*slot_id)};
}

/**
 * @brief Retrieves a copy of a record's data.
 * @param rid The RecordId of the record.
 * @return A vector containing the data, or empty if the record does not exist.
 */
std::vector<std::byte> HeapFile::lookup(RecordId rid) {
    if (rid.page_num < 0 || static_cast<size_t>(rid.page_num) >= page_count_) {
        return {};
    }

    auto guard = pool_->fetch({0, rid.page_num});
    SlottedPage sp(guard.data());

    uint16_t offset, length;
    // SlottedPage encapsulates the logic for validating the slot and identifying tombstones.
    if (!sp.get_slot_info(static_cast<uint16_t>(rid.slot_num), offset, length)) {
        return {};
    }

    std::vector<std::byte> result(length);
    std::memcpy(result.data(), guard.data() + offset, length);
    return result;
}

/**
 * @brief Updates a record, possibly relocating it if the size increases.
 * @param rid The original RecordId.
 * @param data New data content.
 * @param len New data length.
 * @return The RecordId of the updated record (may differ from the input).
 */
RecordId HeapFile::update(RecordId rid, const std::byte* data, size_t len) {
    auto guard = pool_->fetch({0, rid.page_num}, true);
    SlottedPage sp(guard.data());

    uint16_t offset, old_len;
    if (!sp.get_slot_info(static_cast<uint16_t>(rid.slot_num), offset, old_len)) {
        // Slot is invalid or already deleted; nothing to update.
        return rid;
    }

    if (len <= old_len) {
        // Optimization: Overwrite data in-place if the existing allocation is sufficient.
        std::memcpy(guard.data() + offset, data, len);
        // Explicitly update metadata to reflect the new size and prevent reading stale data.
        sp.update_slot_length(static_cast<uint16_t>(rid.slot_num), static_cast<uint16_t>(len));
        return rid;
    } else {
        // Relocation: Mark the current slot as a tombstone and find a new home for the expanded data.
        sp.delete_record(static_cast<uint16_t>(rid.slot_num));
        return insert(data, len);
    }
}

/**
 * @brief Deletes a record, marking its slot as free (tombstone).
 * @param rid The RecordId of the record to delete.
 */
void HeapFile::delete_record(RecordId rid) {
    auto guard = pool_->fetch({0, rid.page_num}, true);
    SlottedPage sp(guard.data());
    // Metadata-only deletion; avoids expensive data shifts until a vacuum/compaction is triggered.
    sp.delete_record(static_cast<uint16_t>(rid.slot_num));
}

/**
 * @brief Scans all pages and calls a visitor for every active record.
 * @param visitor Callback invoked for each record.
 */
void HeapFile::scan(std::function<void(RecordId, const std::byte*, size_t)> visitor) {
    for (size_t i = 0; i < page_count_; ++i) {
        auto guard = pool_->fetch({0, static_cast<int>(i)});
        SlottedPage sp(guard.data());

        for (uint16_t s = 0; s < sp.get_slot_count(); ++s) {
            uint16_t offset, length;
            // Filter out tombstones and invalid slots during the sequential scan.
            if (sp.get_slot_info(s, offset, length)) {
                visitor({static_cast<int>(i), static_cast<int>(s)},
                        guard.data() + offset, length);
            }
        }
    }
}

/**
 * @brief Searches for a page that can hold a record of given length.
 * @param record_len Required space in bytes.
 * @return The page number and its free space offset, if found.
 */
std::optional<std::pair<int, uint16_t>> HeapFile::find_free_page(size_t record_len) {
    for (int page_num : free_page_candidates_) {
        auto guard = pool_->fetch({0, page_num});
        SlottedPage sp(guard.data());

        // Account for both the record payload and the directory entry overhead.
        if (sp.free_space_remaining() >= record_len + sizeof(Slot)) {
            return std::make_pair(page_num, sp.free_space_remaining());
        }
    }
    return std::nullopt;
}

/**
 * @brief Allocates a new physical page and updates internal tracking.
 * @return The new page number.
 */
int HeapFile::allocate_page() {
    int page_num = disk_->alloc_page();
    page_count_++;
    return page_num;
}

} // namespace pageforge