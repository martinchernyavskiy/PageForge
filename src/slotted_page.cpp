#include "slotted_page.h"
#include "buffer_pool.h"
#include <cstring>

namespace pageforge {

/**
 * @brief Constructs a SlottedPage wrapper around a raw page buffer.
 * @param data Pointer to the 4 KiB memory block.
 */
SlottedPage::SlottedPage(std::byte* data) : data_(data) {
    // Map the metadata structures directly onto the raw byte buffer.
    header_ = reinterpret_cast<SlottedPageHeader*>(data_);
    slots_  = reinterpret_cast<Slot*>(data_ + sizeof(SlottedPageHeader));
}

/**
 * @brief Initializes the page as a fresh, empty slotted page.
 */
void SlottedPage::init() {
    header_->slot_count = 0;
    // Data area grows backward from the absolute end of the 4 KiB page.
    header_->free_space_ptr = static_cast<uint16_t>(PAGE_SIZE);
}

/**
 * @brief Returns the number of contiguous bytes available for new records.
 * @return Available free space in bytes.
 */
uint16_t SlottedPage::free_space_remaining() const {
    // Calculate the gap between the end of the directory and the start of records.
    uint16_t directory_end = sizeof(SlottedPageHeader) + (header_->slot_count * sizeof(Slot));
    if (header_->free_space_ptr < directory_end) return 0;
    return header_->free_space_ptr - directory_end;
}

/**
 * @brief Inserts a record into the page.
 * @param record_data Pointer to the record data.
 * @param size Size of the record in bytes.
 * @return The slot index assigned to the record, or std::nullopt if insufficient space.
 */
std::optional<uint16_t> SlottedPage::insert_record(const std::byte* record_data, uint16_t size) {
    // Ensure room for both the payload and the new directory entry.
    if (size + sizeof(Slot) > free_space_remaining()) {
        return std::nullopt;
    }

    // Allocate space by moving the pointer toward the beginning of the page.
    header_->free_space_ptr -= size;
    std::memcpy(data_ + header_->free_space_ptr, record_data, size);

    // Append the new slot to the forward-growing directory.
    uint16_t slot_id = header_->slot_count++;
    slots_[slot_id].offset = header_->free_space_ptr;
    slots_[slot_id].length = size;

    return slot_id;
}

/**
 * @brief Retrieves a pointer to the data of a record.
 * @param slot_id The slot index.
 * @return Pointer to the record data, or nullptr if the slot is invalid or empty.
 */
std::byte* SlottedPage::get_record(uint16_t slot_id) {
    // A length of 0 identifies a tombstoned (deleted) record.
    if (slot_id >= header_->slot_count || slots_[slot_id].length == 0) {
        return nullptr;
    }
    return data_ + slots_[slot_id].offset;
}

/**
 * @brief Marks a record as deleted by setting its length to zero.
 * @param slot_id The slot index to delete.
 */
void SlottedPage::delete_record(uint16_t slot_id) {
    if (slot_id < header_->slot_count) {
        // Simple tombstone: zeroing the length effectively removes the record from logical view.
        slots_[slot_id].length = 0;
    }
}

/**
 * @brief Updates the length of an existing slot.
 * @param slot_id The slot index to modify.
 * @param new_len The new length value.
 */
void SlottedPage::update_slot_length(uint16_t slot_id, uint16_t new_len) {
    if (slot_id < header_->slot_count) {
        // Explicitly update the metadata length; crucial for handling in-place record shrinkages.
        slots_[slot_id].length = new_len;
    }
}

/**
 * @brief Retrieves the offset and length of a record.
 * @param slot_id The slot index.
 * @param offset Output parameter for the record's offset.
 * @param length Output parameter for the record's length.
 * @return True if the slot is valid and contains a record, false otherwise.
 */
bool SlottedPage::get_slot_info(uint16_t slot_id, uint16_t& offset, uint16_t& length) const {
    if (slot_id >= header_->slot_count || slots_[slot_id].length == 0) {
        return false;
    }
    offset = slots_[slot_id].offset;
    length = slots_[slot_id].length;
    return true;
}

} // namespace pageforge