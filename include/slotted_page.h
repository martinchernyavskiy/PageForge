#pragma once

#include <cstdint>
#include <cstddef>
#include <optional>

namespace pageforge {

/**
 * @brief Metadata for a single record.
 */
struct Slot {
    uint16_t offset; ///< Byte offset from the start of the page.
    uint16_t length; ///< Record length; 0 indicates a deleted slot.
};

/**
 * @brief Header at the beginning of a slotted page.
 */
struct SlottedPageHeader {
    uint16_t slot_count;     ///< Number of slots in the directory.
    uint16_t free_space_ptr; ///< Offset to the start of the free space.
};

/**
 * @brief Manages a 4 KiB page as a slotted page for variable-length records.
 *
 * The slot directory grows forward from the header; record data grows backward
 * from the end of the page to maximize space utilization.
 */
class SlottedPage {
public:
    /**
     * @brief Constructs a SlottedPage wrapper around a raw page buffer.
     * @param data Pointer to the 4 KiB memory block.
     */
    explicit SlottedPage(std::byte* data);

    /**
     * @brief Initializes the page as a fresh, empty slotted page.
     */
    void init();

    /**
     * @brief Inserts a record into the page.
     * @param record_data Pointer to the record data.
     * @param size Size of the record in bytes.
     * @return The slot index assigned to the record, or std::nullopt if insufficient space.
     */
    std::optional<uint16_t> insert_record(const std::byte* record_data, uint16_t size);

    /**
     * @brief Retrieves a pointer to the data of a record.
     * @param slot_id The slot index.
     * @return Pointer to the record data, or nullptr if the slot is invalid or empty.
     */
    std::byte* get_record(uint16_t slot_id);

    /**
     * @brief Marks a record as deleted by setting its length to zero.
     * @param slot_id The slot index to delete.
     */
    void delete_record(uint16_t slot_id);

    /**
     * @brief Updates the length of an existing slot.
     * @param slot_id The slot index to modify.
     * @param new_len The new length value.
     */
    void update_slot_length(uint16_t slot_id, uint16_t new_len);

    /**
     * @brief Retrieves the offset and length of a record.
     * @param slot_id The slot index.
     * @param offset Output parameter for the record's offset.
     * @param length Output parameter for the record's length.
     * @return True if the slot is valid and contains a record, false otherwise.
     */
    bool get_slot_info(uint16_t slot_id, uint16_t& offset, uint16_t& length) const;

    /**
     * @brief Returns the total number of slots in the directory.
     * @return The slot count.
     */
    uint16_t get_slot_count() const { return header_->slot_count; }

    /**
     * @brief Returns the number of contiguous bytes available for new records.
     * @return Available free space in bytes.
     */
    uint16_t free_space_remaining() const;

private:
    std::byte* data_;           ///< Raw page buffer.
    SlottedPageHeader* header_; ///< Pointer to the page header.
    Slot* slots_;               ///< Pointer to the slot directory.
};

} // namespace pageforge