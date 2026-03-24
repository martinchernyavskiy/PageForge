#pragma once

#include "buffer_pool.h"
#include <optional>
#include <functional>

namespace pageforge {

using Key   = int64_t;
using Value = int64_t;

/**
 * @brief Sentinel value indicating a null or non-existent page pointer.
 */
static constexpr int INVALID_PAGE = -1;

/**
 * @brief Metadata header stored at the beginning of every B+ Tree node page.
 */
struct NodeHeader {
    bool is_leaf;    ///< True if this is a leaf node, false for internal nodes.
    int  num_keys;   ///< Number of keys currently stored in the node.
    int  next_leaf;  ///< Page number of the right sibling; used for range scans.
    int  parent;     ///< Page number of the parent node to facilitate splits.
};

/**
 * @brief Maximum number of keys per node, calculated to fit within one 4 KiB page.
 */
static constexpr int ORDER = (PAGE_SIZE - sizeof(NodeHeader)) 
                             / (sizeof(Key) + sizeof(Value)) - 1;

/**
 * @brief A disk-backed B+ Tree index providing logarithmic search and insertion.
 */
class BPlusTree {
public:
    /**
     * @brief Constructs a new B+ Tree with a single empty leaf node as the root.
     * @param pool Pointer to the buffer pool.
     */
    explicit BPlusTree(BufferPool* pool);

    /**
     * @brief Inserts a key-value pair into the tree.
     *
     * Performs an iterative top-down descent, preemptively splitting full nodes
     * to ensure a single-pass insertion.
     * @param key The unique key to be indexed.
     * @param value The value associated with the key.
     */
    void insert(Key key, Value value);

    /**
     * @brief Searches for a key and returns its associated value if it exists.
     * @param key The key to look for.
     * @return std::optional containing the value, or std::nullopt if not found.
     */
    std::optional<Value> search(Key key);

    /**
     * @brief Traverses the leaf nodes to find all keys within the specified range.
     * @param lo Lower bound (inclusive).
     * @param hi Upper bound (inclusive).
     * @param visitor Function called for each key-value pair found.
     */
    void range_scan(Key lo, Key hi, std::function<void(Key, Value)> visitor);

    /**
     * @brief Returns the page number of the current root of the tree.
     * @return The physical page index of the root node.
     */
    int root_page() const { return root_page_; }

private:
    /**
     * @brief Splits a full child node and moves the median key into the parent.
     * @param parent_num Page ID of the parent node.
     * @param child_idx Index of the child within the parent's pointer array.
     * @param child_num Page ID of the node to split.
     */
    void split_child(int parent_num, int child_idx, int child_num);

    /**
     * @brief Navigates from the root down to the leaf level for a given key.
     * @param key The target key used for navigation.
     * @return The page number of the target leaf node.
     */
    int find_leaf(Key key);

    /**
     * @brief Interprets the beginning of a page as a NodeHeader.
     * @param page Raw pointer to the page data.
     * @return Typed pointer to the header.
     */
    NodeHeader* header(std::byte* page);

    /**
     * @brief Locates the start of the key array within a page.
     * @param page Raw pointer to the page data.
     * @return Typed pointer to the first key.
     */
    Key* keys(std::byte* page);

    /**
     * @brief Locates the start of the value array within a leaf page.
     * @param page Raw pointer to the page data.
     * @return Typed pointer to the first value.
     */
    Value* values(std::byte* page);

    /**
     * @brief Locates the start of the child page ID array within an internal node.
     * @param page Raw pointer to the page data.
     * @return Typed pointer to the first child page ID.
     */
    int* children(std::byte* page);

    /**
     * @brief Allocates a new page and initializes it as a B+ Tree node.
     * @param is_leaf True if the node should be formatted as a leaf.
     * @return The physical page index of the newly initialized node.
     */
    int alloc_node(bool is_leaf);

    BufferPool* pool_;
    int         root_page_;
};

} // namespace pageforge