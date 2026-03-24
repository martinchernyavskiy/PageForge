#include "btree.h"
#include <cstring>
#include <cassert>

namespace pageforge {

/**
 * @brief Interprets the beginning of a page as a NodeHeader.
 * @param page Raw pointer to the page data.
 * @return Typed pointer to the header.
 */
NodeHeader* BPlusTree::header(std::byte* page) {
    return reinterpret_cast<NodeHeader*>(page);
}

/**
 * @brief Locates the start of the key array within a page.
 * @param page Raw pointer to the page data.
 * @return Typed pointer to the first key.
 */
Key* BPlusTree::keys(std::byte* page) {
    // Keys follow the fixed-size header at the start of the page.
    return reinterpret_cast<Key*>(page + sizeof(NodeHeader));
}

/**
 * @brief Locates the start of the value array within a leaf page.
 * @param page Raw pointer to the page data.
 * @return Typed pointer to the first value.
 */
Value* BPlusTree::values(std::byte* page) {
    // Leaf nodes store values immediately following the full capacity of the key array.
    return reinterpret_cast<Value*>(page + sizeof(NodeHeader) + ORDER * sizeof(Key));
}

/**
 * @brief Locates the start of the child page ID array within an internal node.
 * @param page Raw pointer to the page data.
 * @return Typed pointer to the first child page ID.
 */
int* BPlusTree::children(std::byte* page) {
    // Internal nodes alias the leaf value space to store child PageIds.
    return reinterpret_cast<int*>(values(page));
}

/**
 * @brief Allocates a new page and initializes it as a B+ Tree node.
 * @param is_leaf True if the node should be formatted as a leaf.
 * @return The physical page index of the newly initialized node.
 */
int BPlusTree::alloc_node(bool is_leaf) {
    PageId pid = pool_->new_page();
    {
        auto guard = pool_->fetch(pid, true);
        // Zero-initialize to ensure no stale data interferes with metadata.
        std::memset(guard.data(), 0, PAGE_SIZE);

        NodeHeader* h = header(guard.data());
        h->is_leaf   = is_leaf;
        h->num_keys  = 0;
        h->next_leaf = INVALID_PAGE;
        h->parent    = INVALID_PAGE;
    }
    // Release the creation pin to allow the page to enter the eviction cycle.
    pool_->unpin_page(pid, true);
    return pid.page_num;
}

/**
 * @brief Constructs a new B+ Tree with a single empty leaf node as the root.
 * @param pool Pointer to the buffer pool.
 */
BPlusTree::BPlusTree(BufferPool* pool) : pool_(pool) {
    root_page_ = alloc_node(true);
}

/**
 * @brief Searches for a key and returns its associated value if it exists.
 * @param key The key to look for.
 * @return std::optional containing the value, or std::nullopt if not found.
 */
std::optional<Value> BPlusTree::search(Key key) {
    int leaf_num = find_leaf(key);
    auto guard = pool_->fetch({0, leaf_num});

    Key* k_arr = keys(guard.data());
    int n = header(guard.data())->num_keys;

    // Linear search is efficient for small-order nodes due to cache locality.
    for (int i = 0; i < n; ++i) {
        if (k_arr[i] == key) return values(guard.data())[i];
    }
    return std::nullopt;
}

/**
 * @brief Inserts a key-value pair into the tree.
 *
 * Performs an iterative top-down descent, preemptively splitting full nodes
 * to ensure a single-pass insertion.
 * @param key The unique key to be indexed.
 * @param value The value associated with the key.
 */
void BPlusTree::insert(Key key, Value value) {
    // Preemptive root split: increases tree height and ensures the descent starts with room.
    {
        auto root_guard = pool_->fetch({0, root_page_}, true);
        if (header(root_guard.data())->num_keys == ORDER) {
            int new_root_num = alloc_node(false);
            int old_root_num = root_page_;
            root_page_ = new_root_num;

            auto new_root_guard = pool_->fetch({0, new_root_num}, true);
            children(new_root_guard.data())[0] = old_root_num;
            header(root_guard.data())->parent = new_root_num;

            split_child(new_root_num, 0, old_root_num);
        }
    }

    // Iterative descent: navigates to the leaf while splitting full children along the path.
    int curr_num = root_page_;
    while (true) {
        auto curr_guard = pool_->fetch({0, curr_num}, true);
        NodeHeader* h = header(curr_guard.data());

        if (h->is_leaf) {
            // Found target leaf; shift elements to maintain sorted order.
            Key* k_arr = keys(curr_guard.data());
            Value* v_arr = values(curr_guard.data());
            int i = h->num_keys - 1;

            while (i >= 0 && key < k_arr[i]) {
                k_arr[i + 1] = k_arr[i];
                v_arr[i + 1] = v_arr[i];
                i--;
            }
            k_arr[i + 1] = key;
            v_arr[i + 1] = value;
            h->num_keys++;
            return;
        }

        // Internal node navigation.
        Key* k_arr = keys(curr_guard.data());
        int* c_arr = children(curr_guard.data());
        int i = 0;
        while (i < h->num_keys && key >= k_arr[i]) i++;

        int next_num = c_arr[i];
        auto next_guard = pool_->fetch({0, next_num}, true);

        // Preemptive split ensures the parent node always has space for a promoted key.
        if (header(next_guard.data())->num_keys == ORDER) {
            split_child(curr_num, i, next_num);
            if (key >= k_arr[i]) i++; 
            curr_num = c_arr[i];
        } else {
            curr_num = next_num;
        }
    }
}

/**
 * @brief Traverses the leaf nodes to find all keys within the specified range.
 * @param lo Lower bound (inclusive).
 * @param hi Upper bound (inclusive).
 * @param visitor Function called for each key-value pair found.
 */
void BPlusTree::range_scan(Key lo, Key hi, std::function<void(Key, Value)> visitor) {
    int curr_page = find_leaf(lo);
    while (curr_page != INVALID_PAGE) {
        auto guard = pool_->fetch({0, curr_page});
        NodeHeader* h = header(guard.data());
        Key* k_arr = keys(guard.data());
        Value* v_arr = values(guard.data());

        // Process keys in current leaf and terminate early if upper bound is exceeded.
        for (int i = 0; i < h->num_keys; ++i) {
            if (k_arr[i] >= lo && k_arr[i] <= hi) visitor(k_arr[i], v_arr[i]);
            if (k_arr[i] > hi) return;
        }
        // Jump to the right sibling to continue the horizontal scan.
        curr_page = h->next_leaf;
    }
}

/**
 * @brief Navigates from the root down to the leaf level for a given key.
 * @param key The target key used for navigation.
 * @return The page number of the target leaf node.
 */
int BPlusTree::find_leaf(Key key) {
    int curr = root_page_;
    while (true) {
        auto guard = pool_->fetch({0, curr});
        NodeHeader* h = header(guard.data());
        if (h->is_leaf) return curr;

        // Use separator keys to identify the correct child pointer.
        Key* k_arr = keys(guard.data());
        int* c_arr = children(guard.data());
        int i = 0;
        while (i < h->num_keys && key >= k_arr[i]) i++;
        curr = c_arr[i];
    }
}

/**
 * @brief Splits a full child node and moves the median key into the parent.
 * @param parent_num Page ID of the parent node.
 * @param child_idx Index of the child within the parent's pointer array.
 * @param child_num Page ID of the node to split.
 */
void BPlusTree::split_child(int parent_num, int child_idx, int child_num) {
    auto child_guard = pool_->fetch({0, child_num}, true);
    bool is_leaf = header(child_guard.data())->is_leaf;
    int new_node = alloc_node(is_leaf);

    auto new_guard = pool_->fetch({0, new_node}, true);
    NodeHeader* ch = header(child_guard.data());
    NodeHeader* nh = header(new_guard.data());
    Key* c_keys = keys(child_guard.data());
    Key* n_keys = keys(new_guard.data());

    int mid = ORDER / 2;

    if (is_leaf) {
        // Leaf splits redistribute keys/values; the median is promoted but kept in the leaf tier.
        int copy_count = ch->num_keys - mid;
        std::memcpy(n_keys, c_keys + mid, copy_count * sizeof(Key));
        std::memcpy(values(new_guard.data()), values(child_guard.data()) + mid,
                    copy_count * sizeof(Value));

        nh->num_keys = copy_count;
        ch->num_keys = mid;

        // Maintain the linked-list structure for range scans.
        nh->next_leaf = ch->next_leaf;
        ch->next_leaf = new_node;
        nh->parent = parent_num;
    } else {
        // Internal splits promote the median separator and redistribute child pointers.
        int copy_start = mid + 1;
        int copy_count = ch->num_keys - copy_start;
        std::memcpy(n_keys, c_keys + copy_start, copy_count * sizeof(Key));
        std::memcpy(children(new_guard.data()), children(child_guard.data()) + copy_start,
                    (copy_count + 1) * sizeof(int));

        nh->num_keys = copy_count;
        ch->num_keys = mid;
        nh->parent = parent_num;

        // Correct child nodes' parent pointers to reflect their new sibling parent.
        int* n_clds = children(new_guard.data());
        for (int i = 0; i <= copy_count; ++i) {
            auto move_guard = pool_->fetch({0, n_clds[i]}, true);
            header(move_guard.data())->parent = new_node;
        }
    }

    // Update parent node with the promoted key and the new child pointer.
    auto parent_guard = pool_->fetch({0, parent_num}, true);
    NodeHeader* ph = header(parent_guard.data());
    Key* p_keys = keys(parent_guard.data());
    int* p_clds = children(parent_guard.data());

    for (int i = ph->num_keys; i > child_idx; --i) {
        p_keys[i] = p_keys[i - 1];
        p_clds[i + 1] = p_clds[i];
    }
    p_keys[child_idx] = c_keys[mid];
    p_clds[child_idx + 1] = new_node;
    ph->num_keys++;
}

} // namespace pageforge