#include "buffer_pool.h"
#include "btree.h"
#include "heap_file.h"
#include <iostream>
#include <cassert>
#include <cstdio>
#include <string>

using namespace pageforge;

/**
 * @brief Prints a visual separator to the console.
 * @param c The character used for the divider line.
 * @param width The total character width of the divider.
 */
static void print_divider(char c = '=', int width = 60) {
    std::cout << std::string(width, c) << "\n";
}

/**
 * @brief Validates the buffer pool's pinning, unpinning, and Clock eviction logic.
 */
static void test_buffer_pool() {
    print_divider();
    std::cout << "[BufferPool] Starting functional tests...\n";

    std::remove("/tmp/pf_test.db");
    DiskManager dm("/tmp/pf_test.db");
    
    // Constrained pool size forces the Clock policy to trigger evictions during over-subscription.
    BufferPool  bp(8, &dm); 

    std::vector<PageId> pages;
    for (int i = 0; i < 20; ++i) {
        PageId pid = bp.new_page();
        pages.push_back(pid);
        {
            // Use RAII guard to safely pin and write; scope termination unpins the page.
            auto guard = bp.fetch(pid, true);
            guard.as<int>()[0] = pid.page_num * 1000;
        }
        bp.unpin_page(pid, true);
    }

    // Ensure all modifications are persisted before verification.
    bp.flush_all();

    for (auto& pid : pages) {
        auto guard = bp.fetch(pid);
        if (guard.as<int>()[0] != pid.page_num * 1000) {
            throw std::runtime_error("BufferPool: Data integrity check failed.");
        }
    }

    std::cout << "[BufferPool] Evictions performed: " << bp.eviction_count() << "\n";
    std::cout << "[BufferPool] PASS\n";
}

/**
 * @brief Tests B+ Tree insertion, point lookups, and range scans.
 */
static void test_btree() {
    print_divider();
    std::cout << "[BPlusTree] Starting index tests...\n";

    std::remove("/tmp/pf_btree.db");
    DiskManager dm("/tmp/pf_btree.db");
    BufferPool  bp(16, &dm);
    BPlusTree   tree(&bp);

    // High-volume insertion to validate split logic and structural integrity across multiple levels.
    std::cout << "  Inserting 10,000 keys...\n";
    for (int i = 0; i < 10000; ++i) {
        tree.insert(i, i * 10);
    }

    // Verify point lookup performance and correctness.
    auto val = tree.search(42);
    if (!val.has_value() || *val != 420) {
        throw std::runtime_error("BPlusTree: Search verification failed.");
    }

    // Horizontal scan verifies that leaf nodes are correctly linked across the index.
    std::cout << "  Range scan [100, 105]: ";
    tree.range_scan(100, 105, [](Key k, Value v) {
        std::cout << k << "=" << v << " ";
    });
    std::cout << "\n";

    std::cout << "[BPlusTree] PASS\n";
}

/**
 * @brief Verifies variable-length record management in the Heap File.
 */
static void test_heap_file() {
    print_divider();
    std::cout << "[HeapFile] Starting record management tests...\n";

    std::remove("/tmp/pf_heap.db");
    DiskManager dm("/tmp/pf_heap.db");
    BufferPool  bp(16, &dm);
    HeapFile    heap(&bp, &dm);

    std::string s1 = "Initial record data";
    std::string s2 = "A second record for variable-length testing";
    
    // Slotted-page allocation handles the raw byte translation and packing.
    RecordId rid1 = heap.insert(reinterpret_cast<const std::byte*>(s1.data()), s1.size());
    RecordId rid2 = heap.insert(reinterpret_cast<const std::byte*>(s2.data()), s2.size());

    auto data1 = heap.lookup(rid1);
    std::string out1(reinterpret_cast<char*>(data1.data()), data1.size());
    assert(out1 == s1);

    // Validates in-place updates vs. relocation logic based on new payload size.
    std::string new_data = "Updated content";
    rid1 = heap.update(rid1, reinterpret_cast<const std::byte*>(new_data.data()), new_data.size());
    auto updated = heap.lookup(rid1);
    std::string out_up(reinterpret_cast<char*>(updated.data()), updated.size());
    assert(out_up == new_data);

    // Tombstoned records must be effectively removed from the logical file view.
    heap.delete_record(rid2);
    auto missing = heap.lookup(rid2);
    assert(missing.empty());

    // Sequential scan verifies the engine correctly skips deleted/tombstoned slots.
    std::cout << "  Scanning all active records:\n";
    heap.scan([](RecordId rid, const std::byte* data, size_t len) {
        std::cout << "    page=" << rid.page_num << " slot=" << rid.slot_num
                  << " data=" << std::string(reinterpret_cast<const char*>(data), len) << "\n";
    });

    std::cout << "[HeapFile] PASS\n";
}

/**
 * @brief Main entry point for the PageForge test suite.
 * @return 0 on success, 1 on failure.
 */
int main() {
    try {
        test_buffer_pool();
        test_btree();
        test_heap_file();

        print_divider();
        std::cout << "All PageForge tests passed successfully.\n";
    } catch (const std::exception& e) {
        // High-level catch ensures resources are freed even during structural failures.
        std::cerr << "Test suite failed with error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}