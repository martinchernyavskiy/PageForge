# PageForge — Persistent Storage Engine

A high-performance C++ storage manager featuring a buffered B+ Tree index and a slotted-page heap file. Built for high-reliability persistence using manual memory management and RAII patterns.

## Architecture

### 1. Buffer Pool Manager
Maintains a fixed-size pool of in-memory page frames to minimize Disk I/O.
- **Eviction Policy:** Implements the **Clock Replacement Algorithm**, providing an $O(1)$ approximation of Least Recently Used (LRU).
- **Memory Safety:** Uses a custom `PageGuard` RAII wrapper to manage page pinning, ensuring no "pin leaks" even during exceptions.
- **Persistence:** Dirty pages are automatically flushed to disk before eviction or on pool destruction.

### 2. B+ Tree Index
A disk-resident B+ Tree optimized for both point lookups and range scans.
- **Fan-out:** Designed to maximize keys per 4 KiB page, resulting in a shallow tree depth ($O(\log_b n)$).
- **Leaf Linkage:** Leaf nodes maintain pointers to siblings for $O(1)$ traversal during horizontal range scans.
- **Persistence:** All structural changes (splits) are reflected in the underlying buffer pool.

### 3. Slotted-Page Heap File
Manages variable-length records with zero external fragmentation.
- **Layout:** Uses a slotted-page directory that grows forward while records grow backward from the page end.
- **Operations:** Supports Insert, Lookup, Update (in-place or relocate), and Delete.
- **Integration:** Directly utilizes the Buffer Pool for transparent caching.

## Performance Benchmarks
- **Search Complexity:** $O(\log_{\text{ORDER}} N)$
- **Insertion Complexity:** $O(\log_{\text{ORDER}} N)$ 
- **Space Efficiency:** Variable-length record packing via Slotted Pages.

## Quick Start
```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
./build/pageforge
```