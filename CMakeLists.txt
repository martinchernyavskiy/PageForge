cmake_minimum_required(VERSION 3.16)
project(PageForge CXX)

# Standardize on the C++20 standard for modern language features.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Release)

# Enable optimization and standard warnings.
add_compile_options(-O2 -Wall -Wextra)

# Ensure header resolution for the project.
include_directories(include)

# Build the PageForge engine executable.
add_executable(pageforge
    src/buffer_pool.cpp
    src/btree.cpp
    src/slotted_page.cpp
    src/heap_file.cpp
    src/main.cpp
)