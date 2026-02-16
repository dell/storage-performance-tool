# SPT S3-RDMA Native Build
#
# Direct libibverbs implementation - no CUDA, no cuObject dependencies.
# Target: NVIDIA/Mellanox ConnectX-4+ with mlx5 driver
#
# Build:
#   mkdir build && cd build
#   cmake ..
#   make
#
# Requirements:
#   - rdma-core-devel (provides libibverbs, libmlx5, librdmacm headers)
#   - JDK 21+ (provides JNI headers)

cmake_minimum_required(VERSION 3.16)
project(spt_rdma C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find JNI
find_package(JNI REQUIRED)

# Check for libibverbs header
find_path(VERBS_INCLUDE_DIR
    NAMES infiniband/verbs.h
    PATHS /usr/include /usr/local/include
)

if(NOT VERBS_INCLUDE_DIR)
    message(FATAL_ERROR "Cannot find infiniband/verbs.h - install rdma-core-devel")
endif()

# Check for mlx5dv header
find_path(MLX5DV_INCLUDE_DIR
    NAMES infiniband/mlx5dv.h
    PATHS /usr/include /usr/local/include
)

if(NOT MLX5DV_INCLUDE_DIR)
    message(FATAL_ERROR "Cannot find infiniband/mlx5dv.h - install rdma-core-devel")
endif()

# Check for rdma_cma header
find_path(RDMACM_INCLUDE_DIR
    NAMES rdma/rdma_cma.h
    PATHS /usr/include /usr/local/include
)

if(NOT RDMACM_INCLUDE_DIR)
    message(FATAL_ERROR "Cannot find rdma/rdma_cma.h - install rdma-core-devel")
endif()

# Find libraries
find_library(VERBS_LIB
    NAMES ibverbs
    PATHS /usr/lib64 /usr/lib /usr/local/lib64 /usr/local/lib
)

find_library(MLX5_LIB
    NAMES mlx5
    PATHS /usr/lib64 /usr/lib /usr/local/lib64 /usr/local/lib
)

find_library(RDMACM_LIB
    NAMES rdmacm
    PATHS /usr/lib64 /usr/lib /usr/local/lib64 /usr/local/lib
)

if(NOT VERBS_LIB)
    message(FATAL_ERROR "Cannot find libibverbs - install rdma-core")
endif()

if(NOT MLX5_LIB)
    message(FATAL_ERROR "Cannot find libmlx5 - install rdma-core")
endif()

if(NOT RDMACM_LIB)
    message(FATAL_ERROR "Cannot find librdmacm - install rdma-core")
endif()

message(STATUS "Found libibverbs: ${VERBS_LIB}")
message(STATUS "Found libmlx5: ${MLX5_LIB}")
message(STATUS "Found librdmacm: ${RDMACM_LIB}")
message(STATUS "JNI include: ${JNI_INCLUDE_DIRS}")

# Create the shared library
# Uses rdma_cm for device binding (supports bonded interfaces)
add_library(spt_rdma SHARED
    rdma_native.c
)

# Include directories
target_include_directories(spt_rdma PRIVATE
    ${JNI_INCLUDE_DIRS}
    ${VERBS_INCLUDE_DIR}
    ${MLX5DV_INCLUDE_DIR}
    ${RDMACM_INCLUDE_DIR}
)

# Link libraries
target_link_libraries(spt_rdma PRIVATE
    ${VERBS_LIB}
    ${MLX5_LIB}
    ${RDMACM_LIB}
    pthread
)

# Set output name
set_target_properties(spt_rdma PROPERTIES
    OUTPUT_NAME "spt_rdma"
    # RPATH for finding libraries at runtime
    INSTALL_RPATH "$ORIGIN:/usr/lib64:/usr/local/lib"
    BUILD_WITH_INSTALL_RPATH TRUE
)

# Installation
install(TARGETS spt_rdma
    LIBRARY DESTINATION lib
)

# Summary
message(STATUS "")
message(STATUS "=== SPT S3-RDMA Build Configuration ===")
message(STATUS "  libibverbs: ${VERBS_LIB}")
message(STATUS "  libmlx5:    ${MLX5_LIB}")
message(STATUS "  librdmacm:  ${RDMACM_LIB}")
message(STATUS "  JNI:        ${JNI_INCLUDE_DIRS}")
message(STATUS "  Output:     libspt_rdma.so")
message(STATUS "========================================")
