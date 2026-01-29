cmake_minimum_required(VERSION 3.16)
project(spt_rdma_native CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find JNI
find_package(JNI REQUIRED)

# Path to rdma-object-client (can be overridden via -DOBJCLIENT_DIR=...)
if(NOT DEFINED OBJCLIENT_DIR)
    set(OBJCLIENT_DIR "$ENV{HOME}/repos/rdma-object-client")
endif()

# Path to AWS CRT installation (NVIDIA RDMA fork)
if(NOT DEFINED AWS_CRT_DIR)
    set(AWS_CRT_DIR "$ENV{HOME}/repos/aws-c-s3/local-install")
endif()

message(STATUS "Using rdma-object-client from: ${OBJCLIENT_DIR}")
message(STATUS "Using AWS CRT from: ${AWS_CRT_DIR}")

# Check that the required files exist
if(NOT EXISTS "${OBJCLIENT_DIR}/utils/include/obj_client.h")
    message(FATAL_ERROR "Cannot find obj_client.h in ${OBJCLIENT_DIR}/utils/include/")
endif()

if(NOT EXISTS "${OBJCLIENT_DIR}/build/utils/libutils.a")
    message(FATAL_ERROR "Cannot find libutils.a in ${OBJCLIENT_DIR}/build/utils/ - build rdma-object-client first")
endif()

if(NOT EXISTS "${AWS_CRT_DIR}/include/aws/common/byte_buf.h")
    message(FATAL_ERROR "Cannot find AWS CRT headers in ${AWS_CRT_DIR}/include/")
endif()

# Create the shared library
add_library(spt_rdma_native SHARED
    rdma_jni.cpp
)

# Include directories
target_include_directories(spt_rdma_native PRIVATE
    ${JNI_INCLUDE_DIRS}
    ${OBJCLIENT_DIR}/utils/include
    ${AWS_CRT_DIR}/include
)

# Find AWS CRT static libraries
set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
set(AWS_LIB_NAMES
    aws-c-s3
    aws-c-auth
    aws-c-http
    aws-c-io
    aws-c-common
    aws-checksums
    aws-c-cal
    aws-c-compression
    aws-c-sdkutils
    s2n
    crypto
    ssl
)

set(AWS_LIBS "")
foreach(LIB_NAME ${AWS_LIB_NAMES})
    unset(FOUND_LIB CACHE)
    find_library(FOUND_LIB
        NAMES ${LIB_NAME}
        PATHS ${AWS_CRT_DIR}/lib ${AWS_CRT_DIR}/lib64
        NO_DEFAULT_PATH
        REQUIRED
    )
    list(APPEND AWS_LIBS ${FOUND_LIB})
endforeach()

# Link libraries - all static linking for portability
target_link_libraries(spt_rdma_native PRIVATE
    ${OBJCLIENT_DIR}/build/utils/libutils.a
    ${AWS_LIBS}
    # System libraries
    pthread
    dl
)

# Set the output name and install RPATH
set_target_properties(spt_rdma_native PROPERTIES
    OUTPUT_NAME "spt_rdma_native"
    # Set RPATH to look for libraries in same directory and standard paths
    INSTALL_RPATH "$ORIGIN:$ORIGIN/lib:/usr/lib64:/usr/local/lib"
    BUILD_WITH_INSTALL_RPATH TRUE
)

# Installation
install(TARGETS spt_rdma_native
    LIBRARY DESTINATION lib
)
