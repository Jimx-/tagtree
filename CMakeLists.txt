cmake_minimum_required(VERSION 3.5)
project(tagtree)

set(CMAKE_CXX_STANDARD 17)

set(TOPDIR ${PROJECT_SOURCE_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${TOPDIR}/cmake")

option(TAGTREE_USE_AVX2 "Enable AVX2 optimizations" ON)

include(FindSSE)
if (TAGTREE_USE_AVX2 AND AVX2_FOUND)
   message(STATUS "AVX2 optimizations enabled")
   add_compile_options(-mavx2)
   add_definitions(-D_TAGTREE_USE_AVX2_)
endif (TAGTREE_USE_AVX2 AND AVX2_FOUND)

set(INCLUDE_DIRS
    ${TOPDIR}/include
    ${TOPDIR}/3rdparty/bptree/include
)

include_directories(
    ${INCLUDE_DIRS}
)

set(SOURCE_FILES
    ${TOPDIR}/src/adapters/prom/appender.cpp
    ${TOPDIR}/src/adapters/prom/indexed_storage.cpp
    ${TOPDIR}/src/adapters/prom/querier.cpp
    ${TOPDIR}/src/index/bitmap.cpp
    ${TOPDIR}/src/index/index_server.cpp
    ${TOPDIR}/src/index/index_tree.cpp
    ${TOPDIR}/src/series/btree_series_manager.cpp
    ${TOPDIR}/src/series/series_manager.cpp
    ${TOPDIR}/src/series/symbol_table.cpp
)
           
set(HEADER_FILES
    ${TOPDIR}/include/tagtree/storage.h
    ${TOPDIR}/include/tagtree/tsid.h
    ${TOPDIR}/include/tagtree/adapters/prom/appender.h
    ${TOPDIR}/include/tagtree/adapters/prom/indexed_storage.h
    ${TOPDIR}/include/tagtree/adapters/prom/querier.h
    ${TOPDIR}/include/tagtree/index/bitmap.h
    ${TOPDIR}/include/tagtree/index/index_server.h
    ${TOPDIR}/include/tagtree/index/index_tree.h
    ${TOPDIR}/include/tagtree/series/btree_series_manager.h
    ${TOPDIR}/include/tagtree/series/series_manager.h
    ${TOPDIR}/include/tagtree/series/symbol_table.h
)

set(EXT_SOURCE_FILES )

add_subdirectory(${TOPDIR}/3rdparty/bptree)

set(LIBRARIES
    pthread
    atomic
    bptree
)

add_library(tagtree STATIC ${SOURCE_FILES} ${HEADER_FILES} ${EXT_SOURCE_FILES})
target_link_libraries(tagtree ${LIBRARIES})
target_include_directories(tagtree PUBLIC ${INCLUDE_DIRS})
install(TARGETS tagtree DESTINATION lib)
