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
    ${TOPDIR}/3rdparty/CRoaringUnityBuild
    ${TOPDIR}/3rdparty/xxHash
    ${TOPDIR}/3rdparty/CRCpp/inc
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
    ${TOPDIR}/src/index/mem_index.cpp
    ${TOPDIR}/src/series/series_file.cpp
    ${TOPDIR}/src/series/series_file_manager.cpp
    ${TOPDIR}/src/series/series_manager.cpp
    ${TOPDIR}/src/series/symbol_table.cpp
    ${TOPDIR}/src/swig/wrapper.cpp    
    ${TOPDIR}/src/tree/item_page_view.cpp
    ${TOPDIR}/src/tree/sorted_list_page_view.cpp
    ${TOPDIR}/src/wal/record_serializer.cpp
    ${TOPDIR}/src/wal/reader.cpp
    ${TOPDIR}/src/wal/wal.cpp
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
    ${TOPDIR}/include/tagtree/index/mem_index.h
    ${TOPDIR}/include/tagtree/series/series_file.h
    ${TOPDIR}/include/tagtree/series/series_file_manager.h
    ${TOPDIR}/include/tagtree/series/series_manager.h
    ${TOPDIR}/include/tagtree/series/symbol_table.h
    ${TOPDIR}/include/tagtree/wal/reader.h
    ${TOPDIR}/include/tagtree/wal/records.h
    ${TOPDIR}/include/tagtree/wal/record_serializer.h
    ${TOPDIR}/include/tagtree/wal/wal.h
)

set(EXT_SOURCE_FILES
    3rdparty/CRoaringUnityBuild/roaring.c
    ${TOPDIR}/3rdparty/xxHash/xxhash.c
)

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

find_package(SWIG 4.0 COMPONENTS go)
include(UseSWIG)
set (UseSWIG_TARGET_NAME_PREFERENCE STANDARD)
set_property(SOURCE ${TOPDIR}/src/swig/tagtree.i PROPERTY CPLUSPLUS ON)
swig_add_library(tagtreego LANGUAGE go SOURCES ${TOPDIR}/src/swig/tagtree.i)
set_property(TARGET tagtreego PROPERTY SWIG_COMPILE_OPTIONS -intgosize 64 -cgo)
set_target_properties(tagtreego PROPERTIES PREFIX "lib")

add_custom_command(TARGET tagtreego
                   POST_BUILD
                   COMMAND sed -i 's/\#undef intgo/\#undef intgo\\n\#cgo LDFLAGS: -L$$\\{SRCDIR\\} -ltagtreego -ltagtree -lstdc++ -lboost_thread -lpromql -lbptree/g' tagtreego.go
                   DEPENDS ${CMAKE_BINARY_DIR}/tagtree/tagtreego.go
                   )
