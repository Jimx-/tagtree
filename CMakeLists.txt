cmake_minimum_required(VERSION 3.5)
project(tagtree)

set(CMAKE_CXX_STANDARD 17)

set(TOPDIR ${PROJECT_SOURCE_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${TOPDIR}/cmake")

option(TAGTREE_USE_RADOS "Use Ceph/RADOS via librados" OFF)
option(TAGTREE_USE_MONGODB "Use MongoDB" OFF)
option(TAGTREE_USE_AVX2 "Enable AVX2 optimizations" ON)

include(FindSSE)
if (TAGTREE_USE_AVX2 AND AVX2_FOUND)
   message(STATUS "AVX2 optimizations enabled")
   add_compile_options(-mavx2)
   add_definitions(-D_TAGTREE_USE_AVX2_)
endif (TAGTREE_USE_AVX2 AND AVX2_FOUND)

# Find libuuid
find_package(Libuuid REQUIRED)
if (NOT LIBUUID_FOUND)
   message(FATAL_ERROR "You might need to install libuuid first")
endif()
include_directories(${LIBUUID_INCLUDE_DIRS})

set(INCLUDE_DIRS
    ${TOPDIR}/include
    ${TOPDIR}/3rdparty/bptree/include
    ${LIBUUID_INCLUDE_DIRS}
)

if (TAGTREE_USE_MONGODB)
   # Find MongoDB driver
   find_package(libmongocxx REQUIRED)
   find_package(libbsoncxx REQUIRED)
   message(STATUS "MongoCXX status:")
   message(STATUS "    libraries: ${LIBMONGOCXX_LIBRARIES}")
   include_directories(${LIBMONGOCXX_INCLUDE_DIRS})
   include_directories(${LIBBSONCXX_INCLUDE_DIRS})

   set(INCLUDE_DIRS
    ${LIBMONGOCXX_INCLUDE_DIRS}
    ${LIBBSONCXX_INCLUDE_DIRS}
   )
endif (TAGTREE_USE_MONGODB)

include_directories(
    ${INCLUDE_DIRS}
)

set(SOURCE_FILES
    ${TOPDIR}/src/adapters/prom/appender.cpp
    ${TOPDIR}/src/adapters/prom/indexed_storage.cpp
    ${TOPDIR}/src/adapters/prom/querier.cpp
    ${TOPDIR}/src/index/index_server.cpp
    ${TOPDIR}/src/index/index_tree.cpp
    ${TOPDIR}/src/series/series_manager.cpp
)
           
set(HEADER_FILES
    ${TOPDIR}/include/tagtree/storage.h
    ${TOPDIR}/include/tagtree/tsid.h
    ${TOPDIR}/include/tagtree/adapters/prom/appender.h
    ${TOPDIR}/include/tagtree/adapters/prom/indexed_storage.h
    ${TOPDIR}/include/tagtree/adapters/prom/querier.h
    ${TOPDIR}/include/tagtree/index/index_server.h
    ${TOPDIR}/include/tagtree/index/index_tree.h
    ${TOPDIR}/include/tagtree/series/series_manager.h
)

if (TAGTREE_USE_RADOS)
   set(SOURCE_FILES
    ${SOURCE_FILES}
    ${TOPDIR}/src/series/rados_series_manager.cpp
   )
   set(HEADER_FILES
    ${HEADER_FILES}
    ${TOPDIR}/include/tagtree/series/rados_series_manager.h
   )
endif (TAGTREE_USE_RADOS)

if (TAGTREE_USE_MONGODB)
set(SOURCE_FILES
    ${SOURCE_FILES}
    ${TOPDIR}/src/series/mongodb_series_manager.cpp
   )
   set(HEADER_FILES
    ${HEADER_FILES}
    ${TOPDIR}/include/tagtree/series/mongodb_series_manager.h
   )
endif (TAGTREE_USE_MONGODB)

set(EXT_SOURCE_FILES )

add_subdirectory(${TOPDIR}/3rdparty/bptree)

set(LIBRARIES
    pthread
    atomic
    bptree
    ${LIBUUID_LIBRARY}
)

if (TAGTREE_USE_RADOS)
   set(LIBRARIES
    ${LIBRARIES}
    rados
   )
endif (TAGTREE_USE_RADOS)

if (TAGTREE_USE_MONGODB)
   set(LIBRARIES
    ${LIBRARIES}
    ${LIBMONGOCXX_LIBRARIES}
   )
endif (TAGTREE_USE_MONGODB)

add_library(tagtree STATIC ${SOURCE_FILES} ${HEADER_FILES} ${EXT_SOURCE_FILES})
target_link_libraries(tagtree ${LIBRARIES})
target_include_directories(tagtree PUBLIC ${INCLUDE_DIRS})
install(TARGETS tagtree DESTINATION lib)