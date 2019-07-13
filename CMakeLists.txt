cmake_minimum_required(VERSION 3.5)
project(tagtree)

set(CMAKE_CXX_STANDARD 17)

set(TOPDIR ${PROJECT_SOURCE_DIR})

set(TAGTREE_INCLUDE_DIRS
    ${TOPDIR}/include
    ${TOPDIR}/3rdparty/bptree/include
    PARENT_SCOPE 
)

include_directories(
    ${TOPDIR}/include
    ${TOPDIR}/3rdparty/bptree/include
)

set(SOURCE_FILES
    ${TOPDIR}/src/adapters/prom/appender.cpp
    ${TOPDIR}/src/adapters/prom/indexed_storage.cpp
    ${TOPDIR}/src/adapters/prom/querier.cpp
    ${TOPDIR}/src/index/index_server.cpp
    ${TOPDIR}/src/index/index_tree.cpp
    ${TOPDIR}/src/index/series_manager.cpp)
            
set(HEADER_FILES
    ${TOPDIR}/include/tagtree/adapters/prom/appender.h
    ${TOPDIR}/include/tagtree/adapters/prom/indexed_storage.h
    ${TOPDIR}/include/tagtree/adapters/prom/querier.h
    ${TOPDIR}/include/tagtree/index/index_server.h
    ${TOPDIR}/include/tagtree/index/index_tree.h
    ${TOPDIR}/include/tagtree/index/series_manager.h
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
install(TARGETS tagtree DESTINATION lib)
