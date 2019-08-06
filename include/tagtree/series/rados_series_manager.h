#ifndef _TAGTREE_RADOS_SERIES_MANAGER_H_
#define _TAGTREE_RADOS_SERIES_MANAGER_H_

#include "tagtree/series/series_manager.h"

#include <rados/librados.hpp>
#include <unordered_map>

namespace tagtree {

class RadosError : public std::runtime_error {
public:
    RadosError(const std::string& message) : std::runtime_error(message) {}
};

class RadosSeriesManager : public AbstractSeriesManager {
public:
    RadosSeriesManager(size_t cache_size, std::string_view conf,
                       std::string_view cluster_name = RADOS_CLUSTER,
                       std::string_view username = RADOS_USERNAME,
                       std::string_view pool = RADOS_POOL);

private:
    static const std::string RADOS_CLUSTER;
    static const std::string RADOS_USERNAME;
    static const std::string RADOS_POOL;

    librados::Rados cluster;
    librados::IoCtx ioctx;

    virtual bool read_entry(SeriesEntry* entry);
    virtual void write_entry(SeriesEntry* entry);
};

} // namespace tagtree

#endif
