#include "tagtree/series/rados_series_manager.h"

namespace tagtree {

const std::string RadosSeriesManager::RADOS_CLUSTER = "ceph";
const std::string RadosSeriesManager::RADOS_USERNAME = "client.admin";
const std::string RadosSeriesManager::RADOS_POOL = "tagtree";

RadosSeriesManager::RadosSeriesManager(size_t cache_size, std::string_view conf,
                                       std::string_view cluster_name,
                                       std::string_view username,
                                       std::string_view pool)
    : AbstractSeriesManager(cache_size)
{
    int ret;
    uint64_t flags = 0;

    ret = cluster.init2(username.data(), cluster_name.data(), flags);
    if (ret < 0) {
        throw RadosError("failed initialize ceph cluster");
    }

    ret = cluster.conf_read_file(conf.data());
    if (ret < 0) {
        throw RadosError("failed to read ceph config file");
    }

    ret = cluster.connect();
    if (ret < 0) {
        throw RadosError("failed to connect to ceph cluster");
    }

    ret = cluster.ioctx_create(pool.data(), ioctx);
    if (ret < 0) {
        throw RadosError("failed to create IO context");
    }
}

bool RadosSeriesManager::read_entry(SeriesEntry* entry)
{
    int ret;
    std::string oid = entry->tsid.to_string();

    std::map<std::string, librados::bufferlist> attrs;
    ret = ioctx.getxattrs(oid, attrs);

    if (ret < 0) return false;

    entry->labels.clear();
    for (auto&& p : attrs) {
        entry->labels.emplace_back(p.first, p.second.to_str());
    }

    return true;
}

void RadosSeriesManager::write_entry(SeriesEntry* entry)
{
    int ret;
    librados::bufferlist bl;
    std::string oid = entry->tsid.to_string();

    bl.append("");
    ret = ioctx.write_full(oid, bl);
    if (ret < 0) {
        throw RadosError("failed to create object: " + entry->tsid.to_string());
    }

    for (auto&& p : entry->labels) {
        bl.clear();
        bl.append(p.value);
        ret = ioctx.setxattr(oid, p.name.c_str(), bl);

        if (ret < 0) {
            throw RadosError("failed to set xattr");
        }
    }
}

} // namespace tagtree
