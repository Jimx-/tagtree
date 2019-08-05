#include "tagtree/series/rados_series_manager.h"

namespace tagtree {

const std::string RadosSeriesManager::RADOS_CLUSTER = "ceph";
const std::string RadosSeriesManager::RADOS_USERNAME = "client.admin";
const std::string RadosSeriesManager::RADOS_POOL = "tagtree";

RadosSeriesManager::RadosSeriesManager(std::string_view conf,
                                       std::string_view cluster_name,
                                       std::string_view username,
                                       std::string_view pool)
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

SeriesEntry* RadosSeriesManager::add(const TSID& tsid,
                                     const std::vector<promql::Label>& labels)
{
    series_map[tsid] = std::make_unique<SeriesEntry>(tsid, labels);
    auto* entry = series_map[tsid].get();
    write_entry(entry);
    return entry;
}

SeriesEntry* RadosSeriesManager::get(const TSID& tsid)
{
    auto it = series_map.find(tsid);
    if (it == series_map.end()) {
        auto entry = std::make_unique<SeriesEntry>(tsid);
        if (!read_entry(entry.get())) {
            return nullptr;
        }

        auto* ret = entry.get();
        series_map[tsid] = std::move(entry);
        return ret;
    }

    return it->second.get();
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
