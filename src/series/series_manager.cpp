#include "tagtree/series/series_manager.h"

namespace tagtree {

SeriesEntry*
AbstractSeriesManager::add(const TSID& tsid,
                           const std::vector<promql::Label>& labels)
{
    series_map[tsid] = std::make_unique<SeriesEntry>(tsid, labels);
    auto* entry = series_map[tsid].get();
    write_entry(entry);
    return entry;
}

SeriesEntry* AbstractSeriesManager::get(const TSID& tsid)
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

} // namespace tagtree
