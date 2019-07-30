#include "tagtree/index/series_manager.h"

namespace tagtree {

SeriesEntry* SeriesManager::add(const TSID& tsid,
                                const std::vector<promql::Label>& labels)
{
    series_map[tsid] = std::make_unique<SeriesEntry>(tsid, labels);
    auto* entry = series_map[tsid].get();
    return entry;
}

SeriesEntry* SeriesManager::get(const TSID& tsid)
{
    auto it = series_map.find(tsid);
    if (it == series_map.end()) {
        return nullptr;
    }

    return it->second.get();
}

} // namespace tagtree
