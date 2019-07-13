#include "tagtree/index/series_manager.h"

namespace tagtree {

SeriesEntry* SeriesManager::add(PostingID pid,
                                const std::vector<promql::Label>& labels)
{
    series_map[pid] = std::make_unique<SeriesEntry>(labels);
    auto* entry = series_map[pid].get();
    tsid_map[entry->tsid] = entry;
    return entry;
}

SeriesEntry* SeriesManager::get(PostingID pid)
{
    auto it = series_map.find(pid);
    if (it == series_map.end()) {
        return nullptr;
    }

    return it->second.get();
}

SeriesEntry* SeriesManager::get_tsid(const TSID& tsid)
{
    auto it = tsid_map.find(tsid);
    if (it == tsid_map.end()) {
        return nullptr;
    }

    return it->second;
}

} // namespace tagtree
