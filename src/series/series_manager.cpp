#include "tagtree/series/series_manager.h"

namespace tagtree {

AbstractSeriesManager::AbstractSeriesManager(size_t cache_size)
    : max_entries(cache_size)
{}

void AbstractSeriesManager::add(const TSID& tsid,
                                const std::vector<promql::Label>& labels)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto new_entry = get_entry();
    new_entry->tsid = tsid;
    new_entry->labels = labels;
    new_entry->dirty = true;
    auto* entryp = new_entry.get();

    lru_list.emplace_front(tsid, std::move(new_entry));
    series_map.emplace(tsid, lru_list.begin());

    entryp->unlock();
}

SeriesEntry* AbstractSeriesManager::get(const TSID& tsid)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto it = series_map.find(tsid);
    if (it == series_map.end()) {
        SeriesEntry entry;
        entry.tsid = tsid;
        if (!read_entry(&entry)) {
            return nullptr;
        }

        auto new_entry = get_entry();
        new_entry->tsid = tsid;
        new_entry->labels = std::move(entry.labels);
        auto* entryp = new_entry.get();

        lru_list.emplace_front(tsid, std::move(new_entry));
        series_map.emplace(tsid, lru_list.begin());

        return entryp;
    }

    lru_list.splice(lru_list.begin(), lru_list, it->second);

    auto* entry = it->second->second.get();
    entry->lock();
    return entry;
}

std::unique_ptr<SeriesEntry> AbstractSeriesManager::get_entry()
{
    std::unique_ptr<SeriesEntry> new_entry;

    if (lru_list.size() < max_entries) {
        new_entry = std::make_unique<SeriesEntry>();
    } else {
        new_entry = std::move(lru_list.back().second);

        if (new_entry->dirty) {
            write_entry(new_entry.get());
            new_entry->dirty = false;
        }
        series_map.erase(lru_list.back().first);
        lru_list.pop_back();
    }

    new_entry->lock();
    new_entry->labels.clear();

    return new_entry;
}

} // namespace tagtree
