#include "tagtree/series/series_manager.h"

namespace tagtree {

AbstractSeriesManager::AbstractSeriesManager(size_t cache_size)
    : max_entries(cache_size), symtab("symbol.tab")
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

    RefSeriesEntry rsent;
    sent_to_rsent(entryp, &rsent);
    write_entry(&rsent);
    entryp->dirty = false;

    entryp->unlock();
}

SeriesEntry* AbstractSeriesManager::get(const TSID& tsid)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto it = series_map.find(tsid);
    if (it == series_map.end()) {
        RefSeriesEntry rsent;
        rsent.tsid = tsid;
        if (!read_entry(&rsent)) {
            return nullptr;
        }

        auto new_entry = get_entry();
        auto* entryp = new_entry.get();
        rsent_to_sent(&rsent, entryp);

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
            RefSeriesEntry rsent;
            sent_to_rsent(new_entry.get(), &rsent);
            write_entry(&rsent);
            new_entry->dirty = false;
        }
        series_map.erase(lru_list.back().first);
        lru_list.pop_back();
    }

    new_entry->lock();
    new_entry->labels.clear();

    return new_entry;
}

void AbstractSeriesManager::sent_to_rsent(SeriesEntry* sent,
                                          RefSeriesEntry* rsent)
{
    rsent->tsid = sent->tsid;
    rsent->labels.clear();
    for (auto&& p : sent->labels) {
        SymbolTable::Ref name_ref, value_ref;
        name_ref = symtab.add_symbol(p.name);
        value_ref = symtab.add_symbol(p.value);
        rsent->labels.emplace_back(name_ref, value_ref);
    }
}

void AbstractSeriesManager::rsent_to_sent(RefSeriesEntry* rsent,
                                          SeriesEntry* sent)
{
    sent->tsid = rsent->tsid;
    sent->labels.clear();
    for (auto&& p : rsent->labels) {
        const auto& name = symtab.get_symbol(p.first);
        const auto& value = symtab.get_symbol(p.second);
        sent->labels.emplace_back(name, value);
    }
}

} // namespace tagtree
