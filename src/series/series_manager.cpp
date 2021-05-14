#include "tagtree/series/series_manager.h"

#include "xxhash.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tagtree {

static uint64_t get_label_set_hash(const std::vector<promql::Label>& lset)
{
    std::string buffer;
    const char sep = 0xff;

    for (auto&& p : lset) {
        buffer += p.name;
        buffer += sep;
        buffer += p.value;
        buffer += sep;
    }

    return XXH64(buffer.c_str(), buffer.length(), 0);
}

AbstractSeriesManager::AbstractSeriesManager(size_t cache_size,
                                             std::string_view series_dir)
    : max_entries(cache_size), series_dir(series_dir),
      symtab((init_series_dir(), this->series_dir + "/symbol.tab"))
{}

void AbstractSeriesManager::init_series_dir()
{
    struct stat sbuf;
    int ret = ::stat(series_dir.c_str(), &sbuf);

    if (ret == -1 && errno == ENOENT) {
        ret =
            ::mkdir(series_dir.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP |
                                            S_IXGRP | S_IROTH | S_IXOTH);

        if (ret == -1) {
            throw std::runtime_error("failed to create WAL directory " +
                                     series_dir);
        }
    }
}

void AbstractSeriesManager::add(TSID tsid,
                                const std::vector<promql::Label>& labels,
                                bool is_new)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto new_entry = get_entry();
    new_entry->tsid = tsid;
    new_entry->labels = labels;
    new_entry->dirty = true;
    auto* entryp = new_entry.get();

    lru_list.emplace_front(tsid, std::move(new_entry));
    series_map.emplace(tsid, lru_list.begin());
    auto hash = get_label_set_hash(labels);
    get_stripe(hash).add(hash, entryp);

    if (is_new) {
        RefSeriesEntry rsent;
        sent_to_rsent(entryp, &rsent);
        write_entry(&rsent);
    }

    entryp->dirty = false;

    entryp->unlock();
}

SeriesEntry* AbstractSeriesManager::get(TSID tsid)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

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
        auto hash = get_label_set_hash(entryp->labels);
        get_stripe(hash).add(hash, entryp);

        return entryp;
    }

    lru_list.splice(lru_list.begin(), lru_list, it->second);

    auto* entry = it->second->second.get();
    entry->lock();
    return entry;
}

bool AbstractSeriesManager::get_label_set(TSID tsid,
                                          std::vector<promql::Label>& lset)
{
    std::shared_lock<std::shared_mutex> lock(mutex);

    auto it = series_map.find(tsid);
    if (it == series_map.end()) return false;

    auto* entry = it->second->second.get();
    std::copy(entry->labels.begin(), entry->labels.end(),
              std::back_inserter(lset));

    return true;
}

std::optional<TSID>
SeriesStripe::get_tsid_by_label_set(uint64_t hash,
                                    const std::vector<promql::Label>& lset)
{
    std::shared_lock<std::shared_mutex> guard(mutex);
    auto it = series_hash_map.find(hash);
    if (it == series_hash_map.end()) return std::nullopt;

    auto entry = it->second;

    if (entry->labels.size() != lset.size()) return std::nullopt;

    auto it1 = lset.begin();
    auto it2 = entry->labels.begin();
    for (; it1 != lset.end(); it1++, it2++) {
        if (it1->name != it2->name || it1->value != it2->value)
            return std::nullopt;
    }

    return entry->tsid;
}

std::optional<TSID> AbstractSeriesManager::get_tsid_by_label_set(
    const std::vector<promql::Label>& lset)
{
    auto hash = get_label_set_hash(lset);
    return get_stripe(hash).get_tsid_by_label_set(hash, lset);
}

SeriesEntry*
AbstractSeriesManager::get_by_label_set(const std::vector<promql::Label>& lset)
{
    auto hash = get_label_set_hash(lset);
    auto entry = get_stripe(hash).get(hash);

    if (!entry) {
        return nullptr;
    }

    if (entry->labels.size() != lset.size()) {
        entry->unlock();
        return nullptr;
    }

    auto it1 = lset.begin();
    auto it2 = entry->labels.begin();
    for (; it1 != lset.end(); it1++, it2++) {
        if (it1->name != it2->name || it1->value != it2->value) {
            entry->unlock();
            return nullptr;
        }
    }

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
        auto hash = get_label_set_hash(new_entry->labels);
        get_stripe(hash).erase(hash);
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

void AbstractSeriesManager::flush() { symtab.flush(); }

} // namespace tagtree
