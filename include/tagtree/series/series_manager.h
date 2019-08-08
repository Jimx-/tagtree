#ifndef _TAGTREE_SERIES_MANAGER_H_
#define _TAGTREE_SERIES_MANAGER_H_

#include "promql/labels.h"
#include "tagtree/tsid.h"

#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace tagtree {

struct SeriesEntry {
    TSID tsid;
    std::vector<promql::Label> labels;
    std::mutex mutex;
    bool dirty;

    SeriesEntry(const std::vector<promql::Label>& labels = {})
        : labels(labels), dirty(false)
    {}
    SeriesEntry(const TSID& tsid, const std::vector<promql::Label>& labels = {})
        : tsid(tsid), labels(labels), dirty(false)
    {}

    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }
};

class AbstractSeriesManager {
public:
    AbstractSeriesManager(size_t cache_size);

    void add(const TSID& tsid, const std::vector<promql::Label>& labels);
    SeriesEntry* get(const TSID& tsid);

protected:
    virtual bool read_entry(SeriesEntry* entry) = 0;
    virtual void write_entry(SeriesEntry* entry) = 0;

private:
    std::mutex mutex;
    size_t max_entries;

    using LRUListType =
        std::list<std::pair<TSID, std::unique_ptr<SeriesEntry>>>;
    LRUListType lru_list;
    std::unordered_map<TSID, LRUListType::iterator> series_map;

    std::unique_ptr<SeriesEntry> get_entry();
};

} // namespace tagtree

#endif
