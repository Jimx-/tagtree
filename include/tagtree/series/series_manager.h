#ifndef _TAGTREE_SERIES_MANAGER_H_
#define _TAGTREE_SERIES_MANAGER_H_

#include "promql/labels.h"
#include "tagtree/series/symbol_table.h"
#include "tagtree/tsid.h"

#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
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
    SeriesEntry(TSID tsid, const std::vector<promql::Label>& labels = {})
        : tsid(tsid), labels(labels), dirty(false)
    {}

    void lock() { mutex.lock(); }
    void unlock() { mutex.unlock(); }
};

struct RefSeriesEntry {
    TSID tsid;
    std::vector<std::pair<SymbolTable::Ref, SymbolTable::Ref>> labels;
};

class AbstractSeriesManager {
public:
    AbstractSeriesManager(size_t cache_size);

    void add(TSID tsid, const std::vector<promql::Label>& labels,
             bool is_new = true);
    SeriesEntry* get(TSID tsid);
    SeriesEntry* get_by_label_set(const std::vector<promql::Label>& lset);

    SymbolTable::Ref add_symbol(std::string_view symbol)
    {
        return symtab.add_symbol(symbol);
    }
    const std::string& get_symbol(SymbolTable::Ref ref)
    {
        return symtab.get_symbol(ref);
    }

    virtual void flush();

protected:
    virtual bool read_entry(RefSeriesEntry* entry) = 0;
    virtual void write_entry(RefSeriesEntry* entry) = 0;

private:
    std::shared_mutex mutex;
    size_t max_entries;
    SymbolTable symtab;

    using LRUListType =
        std::list<std::pair<TSID, std::unique_ptr<SeriesEntry>>>;
    LRUListType lru_list;
    std::unordered_map<TSID, LRUListType::iterator> series_map;
    std::unordered_map<uint64_t, SeriesEntry*> series_hash_map;

    std::unique_ptr<SeriesEntry> get_entry();

    void sent_to_rsent(SeriesEntry* sent, RefSeriesEntry* rsent);
    void rsent_to_sent(RefSeriesEntry* rsent, SeriesEntry* sent);
};

} // namespace tagtree

#endif
