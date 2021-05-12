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

class SeriesStripe {
public:
    void add(uint64_t hash, SeriesEntry* entry)
    {
        std::unique_lock<std::shared_mutex> guard(mutex);
        series_hash_map[hash] = entry;
    }
    SeriesEntry* get(uint64_t hash)
    {
        std::shared_lock<std::shared_mutex> guard(mutex);
        auto it = series_hash_map.find(hash);
        if (it == series_hash_map.end()) return nullptr;
        it->second->lock();
        return it->second;
    }
    void erase(uint64_t hash)
    {
        std::unique_lock<std::shared_mutex> guard(mutex);
        series_hash_map.erase(hash);
    }

private:
    std::unordered_map<uint64_t, SeriesEntry*> series_hash_map;
    std::shared_mutex mutex;

    struct __Inner {
        std::unordered_map<uint64_t, SeriesEntry*> __map;
        std::shared_mutex __mutex;
    };
    char __padding[-sizeof(__Inner) & 63];
};

class AbstractSeriesManager {
public:
    AbstractSeriesManager(size_t cache_size, std::string_view series_dir);

    void add(TSID tsid, const std::vector<promql::Label>& labels,
             bool is_new = true);
    SeriesEntry* get(TSID tsid);
    SeriesEntry* get_by_label_set(const std::vector<promql::Label>& lset);

    bool get_label_set(TSID tsid, std::vector<promql::Label>& lset);

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
    std::string series_dir;

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

    static const size_t NUM_STRIPES = 16;
    static const size_t STRIPE_MASK = NUM_STRIPES - 1;
    std::array<SeriesStripe, NUM_STRIPES> stripes;

    inline SeriesStripe& get_stripe(uint64_t hash)
    {
        return stripes[hash & STRIPE_MASK];
    }

    std::unique_ptr<SeriesEntry> get_entry();

    void init_series_dir();

    void sent_to_rsent(SeriesEntry* sent, RefSeriesEntry* rsent);
    void rsent_to_sent(RefSeriesEntry* rsent, SeriesEntry* sent);
};

} // namespace tagtree

#endif
