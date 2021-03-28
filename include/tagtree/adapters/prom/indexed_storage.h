#ifndef _TAGTREE_PROM_INDEXED_STORAGE_H_
#define _TAGTREE_PROM_INDEXED_STORAGE_H_

#include "promql/storage.h"
#include "tagtree/index/index_server.h"
#include "tagtree/storage.h"

namespace tagtree {
namespace prom {

class IndexedStorage : public promql::Storage {
public:
    IndexedStorage(std::string_view index_dir, size_t cache_size,
                   tagtree::Storage* storage,
                   tagtree::AbstractSeriesManager* sm, bool bitmap_only = false,
                   bool full_cache = true);
    tagtree::Storage* get_storage() const { return storage; }
    IndexServer* get_index() const { return index_server.get(); }

    virtual std::shared_ptr<promql::Querier> querier(uint64_t mint,
                                                     uint64_t maxt);
    virtual void label_values(const std::string& name,
                              std::unordered_set<std::string>& values);
    virtual std::shared_ptr<promql::Appender> appender();

private:
    std::unique_ptr<IndexServer> index_server;
    tagtree::Storage* storage;
};

} // namespace prom
} // namespace tagtree

#endif
