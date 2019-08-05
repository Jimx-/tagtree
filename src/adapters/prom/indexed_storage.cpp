#include "tagtree/adapters/prom/indexed_storage.h"
#include "tagtree/adapters/prom/appender.h"
#include "tagtree/adapters/prom/querier.h"

namespace tagtree {
namespace prom {

IndexedStorage::IndexedStorage(std::string_view index_dir,
                               tagtree::Storage* storage,
                               tagtree::AbstractSeriesManager* sm)
    : storage(storage)
{
    index_server = std::make_unique<IndexServer>(index_dir, sm);
}

std::shared_ptr<promql::Querier> IndexedStorage::querier(uint64_t mint,
                                                         uint64_t maxt)
{
    return std::make_shared<PromQuerier>(this, mint, maxt);
}

void IndexedStorage::label_values(const std::string& name,
                                  std::unordered_set<std::string>& values)
{
    index_server->label_values(name, values);
}

std::shared_ptr<promql::Appender> IndexedStorage::appender()
{
    return std::make_shared<PromAppender>(this);
}

} // namespace prom
} // namespace tagtree
