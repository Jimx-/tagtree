#include "tagtree/adapters/prom/querier.h"
#include "tagtree/adapters/prom/indexed_storage.h"

namespace tagtree {
namespace prom {

PromQuerier::PromQuerier(IndexedStorage* parent, uint64_t mint, uint64_t maxt)
    : parent(parent)
{
    querier = parent->get_storage()->querier(mint, maxt);
}

std::shared_ptr<promql::SeriesSet>
PromQuerier::select(const std::vector<promql::LabelMatcher>& matchers)
{
    std::unordered_set<TSID> tsids;
    parent->get_index()->resolve_label_matchers(matchers, tsids);

    auto ss = querier->select(tsids);
    return std::make_shared<PromSeriesSet>(parent, ss);
}

void PromSeries::labels(std::vector<promql::Label>& labels)
{
    parent->get_index()->get_labels(series->tsid(), labels);
}

} // namespace prom
} // namespace tagtree
