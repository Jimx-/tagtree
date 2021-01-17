#include "tagtree/adapters/prom/appender.h"
#include "tagtree/adapters/prom/indexed_storage.h"

using promql::MatchOp;

namespace tagtree {
namespace prom {

PromAppender::PromAppender(IndexedStorage* parent) : parent(parent)
{
    app = parent->get_storage()->appender();
}

void PromAppender::add(const std::vector<promql::Label>& labels, uint64_t t,
                       double v)
{

    MemPostingList tsids;
    parent->get_index()->exists(labels, tsids);

    if (tsids.cardinality() > 1) {
        throw std::runtime_error("series is not unique");
    }

    TSID tsid;
    if (!tsids.cardinality()) {
        tsid = parent->get_index()->add_series(t, labels);
        series.emplace_back(tsid, labels, t);
    } else {
        tsid = *tsids.begin();
    }

    app->add(tsid, t, v);
}

void PromAppender::commit()
{
    if (series.size()) {
        parent->get_index()->commit(series);
    }

    app->commit();

    series.clear();
}

} // namespace prom
} // namespace tagtree
