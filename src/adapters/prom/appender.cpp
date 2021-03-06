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

    auto [tsid, inserted] = parent->get_index()->add_series(t, labels);

    if (inserted) series.emplace_back(tsid, labels, t);

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
