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
    std::vector<promql::LabelMatcher> matchers;
    for (auto&& p : labels) {
        matchers.emplace_back(MatchOp::EQL, p.name, p.value);
    }

    std::unordered_set<TSID> tsids;
    parent->get_index()->resolve_label_matchers(matchers, tsids);

    if (tsids.size() > 1) {
        throw std::runtime_error("series is not unique");
    }

    if (tsids.empty()) {
        auto tsid = parent->get_index()->add_series(labels);
        tsids.insert(tsid);
    }

    for (auto&& p : tsids) {
        app->add(p, t, v);
    }
}

void PromAppender::commit() { app->commit(); }

} // namespace prom
} // namespace tagtree
