#include "tagtree/index/index_server.h"
#include "tagtree/series/series_manager.h"

#include <sstream>
#include <unordered_set>

using promql::MatchOp;

namespace tagtree {

IndexServer::IndexServer(std::string_view index_dir, size_t cache_size,
                         AbstractSeriesManager* sm)
    : index_tree(this, index_dir, cache_size)
{
    series_manager = sm;
    id_counter.store(sm->get_size() + 1);
}

IndexServer::~IndexServer() {}

TSID IndexServer::add_series(const std::vector<promql::Label>& labels)
{
    TSID new_id = get_tsid();
    index_tree.add_series(new_id, labels);
    series_manager->add(new_id, labels);
    return new_id;
}

bool IndexServer::get_labels(TSID tsid, std::vector<promql::Label>& labels)
{
    auto* entry = series_manager->get(tsid);
    if (!entry) return false;
    labels.clear();
    std::copy(entry->labels.begin(), entry->labels.end(),
              std::back_inserter(labels));
    entry->unlock();
    return true;
}

void IndexServer::label_values(const std::string& label_name,
                               std::unordered_set<std::string>& values)
{
    std::unordered_set<TSID> tsids;
    std::vector<promql::Label> labels;
    index_tree.resolve_label_matchers({{MatchOp::NEQ, label_name, ""}}, tsids);

    for (auto&& tsid : tsids) {
        labels.clear();
        auto* entry = series_manager->get(tsid);

        for (auto&& p : entry->labels) {
            if (p.name == label_name) {
                values.insert(p.value);
                break;
            }
        }

        entry->unlock();
    }
}

} // namespace tagtree
