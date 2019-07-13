#include "tagtree/index/index_server.h"
#include "bptree/mem_page_cache.h"

#include <sstream>
#include <unordered_set>

using promql::MatchOp;

namespace tagtree {

IndexServer::IndexServer()
    : page_cache(std::make_unique<bptree::MemPageCache>(4096)), index_tree(this)
{}

void IndexServer::label_values(const std::string& label_name,
                               std::unordered_set<std::string>& values)
{
    std::unordered_set<TSID> tsids;
    std::vector<promql::Label> labels;
    index_tree.resolve_label_matchers({{MatchOp::NEQ, label_name, ""}}, tsids);

    for (auto&& tsid : tsids) {
        labels.clear();
        index_tree.get_labels(tsid, labels);

        for (auto&& p : labels) {
            if (p.name == label_name) {
                values.insert(p.value);
                break;
            }
        }
    }
}

} // namespace tagtree
