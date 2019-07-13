#ifndef _TAGTREE_INDEX_SERVER_H_
#define _TAGTREE_INDEX_SERVER_H_

#include "bptree/page_cache.h"
#include "tagtree/index/index_tree.h"
#include "tagtree/index/series_manager.h"

#include <memory>
#include <unordered_map>

namespace tagtree {

class IndexServer {
public:
    IndexServer();

    void
    resolve_label_matchers(const std::vector<promql::LabelMatcher>& matcher,
                           std::unordered_set<TSID>& tsids)
    {
        index_tree.resolve_label_matchers(matcher, tsids);
    }

    bool get_labels(const TSID& tsid, std::vector<promql::Label>& labels)
    {
        return index_tree.get_labels(tsid, labels);
    }

    TSID add_series(const std::vector<promql::Label>& labels)
    {
        return index_tree.add_series(labels);
    }

    void label_values(const std::string& label_name,
                      std::unordered_set<std::string>& values);

    bptree::AbstractPageCache* get_page_cache() { return page_cache.get(); }

private:
    std::unique_ptr<bptree::AbstractPageCache> page_cache;
    IndexTree index_tree;

    PostingID create_series(const std::vector<promql::Label>& labels);
};

} // namespace tagtree

#endif
