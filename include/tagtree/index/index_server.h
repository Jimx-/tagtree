#ifndef _TAGTREE_INDEX_SERVER_H_
#define _TAGTREE_INDEX_SERVER_H_

#include "tagtree/index/index_tree.h"
#include "tagtree/series/series_manager.h"

#include <atomic>
#include <memory>
#include <unordered_map>

namespace tagtree {

class IndexServer {
public:
    IndexServer(std::string_view index_dir, size_t cache_size,
                AbstractSeriesManager* sm);

    ~IndexServer();

    AbstractSeriesManager* get_series_manager() const { return series_manager; }

    inline void
    resolve_label_matchers(const std::vector<promql::LabelMatcher>& matcher,
                           std::unordered_set<TSID>& tsids)
    {
        index_tree.resolve_label_matchers(matcher, tsids);
    }

    bool get_labels(TSID tsid, std::vector<promql::Label>& labels);

    TSID add_series(const std::vector<promql::Label>& labels);

    void label_values(const std::string& label_name,
                      std::unordered_set<std::string>& values);

private:
    IndexTree index_tree;
    AbstractSeriesManager* series_manager;
    std::atomic<TSID> id_counter;

    inline TSID get_tsid() { return id_counter.fetch_add(1); }
};

} // namespace tagtree

#endif
