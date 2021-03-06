#ifndef _TAGTREE_INDEX_SERVER_H_
#define _TAGTREE_INDEX_SERVER_H_

#include "tagtree/index/index_tree.h"
#include "tagtree/index/mem_index.h"
#include "tagtree/series/series_manager.h"
#include "tagtree/wal/records.h"
#include "tagtree/wal/wal.h"

#include <atomic>
#include <memory>
#include <unordered_map>

namespace tagtree {

enum class CheckpointPolicy {
    NORMAL,
    DISABLED,
    PRINT,
};

class IndexServer {
public:
    IndexServer(std::string_view index_dir, size_t cache_size,
                AbstractSeriesManager* sm, bool bitmap_only = false,
                bool full_cache = true,
                CheckpointPolicy checkpoint_policy = CheckpointPolicy::NORMAL);

    AbstractSeriesManager* get_series_manager() const { return series_manager; }

    void
    resolve_label_matchers(const std::vector<promql::LabelMatcher>& matchers,
                           uint64_t start, uint64_t end, MemPostingList& tsids);

    void exists(const std::vector<promql::Label>& labels, MemPostingList& tsids,
                bool skip_tree = false);

    bool get_labels(TSID tsid, std::vector<promql::Label>& labels);

    std::pair<TSID, bool> add_series(uint64_t t,
                                     const std::vector<promql::Label>& labels);

    void label_values(const std::string& label_name,
                      std::unordered_set<std::string>& values);

    void commit(const std::vector<SeriesRef>& series);

    void manual_compact();

    TSID current_tsid() const { return id_counter.load(); }

private:
    MemIndex mem_index;
    IndexTree index_tree;
    AbstractSeriesManager* series_manager;
    WAL wal;
    std::atomic<TSID> id_counter;
    bool full_cache;
    CheckpointPolicy checkpoint_policy;

    std::mutex compaction_mutex;
    std::atomic<bool> compacting;
    TSID last_compaction_wm;
    uint64_t last_compaction_timestamp;

    inline TSID get_tsid() { return id_counter.fetch_add(1); }

    inline bool compactable(TSID current_id);
    bool try_compact(bool force, bool detach);
    void compact(TSID current_id);

    void replay_wal();
};

} // namespace tagtree

#endif
