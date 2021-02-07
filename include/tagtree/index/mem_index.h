#ifndef _TAGTREE_MEM_INDEX_H_
#define _TAGTREE_MEM_INDEX_H_

#include "promql/labels.h"
#include "tagtree/index/mem_postings.h"
#include "tagtree/tsid.h"

#include "roaring.hh"

#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace tagtree {

using MemPostingList = Roaring;

struct LabeledPostings {
    std::string value;
    Roaring postings;
    uint64_t min_timestamp, max_timestamp;

    LabeledPostings(const std::string& value, uint64_t min_timestamp,
                    uint64_t max_timestamp)
        : value(value), min_timestamp(min_timestamp),
          max_timestamp(max_timestamp)
    {}
};

using MemIndexSnapshot =
    std::unordered_map<std::string, std::vector<LabeledPostings>>;

class MemIndex {
public:
    MemIndex(size_t capacity = 512);

    bool add(const std::vector<promql::Label>& labels, TSID tsid,
             uint64_t timestamp);

    void
    resolve_label_matchers(const std::vector<promql::LabelMatcher>& matchers,
                           MemPostingList& tsids);

    void label_values(const std::string& label_name,
                      std::unordered_set<std::string>& values);

    void set_low_watermark(TSID wm, bool force = false);
    void snapshot(TSID limit, MemIndexSnapshot& snapshot);

    void gc();

private:
    using MemMapType =
        std::unordered_map<std::string,
                           std::unordered_map<std::string, MemPostings>>;

    MemMapType map;
    std::shared_mutex mutex;
    TSID low_watermark;

    static const TSID NO_LIMIT = UINT64_MAX;
    TSID current_limit;

    void add_label(const promql::Label& label, TSID tsid, uint64_t timestamp);

    void resolve_label_matchers_unsafe(
        const std::vector<promql::LabelMatcher>& matchers,
        MemPostingList& tsids);

    void get_matcher_postings(const promql::LabelMatcher& matcher,
                              MemPostingList& tsids);
};

} // namespace tagtree

#endif
