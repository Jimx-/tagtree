#ifndef _TAGTREE_MEM_INDEX_H_
#define _TAGTREE_MEM_INDEX_H_

#include "promql/labels.h"
#include "tagtree/tsid.h"

#include "roaring.hh"

#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace tagtree {

using MemPostingList = Roaring;

struct LabeledPostings {
    promql::Label label;
    MemPostingList postings;

    LabeledPostings(const std::string& name, const std::string& value)
        : label(name, value)
    {}
};

class MemIndex {
public:
    MemIndex(size_t capacity = 512);

    bool add(const std::vector<promql::Label>& labels, TSID tsid);

    void
    resolve_label_matchers(const std::vector<promql::LabelMatcher>& matchers,
                           MemPostingList& tsids);

    void set_low_watermark(TSID wm);
    void snapshot(TSID limit, std::vector<LabeledPostings>& labeled_postings);

    void gc();

private:
    using MemMapType =
        std::unordered_map<std::string,
                           std::unordered_map<std::string, MemPostingList>>;

    MemMapType map;
    std::shared_mutex mutex;
    TSID low_watermark;

    void add_label(const promql::Label& label, TSID tsid);

    void resolve_label_matchers_unsafe(
        const std::vector<promql::LabelMatcher>& matchers,
        MemPostingList& tsids);
};

} // namespace tagtree

#endif
