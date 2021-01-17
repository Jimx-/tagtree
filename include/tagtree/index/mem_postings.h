#ifndef _TAGTREE_MEM_POSTINGS_H_
#define _TAGTREE_MEM_POSTINGS_H_

#include "tagtree/tsid.h"

#include "roaring.hh"

#include <set>

namespace tagtree {

struct MemPostings {
    Roaring bitmap;
    std::set<std::tuple<uint64_t, TSID>> series_set;

    void add(TSID tsid, uint64_t timestamp)
    {
        bitmap.add(tsid);
        series_set.insert(std::tie(timestamp, tsid));
    }
};

} // namespace tagtree

#endif
