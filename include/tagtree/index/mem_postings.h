#ifndef _TAGTREE_MEM_POSTINGS_H_
#define _TAGTREE_MEM_POSTINGS_H_

#include "tagtree/tsid.h"

#include "roaring.hh"

#include <set>

namespace tagtree {

struct MemPostings {
    Roaring bitmap;
    std::set<std::tuple<uint64_t, TSID>> series_set;
    uint64_t min_timestamp, max_timestamp;

    MemPostings() : min_timestamp(UINT64_MAX), max_timestamp(0) {}

    void add(TSID tsid, uint64_t timestamp)
    {
        bitmap.add(tsid);
        series_set.insert(std::tie(timestamp, tsid));
        min_timestamp = std::min(min_timestamp, timestamp);
        max_timestamp = std::max(max_timestamp, timestamp);
    }
};

} // namespace tagtree

#endif
