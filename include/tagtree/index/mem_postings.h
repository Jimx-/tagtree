#ifndef _TAGTREE_MEM_POSTINGS_H_
#define _TAGTREE_MEM_POSTINGS_H_

#include "tagtree/tsid.h"

#include "roaring.hh"

#include <atomic>
#include <set>

namespace tagtree {

struct MemPostings {
    Roaring bitmap;
    uint64_t min_timestamp, next_timestamp;
    std::atomic<uint64_t> max_timestamp;

    MemPostings()
        : min_timestamp(UINT64_MAX), max_timestamp(0),
          next_timestamp(UINT64_MAX)
    {}

    void add(TSID tsid, uint64_t timestamp, bool set_next)
    {
        bitmap.add(tsid);

        if (set_next)
            next_timestamp = std::min(next_timestamp, timestamp);
        else
            min_timestamp = std::min(min_timestamp, timestamp);

        max_timestamp.store(std::max(max_timestamp.load(), timestamp));
    }

    void touch(uint64_t timestamp)
    {
        uint64_t prev_value = max_timestamp;
        while (prev_value < timestamp &&
               !max_timestamp.compare_exchange_weak(prev_value, timestamp)) {
        }
    }
};

} // namespace tagtree

#endif
