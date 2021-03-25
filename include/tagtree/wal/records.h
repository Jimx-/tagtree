#ifndef _TAGTREE_WAL_RECORDS_H_
#define _TAGTREE_WAL_RECORDS_H_

#include "promql/labels.h"
#include "tagtree/tsid.h"

namespace tagtree {

enum LogRecordType {
    LRT_NONE = 0,
    LRT_SERIES,
};

struct SeriesRef {
    TSID tsid;
    std::vector<promql::Label> labels;
    uint64_t timestamp;

    SeriesRef() {}

    SeriesRef(TSID tsid, const std::vector<promql::Label> labels, uint64_t t)
        : tsid(tsid), labels(labels), timestamp(t)
    {}
};

} // namespace tagtree

#endif
