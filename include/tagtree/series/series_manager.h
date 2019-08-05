#ifndef _TAGTREE_SERIES_MANAGER_H_
#define _TAGTREE_SERIES_MANAGER_H_

#include "promql/labels.h"
#include "tagtree/tsid.h"

#include <memory>
#include <unordered_map>

namespace tagtree {

struct SeriesEntry {
    TSID tsid;
    std::vector<promql::Label> labels;

    SeriesEntry(const std::vector<promql::Label>& labels) : labels(labels) {}
    SeriesEntry(const TSID& tsid, const std::vector<promql::Label>& labels = {})
        : tsid(tsid), labels(labels)
    {}
};

class AbstractSeriesManager {
public:
    virtual SeriesEntry* add(const TSID& tsid,
                             const std::vector<promql::Label>& labels) = 0;
    virtual SeriesEntry* get(const TSID& tsid) = 0;
};

} // namespace tagtree

#endif
