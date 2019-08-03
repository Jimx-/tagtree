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
    SeriesEntry(const TSID& tsid, const std::vector<promql::Label>& labels)
        : tsid(tsid), labels(labels)
    {}
};

class SeriesManager {
public:
    SeriesEntry* add(const TSID& tsid,
                     const std::vector<promql::Label>& labels);
    SeriesEntry* get(const TSID& tsid);

private:
    std::unordered_map<TSID, std::unique_ptr<SeriesEntry>> series_map;
};

} // namespace tagtree

#endif
