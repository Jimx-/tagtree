#ifndef _TAGTREE_SERIES_MANAGER_H_
#define _TAGTREE_SERIES_MANAGER_H_

#include "promql/labels.h"
#include "tagtree/common.h"
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
    SeriesEntry* add(PostingID pid, const std::vector<promql::Label>& labels);
    SeriesEntry* get(PostingID pid);
    SeriesEntry* get_tsid(const TSID& tsid);

private:
    std::unordered_map<PostingID, std::unique_ptr<SeriesEntry>> series_map;
    std::unordered_map<TSID, SeriesEntry*> tsid_map;
};

} // namespace tagtree

#endif
