#ifndef _TAGTREE_SWIG_WRAPPER_H_
#define _TAGTREE_SWIG_WRAPPER_H_

#include "tagtree/index/index_server.h"
#include "tagtree/series/series_file_manager.h"

tagtree::AbstractSeriesManager* CreateSeriesFileManager(size_t cache_size,
                                                        const std::string& dir,
                                                        size_t segment_size);

class IndexServerWrapper {
public:
    IndexServerWrapper(const std::string& dir, size_t cache_size,
                       tagtree::AbstractSeriesManager* sm);

    std::pair<tagtree::SeriesRef, bool>
    AddSeries(unsigned long t, const std::vector<promql::Label>& labels);

    void CommitBatch(const std::vector<tagtree::SeriesRef>& refs);

    void SeriesLabels(unsigned long tsid, std::vector<promql::Label>& labels);

    void ResolveLabelMatchers(const std::vector<promql::LabelMatcher>& matchers,
                              unsigned long mint, unsigned long maxt,
                              std::vector<unsigned long>& tsids);

private:
    tagtree::IndexServer server;
};

IndexServerWrapper* CreateIndexServer(const std::string& dir, size_t cache_size,
                                      tagtree::AbstractSeriesManager* sm);

#endif
