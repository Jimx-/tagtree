#include "tagtree/swig/wrapper.h"

#include <iostream>

using namespace tagtree;

AbstractSeriesManager* CreateSeriesFileManager(size_t cache_size,
                                               const std::string& dir,
                                               size_t segment_size)
{
    return new SeriesFileManager(cache_size, dir, segment_size);
}

IndexServerWrapper::IndexServerWrapper(const std::string& dir,
                                       size_t cache_size,
                                       tagtree::AbstractSeriesManager* sm)
    : server(dir, cache_size, sm)
{}

IndexServerWrapper* CreateIndexServer(const std::string& dir, size_t cache_size,
                                      tagtree::AbstractSeriesManager* sm)
{
    return new IndexServerWrapper(dir, cache_size, sm);
}

std::pair<SeriesRef, bool>
IndexServerWrapper::AddSeries(long t, const std::vector<promql::Label>& labels)
{
    auto p = server.add_series(t, labels);

    return {{p.first, labels, t}, p.second};
}

void IndexServerWrapper::CommitBatch(const std::vector<SeriesRef>& refs)
{
    server.commit(refs);
}

void IndexServerWrapper::SeriesLabels(unsigned long tsid,
                                      std::vector<promql::Label>& labels)
{
    server.get_labels(tsid, labels);
}

void IndexServerWrapper::ResolveLabelMatchers(
    const std::vector<promql::LabelMatcher>& matchers, long mint, long maxt,
    std::vector<unsigned long>& tsids)
{
    MemPostingList bitmap;

    server.resolve_label_matchers(matchers, mint, maxt, bitmap);

    tsids.clear();
    tsids.reserve(bitmap.cardinality());
    for (auto&& i : bitmap) {
        tsids.emplace_back(i);
    }
}
