#include "tagtree/swig/wrapper.h"

#include <chrono>
#include <iostream>
typedef std::chrono::high_resolution_clock Clock;
using namespace tagtree;

#define ACC_TIME(var, t1, t2) \
    var +=                    \
        std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()

AbstractSeriesManager* CreateSeriesFileManager(size_t cache_size,
                                               const std::string& dir,
                                               size_t segment_size)
{
    return new SeriesFileManager(cache_size, dir, segment_size);
}

IndexServerWrapper::IndexServerWrapper(const std::string& dir,
                                       size_t cache_size,
                                       tagtree::AbstractSeriesManager* sm)
    : server(dir, cache_size, sm, true)
{}

void IndexServerWrapper::PrintStats()
{
    std::cout << "add_series_time,commit_batch_time,series_labels_time,resolve_"
                 "label_time,compact_time,sum"
              << std::endl;
    std::cout << add_series_time << "," << commit_batch_time << ","
              << series_labels_time << "," << resolve_label_time << ","
              << compact_time << ","
              << add_series_time + commit_batch_time + series_labels_time +
                     resolve_label_time + compact_time
              << std::endl;
}

IndexServerWrapper* CreateIndexServer(const std::string& dir, size_t cache_size,
                                      tagtree::AbstractSeriesManager* sm)
{
    return new IndexServerWrapper(dir, cache_size, sm);
}

std::pair<SeriesRef, bool>
IndexServerWrapper::AddSeries(long t, const std::vector<promql::Label>& labels)
{
    auto t1 = Clock::now();
    auto p = server.add_series(t, labels);
    auto t2 = Clock::now();
    ACC_TIME(add_series_time, t1, t2);

    return {{p.first, labels, t}, p.second};
}

void IndexServerWrapper::CommitBatch(const std::vector<SeriesRef>& refs)
{
    auto t1 = Clock::now();
    server.commit(refs);
    auto t2 = Clock::now();
    ACC_TIME(commit_batch_time, t1, t2);
}

void IndexServerWrapper::SeriesLabels(unsigned long tsid,
                                      std::vector<promql::Label>& labels)
{
    auto t1 = Clock::now();
    server.get_labels(tsid, labels);
    auto t2 = Clock::now();
    ACC_TIME(series_labels_time, t1, t2);
}

void IndexServerWrapper::ResolveLabelMatchers(
    const std::vector<promql::LabelMatcher>& matchers, long mint, long maxt,
    std::vector<unsigned long>& tsids)
{
    MemPostingList bitmap;

    auto t1 = Clock::now();
    server.resolve_label_matchers(matchers, mint, maxt, bitmap);
    auto t2 = Clock::now();
    ACC_TIME(resolve_label_time, t1, t2);

    tsids.clear();
    tsids.reserve(bitmap.cardinality());
    for (auto&& i : bitmap) {
        tsids.emplace_back(i);
    }
}

void IndexServerWrapper::ManualCompact()
{
    auto t1 = Clock::now();
    server.manual_compact();
    auto t2 = Clock::now();
    ACC_TIME(compact_time, t1, t2);
}
