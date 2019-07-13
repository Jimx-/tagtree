#ifndef _TAGTREE_PROM_QUERIER_H_
#define _TAGTREE_PROM_QUERIER_H_

#include "promql/storage.h"
#include "tagtree/storage.h"

namespace tagtree {
namespace prom {

class IndexedStorage;

class PromQuerier : public promql::Querier {
public:
    PromQuerier(IndexedStorage* parent, uint64_t mint, uint64_t maxt);

    virtual std::shared_ptr<promql::SeriesSet>
    select(const std::vector<promql::LabelMatcher>& matchers);

private:
    IndexedStorage* parent;
    std::shared_ptr<tagtree::Querier> querier;
};

class PromSeriesIterator : public promql::SeriesIterator {
public:
    PromSeriesIterator(std::unique_ptr<tagtree::SeriesIterator> si)
        : si(std::move(si))
    {}
    virtual bool seek(uint64_t t) { return si->seek(t); }
    virtual std::pair<uint64_t, double> at() { return si->at(); };
    virtual bool next() { return si->next(); }

private:
    std::unique_ptr<tagtree::SeriesIterator> si;
};

class PromSeries : public promql::Series {
public:
    PromSeries(IndexedStorage* parent, std::shared_ptr<tagtree::Series> series)
        : series(series), parent(parent)
    {}
    virtual void labels(std::vector<promql::Label>& labels);
    virtual std::unique_ptr<promql::SeriesIterator> iterator()
    {
        return std::make_unique<PromSeriesIterator>(series->iterator());
    }

private:
    IndexedStorage* parent;
    std::shared_ptr<tagtree::Series> series;
};

class PromSeriesSet : public promql::SeriesSet {
public:
    PromSeriesSet(IndexedStorage* parent,
                  std::shared_ptr<tagtree::SeriesSet> ss)
        : ss(ss), parent(parent)
    {}
    virtual bool next() { return ss->next(); }
    virtual std::shared_ptr<promql::Series> at()
    {
        auto series = ss->at();
        return std::make_shared<PromSeries>(parent, series);
    }

private:
    IndexedStorage* parent;
    std::shared_ptr<tagtree::SeriesSet> ss;
};

} // namespace prom
} // namespace tagtree

#endif
