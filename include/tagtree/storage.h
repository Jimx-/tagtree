#ifndef _TAGTREE_STORAGE_H_
#define _TAGTREE_STORAGE_H_

#include "tagtree/index/mem_index.h"
#include "tagtree/tsid.h"

#include <cstdint>
#include <memory>
#include <unordered_set>

namespace tagtree {

class SeriesIterator {
public:
    virtual bool seek(uint64_t t) = 0;
    virtual std::pair<uint64_t, double> at() = 0;
    virtual bool next() = 0;
};

class Series {
public:
    virtual TSID tsid() = 0;
    virtual std::unique_ptr<SeriesIterator> iterator() = 0;
};

class SeriesSet {
public:
    virtual bool next() = 0;
    virtual std::shared_ptr<Series> at() = 0;
};

class Querier {
public:
    virtual std::shared_ptr<SeriesSet> select(const MemPostingList& tsids) = 0;
};

class Queryable {
public:
    virtual std::shared_ptr<Querier> querier(uint64_t mint, uint64_t maxt) = 0;
};

class Appender {
public:
    virtual void add(TSID tsid, uint64_t t, double v) = 0;

    virtual void commit() {}
};

class Storage : public Queryable {
public:
    virtual std::shared_ptr<Appender> appender() = 0;
    virtual void close() {}
};

} // namespace tagtree

#endif
