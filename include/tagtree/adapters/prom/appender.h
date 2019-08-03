#ifndef _TAGTREE_PROM_APPENDER_H_
#define _TAGTREE_PROM_APPENDER_H_

#include "promql/storage.h"
#include "tagtree/storage.h"

namespace tagtree {
namespace prom {

class IndexedStorage;

class PromAppender : public promql::Appender {
public:
    PromAppender(IndexedStorage* parent);
    virtual void add(const std::vector<promql::Label>& labels, uint64_t t,
                     double v);

    virtual void commit();

private:
    IndexedStorage* parent;
    std::shared_ptr<tagtree::Appender> app;
};

} // namespace prom
} // namespace tagtree

#endif
