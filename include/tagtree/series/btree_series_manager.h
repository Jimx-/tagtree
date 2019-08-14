#ifndef _TAGTREE_BTREE_SERIES_MANAGER_H_
#define _TAGTREE_BTREE_SERIES_MANAGER_H_

#include "bptree/page_cache.h"
#include "bptree/tree.h"
#include "tagtree/series/series_manager.h"

#include <unordered_map>

namespace tagtree {

class BTreeSeriesManager : public AbstractSeriesManager {
public:
    BTreeSeriesManager(size_t cache_size, std::string_view filename,
                       std::string_view index_file);

    virtual size_t get_size() const { return btree->size(); }

private:
    static const uint32_t MAGIC = 0x54534553;
    int fd;
    std::string filename;

    using BPTree = bptree::BTree<168, TSID, off_t>;
    std::unique_ptr<bptree::AbstractPageCache> page_cache;
    std::unique_ptr<BPTree> btree;

    void open_db();
    void create_db();

    virtual bool read_entry(RefSeriesEntry* entry);
    virtual void write_entry(RefSeriesEntry* entry);
};

} // namespace tagtree

#endif
