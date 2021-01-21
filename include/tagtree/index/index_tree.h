#ifndef _TAGTREE_INDEX_TREE_H_
#define _TAGTREE_INDEX_TREE_H_

#ifdef _TAGTREE_USE_AVX2_
#include "tagtree/index/string_key_avx.h"
#else
#include "tagtree/index/string_key.h"
#endif

#include "tagtree/index/tuple_key.h"

#include "bptree/page_cache.h"
#include "bptree/tree.h"
#include "promql/labels.h"
#include "tagtree/index/mem_index.h"
#include "tagtree/series/series_manager.h"
#include "tagtree/tree/cow_tree_node.h"
#include "tagtree/tsid.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <type_traits>
#include <unordered_set>
#include <vector>

namespace tagtree {

class IndexServer;

class IndexTree {
public:
    IndexTree(IndexServer* server, std::string_view filename,
              size_t cache_size);
    ~IndexTree();

    void write_postings(TSID limit,
                        const std::vector<LabeledPostings>& labeled_postings);

    void
    resolve_label_matchers(const std::vector<promql::LabelMatcher>& matcher,
                           Roaring& postings);

    void label_values(const std::string& label_name,
                      std::unordered_set<std::string>& values);

private:
    static const size_t NAME_BYTES = 6;
    static const size_t VALUE_BYTES = 8;
    static const size_t SEGSEL_BYTES = 2;

    using KeyType = TupleKey<NAME_BYTES, VALUE_BYTES>;
    using COWTreeType = tagtree::COWTree<100, KeyType, bptree::PageID>;

    IndexServer* server;
    std::unique_ptr<bptree::AbstractPageCache> page_cache;
    COWTreeType cow_tree;
    size_t postings_per_page;

    inline unsigned int tsid_segsel(TSID tsid)
    {
        return tsid / postings_per_page;
    }

    size_t read_page_metadata(const uint8_t* buf, promql::Label& label);
    size_t write_page_metadata(uint8_t* buf, const promql::Label& label);

    /* Create a posting page and fill in the metadata.
     * The new page is locked when returned */
    bptree::Page* create_posting_page(const promql::Label& label,
                                      boost::upgrade_lock<bptree::Page>& lock);

    bptree::PageID write_posting_page(const std::string& name,
                                      const std::string& value,
                                      unsigned int segsel,
                                      const RoaringSetBitForwardIterator& first,
                                      const RoaringSetBitForwardIterator& last,
                                      bool& updated);

    void
    query_postings(const promql::LabelMatcher& matcher,
                   std::map<unsigned int, std::unique_ptr<uint8_t[]>>& bitmaps,
                   const std::set<unsigned int>& seg_mask);

    KeyType make_key(const std::string& name, const std::string& value,
                     unsigned int segsel);

    void _hash_string_name(const std::string& str, uint8_t* out);
    void _hash_string_value(const std::string& str, uint8_t* out);
    void _hash_segsel(unsigned int segsel, uint8_t* out);
};

} // namespace tagtree

#endif
