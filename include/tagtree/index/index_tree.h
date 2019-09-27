#ifndef _TAGTREE_INDEX_TREE_H_
#define _TAGTREE_INDEX_TREE_H_

#ifdef _TAGTREE_USE_AVX2_
#include "tagtree/index/string_key_avx.h"
#else
#include "tagtree/index/string_key.h"
#endif

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

/* use 64-bit unsigned integer as key when possible */
template <size_t N, typename Enable = void> struct KeyTypeSelector;
template <size_t N>
struct KeyTypeSelector<N,
                       typename std::enable_if<N <= sizeof(uint64_t)>::type> {
    typedef uint64_t key_type;
};

template <size_t N>
struct KeyTypeSelector<
    N, typename std::enable_if<!(N <= sizeof(uint64_t))>::type> {
    typedef StringKey<N> key_type;
};

class IndexTree {
public:
    IndexTree(IndexServer* server, std::string_view dir, size_t cache_size);

    void write_postings(const std::vector<LabeledPostings>& labeled_postings);

    void
    resolve_label_matchers(const std::vector<promql::LabelMatcher>& matcher,
                           Roaring& postings);

private:
    static const size_t NAME_BYTES = 4;
    static const size_t VALUE_BYTES = 6;
    static const size_t SEGSEL_BYTES = 2;
    static constexpr size_t KEY_WIDTH = NAME_BYTES + VALUE_BYTES + SEGSEL_BYTES;

    // using KeyType = KeyTypeSelector<KEY_WIDTH>::key_type;
    using KeyType = std::conditional<KEY_WIDTH <= sizeof(uint64_t), uint64_t,
                                     StringKey<KEY_WIDTH>>::type;
    using COWTreeType = tagtree::COWTree<200, KeyType, bptree::PageID>;

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

    template <typename K> void pack_key(const uint8_t* key_buf, K& key);
    template <typename K> unsigned int get_segsel(const K& key);
    template <typename K> void clear_key(K& key);
};

} // namespace tagtree

#endif
