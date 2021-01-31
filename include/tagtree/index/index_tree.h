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

    void write_postings(TSID limit, MemIndexSnapshot& snapshot);

    void
    resolve_label_matchers(const std::vector<promql::LabelMatcher>& matcher,
                           uint64_t start, uint64_t end, Roaring& postings);

    void label_values(const std::string& label_name,
                      std::unordered_set<std::string>& values);

private:
    static const size_t NAME_BYTES = 6;
    static const size_t VALUE_BYTES = 8;
    static const size_t SEGSEL_BYTES = 2;

    static const size_t BITMAP_PAGE_OFFSET =
        2 * sizeof(SymbolTable::Ref) + sizeof(uint64_t);

    using KeyType = TupleKey<NAME_BYTES, VALUE_BYTES>;
    using COWTreeType = tagtree::COWTree<100, KeyType, bptree::PageID>;

    struct TreeEntry {
        IndexTree::KeyType key;
        bptree::PageID pid;
        bool updated;

        TreeEntry(IndexTree::KeyType key, bptree::PageID pid, bool updated)
            : key(key), pid(pid), updated(updated)
        {}
    };

    IndexServer* server;
    std::unique_ptr<bptree::AbstractPageCache> page_cache;
    COWTreeType cow_tree;
    size_t postings_per_page;

    inline unsigned int tsid_segsel(TSID tsid)
    {
        return tsid / postings_per_page;
    }

    enum class TreePageType {
        BITMAP,
        SORTED_LIST,
    };

    TreePageType choose_page_type(const std::string& tag_name,
                                  const std::vector<LabeledPostings>& entry);

    size_t read_page_metadata(const uint8_t* buf, promql::Label& label,
                              uint64_t& end_timestamp, TreePageType& type);
    size_t write_page_metadata(uint8_t* buf, const promql::Label& label,
                               uint64_t end_timestamp, TreePageType type);

    /* Create a posting page and fill in the metadata.
     * The new page is locked when returned */
    bptree::Page* create_posting_page(const promql::Label& label,
                                      uint64_t end_timestamp, TreePageType type,
                                      boost::upgrade_lock<bptree::Page>& lock);

    void write_postings_bitmap(TSID limit, const std::string& name,
                               const std::string& value, const Roaring& bitmap,
                               uint64_t min_timestamp, uint64_t max_timestamp,
                               std::vector<TreeEntry>& tree_entries);
    void write_postings_sorted_list(TSID limit, const std::string& name,
                                    const std::vector<LabeledPostings>& entries,
                                    std::vector<TreeEntry>& tree_entries);

    bptree::PageID
    write_posting_page(const std::string& name, const std::string& value,
                       uint64_t start_timestamp, uint64_t end_timestamp,
                       unsigned int segsel,
                       const RoaringSetBitForwardIterator& first,
                       const RoaringSetBitForwardIterator& last, bool& updated);

    void
    query_postings(const promql::LabelMatcher& matcher, uint64_t start,
                   uint64_t end,
                   std::map<unsigned int, std::unique_ptr<uint8_t[]>>& bitmaps,
                   const std::set<unsigned int>& seg_mask);
    void query_postings_sorted_list(
        const promql::LabelMatcher& matcher, uint64_t start, uint64_t end,
        std::map<unsigned int, std::unique_ptr<uint8_t[]>>& bitmaps,
        const std::set<unsigned int>& seg_mask);

    KeyType make_key(const std::string& name, const std::string& value,
                     uint64_t start_time, unsigned int segsel);

    void _hash_string_name(const std::string& str, uint8_t* out);
    void _hash_string_value(const std::string& str, uint8_t* out);
    void _hash_segsel(unsigned int segsel, uint8_t* out);

    void
    copy_to_bitmaps(const Roaring& bitmap,
                    std::map<unsigned int, std::unique_ptr<uint8_t[]>>& bitmaps,
                    const std::set<unsigned int>& seg_mask);
};

} // namespace tagtree

#endif
