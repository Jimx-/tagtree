#ifndef _TAGTREE_INDEX_TREE_H_
#define _TAGTREE_INDEX_TREE_H_

#include "bptree/tree.h"
#include "promql/labels.h"
#include "tagtree/index/series_manager.h"
#include "tagtree/index/string_key.h"
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
    IndexTree(IndexServer* server);

    TSID add_series(const std::vector<promql::Label>& labels);
    void
    resolve_label_matchers(const std::vector<promql::LabelMatcher>& matcher,
                           std::unordered_set<TSID>& tsids);
    bool get_labels(const TSID& tsid, std::vector<promql::Label>& labels);

private:
    static const size_t NAME_BYTES = 4;
    static const size_t VALUE_BYTES = 4;
    static constexpr size_t KEY_WIDTH = NAME_BYTES + VALUE_BYTES;

    // using KeyType = KeyTypeSelector<KEY_WIDTH>::key_type;
    using KeyType = std::conditional<KEY_WIDTH <= sizeof(uint64_t), uint64_t,
                                     StringKey<KEY_WIDTH>>::type;
    using BPTree = bptree::BTree<200, KeyType, bptree::PageID>;

    IndexServer* server;
    SeriesManager series_manager;
    std::atomic<PostingID> next_id;
    std::mutex tree_mutex;
    std::unique_ptr<BPTree> btree;

    PostingID get_new_id();

    void insert_label(const promql::Label& label, PostingID pid);
    void insert_posting_id(const KeyType& key, PostingID pid);

    void query_postings(const promql::LabelMatcher& matcher,
                        std::set<PostingID>& posting_ids);

    KeyType make_key(const std::string& name, const std::string& value);

    void _hash_string_name(const std::string& str, uint8_t* out);
    void _hash_string_value(const std::string& str, uint8_t* out);

    template <typename K> void pack_key(const uint8_t* key_buf, K& key);
    template <typename K> void clear_key(K& key);
};

} // namespace tagtree

#endif
