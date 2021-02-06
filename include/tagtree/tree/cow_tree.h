#ifndef _TAGTREE_COW_TREE_H_
#define _TAGTREE_COW_TREE_H_

#include "bptree/page_cache.h"
#include "bptree/serializer.h"

#include "CRC.h"

#include <cassert>
#include <iostream>
#include <shared_mutex>
#include <unordered_map>

namespace tagtree {

class TransactionAborted : public std::exception {};

template <unsigned int N, typename K, typename V, typename KeySerializer,
          typename KeyComparator, typename KeyEq, typename ValueSerializer>
class BaseCOWNode;
template <unsigned int N, typename K, typename V, typename KeySerializer,
          typename KeyComparator, typename KeyEq, typename ValueSerializer>
class InnerCOWNode;
template <unsigned int N, typename K, typename V, typename KeySerializer,
          typename KeyComparator, typename KeyEq, typename ValueSerializer>
class LeafCOWNode;

template <unsigned int N, typename K, typename V,
          typename KeySerializer = bptree::CopySerializer<K>,
          typename KeyComparator = std::less<K>,
          typename KeyEq = std::equal_to<K>,
          typename ValueSerializer = bptree::CopySerializer<V>>
class COWTree {
public:
    typedef uint32_t Version;
    static const Version LATEST_VERSION = 0;

    using TreeType =
        COWTree<N, K, V, KeySerializer, KeyComparator, KeyEq, ValueSerializer>;
    using BaseNodeType = BaseCOWNode<N, K, V, KeySerializer, KeyComparator,
                                     KeyEq, ValueSerializer>;

    class Transaction {
        friend TreeType;

    public:
        Transaction() : tree(nullptr), old_version(0) {}
        Transaction(TreeType* tree, Version old_version, BaseNodeType* root)
            : tree(tree), old_version(old_version)
        {}

        template <typename T, typename std::enable_if<std::is_base_of<
                                  BaseNodeType, T>::value>::type* = nullptr>
        std::shared_ptr<T> create_node(BaseNodeType* parent)
        {
            std::shared_ptr<T> new_node = tree->create_node<T>(parent);
            new_nodes.emplace_back(new_node);
            return new_node;
        }

    private:
        TreeType* tree;
        Version old_version;
        std::shared_ptr<BaseNodeType> new_root;
        std::vector<std::shared_ptr<BaseNodeType>> new_nodes;
    };

    COWTree(bptree::AbstractPageCache* page_cache) : page_cache(page_cache)
    {
        auto created = !read_metadata();

        if (created) {
            {
                boost::upgrade_lock<bptree::Page> lock;
                auto page = page_cache->new_page(lock);
                assert(page->get_id() == META_PAGE_ID);
            }

            latest_version.store(1);
            auto new_root =
                create_node<LeafCOWNode<N, K, V, KeySerializer, KeyComparator,
                                        KeyEq, ValueSerializer>>(nullptr);
            root_map[1] = new_root;

            metadata_index = 0;

            new_root->set_new_node(false);
            write_node(new_root.get());
            write_metadata(1, new_root->get_pid());
            write_metadata(1, new_root->get_pid());
        }
    }

    template <typename T, typename std::enable_if<std::is_base_of<
                              BaseNodeType, T>::value>::type* = nullptr>
    std::shared_ptr<T> create_node(BaseNodeType* parent)
    {
        boost::upgrade_lock<bptree::Page> lock;
        auto page = page_cache->new_page(lock);
        auto node = std::make_shared<T>(this, parent, page->get_id(), true);
        page_cache->unpin_page(page, false, lock);

        return node;
    }

    void get_value(const K& key, std::vector<V>& value_list,
                   Version version = LATEST_VERSION)
    {
        typename BaseNodeType::ValueListIterator value_first = nullptr,
                                                 value_last = nullptr;
        auto* root = get_read_tree(version);
        root->get_values(key, false, nullptr, nullptr, nullptr, value_first,
                         value_last);
        value_list.assign(value_first, value_last);
    }

    void insert(const K& key, const V& value, Transaction& txn)
    {
        auto root = txn.new_root;

        K split_key;
        bool updated;
        std::shared_ptr<BaseNodeType> new_node, right_sibling;
        std::tie(new_node, right_sibling) =
            root->insert_value(txn, key, value, split_key, false, updated);

        if (new_node) root = std::move(new_node);

        if (right_sibling) {
            auto new_root = txn.template create_node<InnerCOWNode<
                N, K, V, KeySerializer, KeyComparator, KeyEq, ValueSerializer>>(
                nullptr);

            new_root->set_size(1);
            new_root->keys[0] = split_key;
            new_root->child_pages[0] = root->get_pid();
            new_root->child_pages[1] = right_sibling->get_pid();
            new_root->child_cache[0] = std::move(root);
            new_root->child_cache[1] = std::move(right_sibling);

            root = std::move(new_root);
        }

        txn.new_root = std::move(root);
    }

    bool update(const K& key, const V& value, Transaction& txn)
    {
        auto root = txn.new_root;

        K split_key;
        bool updated = false;
        std::shared_ptr<BaseNodeType> new_node, right_sibling;
        std::tie(new_node, right_sibling) =
            root->insert_value(txn, key, value, split_key, true, updated);

        if (new_node) {
            txn.new_root = std::move(new_node);
        }

        return updated;
    }

    void get_write_tree(Transaction& txn)
    {
        auto version = latest_version.load();

        typename RootMapType::iterator it;
        {
            std::shared_lock<std::shared_mutex> lock(root_mutex);
            it = root_map.find(version);
        }

        txn.new_root = it->second;
        txn.tree = this;
        txn.old_version = version;
    }

    Version commit(Transaction& txn)
    {
        if (txn.new_nodes.empty()) {
            /* no changes */
            return latest_version.load();
        }

        if (txn.old_version != latest_version.load()) {
            throw TransactionAborted();
        }

        for (auto&& node : txn.new_nodes) {
            write_node(node.get());
            node->set_new_node(false);
        }
        txn.new_nodes.clear();

        {
            std::unique_lock<std::shared_mutex> lock(root_mutex);
            root_map.emplace(txn.old_version + 1, txn.new_root);
        }
        write_metadata(txn.old_version + 1, txn.new_root->get_pid());

        txn.new_root = nullptr;
        txn.old_version = 0;

        return latest_version.fetch_add(1) + 1;
    }

    std::shared_ptr<BaseNodeType> read_node(BaseNodeType* parent,
                                            bptree::PageID pid)
    {
        boost::upgrade_lock<bptree::Page> lock;
        auto page = page_cache->fetch_page(pid, lock);

        if (!page) {
            return nullptr;
        }
        const auto* buf = page->get_buffer(lock);

        uint32_t tag = *reinterpret_cast<const uint32_t*>(buf);
        std::shared_ptr<BaseNodeType> node;

        if (tag == INNER_TAG) {
            node = std::make_shared<InnerCOWNode<
                N, K, V, KeySerializer, KeyComparator, KeyEq, ValueSerializer>>(
                this, parent, pid, false);
        } else if (tag == LEAF_TAG) {
            node = std::make_shared<LeafCOWNode<
                N, K, V, KeySerializer, KeyComparator, KeyEq, ValueSerializer>>(
                this, parent, pid, false);
        }

        node->deserialize(&buf[sizeof(uint32_t)],
                          page->get_size() - sizeof(uint32_t));

        page_cache->unpin_page(page, false, lock);
        return node;
    }

    void write_node(const BaseNodeType* node)
    {
        boost::upgrade_lock<bptree::Page> lock;
        auto page = page_cache->fetch_page(node->get_pid(), lock);

        {
            boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
            if (!page) return;
            auto* buf = page->get_buffer(ulock);
            uint32_t tag = node->is_leaf() ? LEAF_TAG : INNER_TAG;

            *reinterpret_cast<uint32_t*>(buf) = tag;
            node->serialize(&buf[sizeof(uint32_t)],
                            page->get_size() - sizeof(uint32_t));
        }

        page_cache->unpin_page(page, true, lock);
    }

    void print(std::ostream& os, Version version = LATEST_VERSION)
    {
        auto* root = get_read_tree(version);
        root->print(os);
    }

    friend std::ostream& operator<<(std::ostream& os, COWTree& tree)
    {
        tree.print(os, tree.latest_version.load());
        return os;
    }

private:
    void collect_values(Version version, const K& key,
                        std::optional<K>* next_key,
                        typename BaseNodeType::KeyListIterator& key_first,
                        typename BaseNodeType::KeyListIterator& key_last,
                        typename BaseNodeType::ValueListIterator& value_first,
                        typename BaseNodeType::ValueListIterator& value_last)
    {
        auto* root = get_read_tree(version);
        root->get_values(key, true, next_key, &key_first, &key_last,
                         value_first, value_last);
    }

public:
    /* iterator interface */
    class iterator {
        friend TreeType;

    public:
        using self_type = iterator;
        using value_type = std::pair<K, V>;
        using reference = value_type&;
        using pointer = value_type*;
        using iterator_category = std::forward_iterator_tag;
        using difference_type = int;

        self_type operator++()
        {
            self_type i = *this;
            inc();
            return i;
        }
        self_type operator++(int _unused)
        {
            inc();
            return *this;
        }

        reference operator*() { return kvp; }
        pointer operator->() { return &kvp; }
        bool operator==(const self_type& rhs) { return false; }
        bool operator!=(const self_type& rhs) { return true; }
        bool is_end() const { return ended; }

    private:
        typename BaseNodeType::KeyListIterator key_first;
        typename BaseNodeType::KeyListIterator key_last;
        typename BaseNodeType::ValueListIterator value_first;
        typename BaseNodeType::ValueListIterator value_last;
        value_type kvp;
        std::optional<K> next_key;
        bool ended;
        Version version;
        KeyComparator kcmp;

        using container_type = TreeType;
        container_type* tree;

        iterator(container_type* tree, Version version, const K& key,
                 KeyComparator kcmp = KeyComparator{})
            : tree(tree), kcmp(kcmp), version(version), next_key(std::nullopt)
        {
            ended = false;
            tree->collect_values(version, key, &next_key, key_first, key_last,
                                 value_first, value_last);
            auto it = std::lower_bound(key_first, key_last, key, kcmp);
            if (it == key_last) {
                ended = true;
            } else {
                value_first += std::distance(key_first, it);
                key_first = it;

                kvp = std::make_pair(*key_first, *value_first);
            }
        }

        void inc()
        {
            if (ended) return;
            if (key_first + 1 == key_last) {
                get_next_batch();
            } else {
                key_first++;
                value_first++;
            }
            if (ended) return;
            kvp = std::make_pair(*key_first, *value_first);
        }

        void get_next_batch()
        {
            if (!next_key) {
                ended = true;
                return;
            }

            K key = *next_key;
            next_key = std::nullopt;
            tree->collect_values(version, key, &next_key, key_first, key_last,
                                 value_first, value_last);
            auto it = std::lower_bound(key_first, key_last, key, kcmp);
            if (it == key_last) {
                ended = true;
            } else {
                value_first += std::distance(key_first, it);
                key_first = it;
            }
        }
    };

private:
    struct Sentinel {
        friend bool operator==(iterator const& it, Sentinel)
        {
            return it.is_end();
        }

        template <class Rhs,
                  std::enable_if_t<!std::is_same<Rhs, Sentinel>{}, int> = 0>
        friend bool operator!=(Rhs const& ptr, Sentinel)
        {
            return !(ptr == Sentinel{});
        }
        friend bool operator==(Sentinel, iterator const& it)
        {
            return it.is_end();
        }
        template <class Lhs,
                  std::enable_if_t<!std::is_same<Lhs, Sentinel>{}, int> = 0>
        friend bool operator!=(Sentinel, Lhs const& ptr)
        {
            return !(Sentinel{} == ptr);
        }
        friend bool operator==(Sentinel, Sentinel) { return true; }
        friend bool operator!=(Sentinel, Sentinel) { return false; }
    };

public:
    iterator begin(const K& key, Version version = LATEST_VERSION)
    {
        if (version == LATEST_VERSION) {
            version = latest_version.load();
        }
        return iterator(this, version, key);
    }
    Sentinel end() const { return Sentinel{}; }

private:
    static const bptree::PageID META_PAGE_ID = 1;
    static const bptree::PageID FIRST_NODE_PAGE_ID = META_PAGE_ID + 1;
    static const uint32_t META_PAGE_MAGIC = 0x00C0FFEE;
    static const uint32_t INNER_TAG = 1;
    static const uint32_t LEAF_TAG = 2;

    bptree::AbstractPageCache* page_cache;
    std::atomic<Version> latest_version;
    using RootMapType =
        std::unordered_map<Version, std::shared_ptr<BaseNodeType>>;
    RootMapType root_map;
    std::shared_mutex root_mutex;
    int metadata_index; // for double write

    BaseNodeType* get_read_tree(Version version)
    {
        if (version == LATEST_VERSION) {
            version = latest_version.load();
        }

        typename RootMapType::iterator it;
        {
            std::shared_lock<std::shared_mutex> lock(root_mutex);
            it = root_map.find(version);
        }

        return it->second.get();
    }

    /* metadata: | magic(4 bytes) | root page id(4 bytes) | */
    bool read_metadata()
    {
        boost::upgrade_lock<bptree::Page> lock;
        auto page = page_cache->fetch_page(META_PAGE_ID, lock);
        if (!page) return false;

        const auto* buf = page->get_buffer(lock);
        auto magic = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);

        if (magic != META_PAGE_MAGIC) {
            return false;
        }

        latest_version.store(0, std::memory_order_relaxed);

        bool ok = false;
        metadata_index = 0;
        for (int i = 0; i < 2; i++) {
            const auto metadata_size =
                sizeof(uint32_t) + sizeof(bptree::PageID);
            uint32_t crc = CRC::Calculate(buf, metadata_size, CRC::CRC_32());
            uint32_t crc_read =
                *reinterpret_cast<const uint32_t*>(buf + metadata_size);

            if (crc != crc_read) {
                continue;
            }

            auto version = *reinterpret_cast<const uint32_t*>(buf);
            buf += sizeof(uint32_t);
            auto root_id = *reinterpret_cast<const bptree::PageID*>(buf);
            buf += sizeof(bptree::PageID);
            buf += sizeof(uint32_t);

            auto root_node = read_node(nullptr, root_id);
            root_map.emplace(version, std::move(root_node));

            if (latest_version.load(std::memory_order_relaxed) < version) {
                latest_version.store(version, std::memory_order_relaxed);
                metadata_index = 1 - i;
            }

            ok = true;
        }

        page_cache->unpin_page(page, false, lock);

        return ok;
    }

    void write_metadata(Version version, bptree::PageID root_pid)
    {
        std::shared_lock<std::shared_mutex> root_lock(root_mutex);

        boost::upgrade_lock<bptree::Page> page_lock;
        auto page = page_cache->fetch_page(META_PAGE_ID, page_lock);

        {
            boost::upgrade_to_unique_lock<bptree::Page> ulock(page_lock);
            auto* buf = page->get_buffer(ulock);

            auto magic = *reinterpret_cast<const uint32_t*>(buf);
            if (magic != META_PAGE_MAGIC) {
                *reinterpret_cast<uint32_t*>(buf) = META_PAGE_MAGIC;
            }
            buf += sizeof(uint32_t);

            const auto metadata_size =
                sizeof(uint32_t) + sizeof(bptree::PageID);

            buf += metadata_index * (metadata_size + sizeof(uint32_t));
            const uint8_t* mdp = buf;
            *reinterpret_cast<uint32_t*>(buf) = version;
            buf += sizeof(uint32_t);
            *reinterpret_cast<bptree::PageID*>(buf) = root_pid;
            buf += sizeof(bptree::PageID);
            *reinterpret_cast<uint32_t*>(buf) =
                CRC::Calculate(mdp, metadata_size, CRC::CRC_32());

            metadata_index = 1 - metadata_index;
        }

        page_cache->unpin_page(page, true, page_lock);
    }
};

} // namespace tagtree

#endif
