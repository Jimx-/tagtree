#include "tagtree/index/index_tree.h"
#include "bptree/heap_page_cache.h"
#include "tagtree/index/index_server.h"

#include <cassert>
#include <iomanip>

using promql::MatchOp;

namespace tagtree {

template <>
void IndexTree::pack_key<uint64_t>(const uint8_t* buf, uint64_t& key)
{
    key = 0;
    for (int i = KEY_WIDTH - 1; i >= 0; i--) {
        key |= ((uint64_t)(*buf++) << (i << 3));
    }
}

template <>
void IndexTree::pack_key<StringKey<IndexTree::KEY_WIDTH>>(
    const uint8_t* buf, StringKey<IndexTree::KEY_WIDTH>& key)
{
    key = StringKey<KEY_WIDTH>(buf);
}

template <> void IndexTree::clear_key<uint64_t>(uint64_t& key) { key = 0; }
template <>
void IndexTree::clear_key<StringKey<IndexTree::KEY_WIDTH>>(
    StringKey<IndexTree::KEY_WIDTH>& key)
{
    key = StringKey<KEY_WIDTH>();
}

IndexTree::IndexTree(std::string_view dir, size_t cache_size)
    : page_cache(
          std::make_unique<bptree::HeapPageCache>(dir, true, cache_size)),
      btree(std::make_unique<BPTree>(page_cache.get()))
{}

void IndexTree::query_postings(const promql::LabelMatcher& matcher,
                               std::unordered_set<TSID>& postings)
{
    KeyType start_key, end_key, match_key, name_mask;
    uint8_t key_buf[KEY_WIDTH];
    auto op = matcher.op;
    auto name = matcher.name;
    auto value = matcher.value;

    postings.clear();
    clear_key(start_key);
    match_key = make_key(name, value);

    memset(key_buf, 0, sizeof(key_buf));
    memset(key_buf, 0xff, NAME_BYTES);
    pack_key(key_buf, name_mask);

    switch (op) {
    case MatchOp::EQL:
        /* key range: from   | hash(name) | hash(value)     | *
         *              to   | hash(name) | hash(value) + 1 | */
        start_key = make_key(name, value);
        break;
    case MatchOp::NEQ:
        /* key range: from   | hash(name)     | 0 | *
         *              to   | hash(name) + 1 | 0 | */
    case MatchOp::LSS:
        /* key range: from   | hash(name) |      0      | *
         *              to   | hash(name) | hash(value) | */
    case MatchOp::GTR:
        /* key range: from   | hash(name)     | hash(value) | *
         *              to   | hash(name) + 1 |      0      | */
    case MatchOp::LTE:
        /* key range: from   | hash(name) |      0      |             *
         *              to   | hash(name) | hash(value) | (inclusive) */
    case MatchOp::GTE:
        /* key range: from   | hash(name)     | hash(value) | (inclusive) *
         *              to   | hash(name) + 1 |      0      |             */
    case MatchOp::EQL_REGEX:
    case MatchOp::NEQ_REGEX:
        /* key range: from   | hash(name)     | 0 | *
         *              to   | hash(name) + 1 | 0 | */
        {
            start_key = make_key(name, value);

            memset(key_buf, 0, sizeof(key_buf));
            key_buf[NAME_BYTES - 1] = 1;
            pack_key(key_buf, end_key);
            end_key = end_key + start_key;

            start_key = start_key & name_mask;
            end_key = end_key & name_mask;

            if (op == MatchOp::LSS || op == MatchOp::LTE) {
                end_key = match_key;
            } else if (op == MatchOp::GTR || op == MatchOp::GTE) {
                start_key = match_key;
            }
            break;
        }
    default:
        break;
    }

    auto it = btree->begin(start_key);
    while (it != btree->end()) {
        switch (op) {
        case MatchOp::EQL:
            if (it->first != start_key) {
                /* no match */
                goto out;
            }
            break;
        case MatchOp::NEQ:
            if (it->first >= end_key) {
                goto out;
            } else if (it->first == match_key) {
                it++;
                continue;
            }

            break;
        case MatchOp::GTR:
            if (it->first == start_key) {
                it++;
                continue;
            }
            /* fallthrough */
        case MatchOp::LSS:
        case MatchOp::GTE:
            if (it->first >= end_key) {
                goto out;
            }
            break;
        case MatchOp::LTE:
            if (it->first > end_key) {
                goto out;
            }
            break;
        case MatchOp::EQL_REGEX:
        case MatchOp::NEQ_REGEX:
            if (it->first >= end_key) {
                goto out;
            }
            break;
        default:
            break;
        }

        auto page_id = it->second;
        auto page = page_cache->fetch_page(page_id);
        char* p = (char*)page->lock();

        uint32_t name_len = *(uint32_t*)p;
        p += sizeof(uint32_t);
        uint32_t value_len = *(uint32_t*)p;
        p += sizeof(uint32_t);
        uint32_t num_postings = *(uint32_t*)p;
        p += sizeof(uint32_t);

        std::string name(p, p + name_len);
        p += name_len;
        std::string value(p, p + value_len);
        p += value_len;
        promql::Label label(name, value);

        if (!matcher.match(label)) {
            page->unlock();
            page_cache->unpin_page(page, true);
            it++;
            continue;
        }

        while (num_postings--) {
            TSID tsid;
            tsid.deserialize(p);
            postings.insert(tsid);
            p += sizeof(TSID);
        }

        page->unlock();
        page_cache->unpin_page(page, true);

        it++;
    }

out:
    return;
}

void IndexTree::resolve_label_matchers(
    const std::vector<promql::LabelMatcher>& matchers,
    std::unordered_set<TSID>& tsids)
{
    bool first = true;
    for (auto&& p : matchers) {
        std::unordered_set<TSID> postings;
        query_postings(p, postings);

        if (postings.empty()) {
            tsids.clear();
            return;
        }

        if (first) {
            tsids = std::move(postings);
            first = false;
        } else {
            auto it1 = tsids.begin();

            for (auto it = tsids.begin(); it != tsids.end();) {
                if (postings.find(*it) == postings.end()) {
                    tsids.erase(it++);
                } else {
                    ++it;
                }
            }
        }
    }
}

void IndexTree::add_series(const TSID& tsid,
                           const std::vector<promql::Label>& labels)
{
    for (auto&& label : labels) {
        insert_posting_id(label, tsid);
    }
}

void IndexTree::insert_posting_id(const promql::Label& label, const TSID& tsid)
{
    auto key = make_key(label.name, label.value);
    std::vector<bptree::PageID> page_ids;
    btree->get_value(key, page_ids);

    if (page_ids.empty()) {
        /* create page */
        auto page = page_cache->new_page();
        char* buf = (char*)page->lock();

        memset(buf, 0, page->get_size());
        *(uint32_t*)buf = (uint32_t)label.name.length();
        buf += sizeof(uint32_t);
        *(uint32_t*)buf = (uint32_t)label.value.length();
        buf += sizeof(uint32_t);
        *(uint32_t*)buf = 0;
        buf += sizeof(uint32_t);
        memcpy(buf, label.name.c_str(), label.name.length());
        buf += label.name.length();
        memcpy(buf, label.value.c_str(), label.value.length());

        page->unlock();
        page_cache->unpin_page(page, true);

        btree->insert(key, page->get_id());
        page_ids.push_back(page->get_id());
    }

    /* update pages */
    for (auto&& pid : page_ids) {
        auto page = page_cache->fetch_page(pid);
        assert(page != nullptr);

        char* buf = (char*)page->lock();
        char* p = buf;

        uint32_t name_len = *(uint32_t*)p;
        p += sizeof(uint32_t);
        uint32_t value_len = *(uint32_t*)p;
        p += sizeof(uint32_t);
        uint32_t num_postings = *(uint32_t*)p;
        p += sizeof(uint32_t);

        std::string name(p, p + name_len);
        p += name_len;
        std::string value(p, p + value_len);
        p += value_len;

        if (name != label.name || value != label.value) {
            page->unlock();
            page_cache->unpin_page(page, true);
            continue;
        }
        tsid.serialize(p + sizeof(TSID) * num_postings);
        *((uint32_t*)buf + 2) = num_postings + 1;

        page->unlock();
        page_cache->unpin_page(page, true);
    }
}

IndexTree::KeyType IndexTree::make_key(const std::string& name,
                                       const std::string& value)
{
    uint8_t key_buf[KEY_WIDTH];
    memset(key_buf, 0, sizeof(key_buf));

    _hash_string_name(name, key_buf);
    _hash_string_value(value, &key_buf[NAME_BYTES]);

    KeyType key;
    pack_key(key_buf, key);

    return key;
}

void IndexTree::_hash_string_name(const std::string& str, uint8_t* out)
{
    /* take LSBs of string hash value */
    auto str_hash = std::hash<std::string>()(str);

    for (int j = NAME_BYTES - 1; j >= 0; j--) {
        *out++ = ((uint64_t)str_hash >> (j << 3)) & 0xff;
    }
}

void IndexTree::_hash_string_value(const std::string& str, uint8_t* out)
{
    /* | 4 bytes string prefix | 2 bytes hash value | */
    int padding = VALUE_BYTES - 2 - str.length();

    for (int i = 0; i < ((VALUE_BYTES - 2) > str.length() ? str.length()
                                                          : (VALUE_BYTES - 2));
         i++) {
        *out++ = str[i];
    }
    while (padding-- > 0) {
        *out++ = '\0';
    }

    auto str_hash = std::hash<std::string>()(str);
    *out++ = (str_hash >> 8) & 0xff;
    *out++ = str_hash & 0xff;
}

} // namespace tagtree
