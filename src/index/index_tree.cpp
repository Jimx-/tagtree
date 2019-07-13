#include "tagtree/index/index_tree.h"
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

IndexTree::IndexTree(IndexServer* server) : next_id(0), server(server)
{
    btree = std::make_unique<BPTree>(server->get_page_cache());
}

void IndexTree::query_postings(const promql::LabelMatcher& matcher,
                               std::set<PostingID>& postings)
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
    case MatchOp::GTE: {
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
        default:
            break;
        }

        auto page_id = it->second;
        auto page = server->get_page_cache()->fetch_page(page_id);
        uint64_t* p = (uint64_t*)page->lock();
        uint64_t* lim = (uint64_t*)((uint8_t*)p + page->get_size());
        size_t start_index = 0;

        while (p < lim) {
            if (*p > 0) {
                for (uint64_t i = 0; i < 64; i++) {
                    if ((*p) & ((uint64_t)1 << i)) {
                        postings.insert((PostingID)(start_index + i));
                    }
                }
            }

            start_index += 64;
            p++;
        }
        page->unlock();
        server->get_page_cache()->unpin_page(page, true);

        if (op == MatchOp::EQL) {
            break;
        }
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
    std::set<PostingID> posting_ids;
    for (auto&& p : matchers) {
        std::set<PostingID> postings;
        query_postings(p, postings);

        if (postings.empty()) {
            tsids.clear();
            return;
        }

        if (first) {
            posting_ids = std::move(postings);
            first = false;
        } else {
            auto it1 = posting_ids.begin();
            auto it2 = postings.begin();

            while ((it1 != posting_ids.end()) && (it2 != postings.end())) {
                if (*it1 < *it2) {
                    posting_ids.erase(it1++);
                } else if (*it2 < *it1) {
                    ++it2;
                } else {
                    ++it1;
                    ++it2;
                }
            }
            posting_ids.erase(it1, posting_ids.end());
        }
    }

    for (auto&& p : posting_ids) {
        auto* entry = series_manager.get(p);
        tsids.insert(entry->tsid);
    }
}

bool IndexTree::get_labels(const TSID& tsid, std::vector<promql::Label>& labels)
{
    auto* entry = series_manager.get_tsid(tsid);
    if (!entry) return false;
    labels.clear();
    std::copy(entry->labels.begin(), entry->labels.end(),
              std::back_inserter(labels));
    return true;
}

PostingID IndexTree::get_new_id() { return next_id++; }

TSID IndexTree::add_series(const std::vector<promql::Label>& labels)
{
    auto pid = get_new_id();

    for (auto&& label : labels) {
        insert_label(label, pid);
    }

    auto* entry = series_manager.add(pid, labels);
    return entry->tsid;
}

void IndexTree::insert_label(const promql::Label& label, PostingID pid)
{
    auto key = make_key(label.name, label.value);
    insert_posting_id(key, pid);
}

void IndexTree::insert_posting_id(const KeyType& key, PostingID pid)
{
    std::vector<bptree::PageID> page_ids;
    btree->get_value(key, page_ids);

    if (page_ids.empty()) {
        /* create page */
        auto page = server->get_page_cache()->new_page();
        auto buf = page->lock();
        memset(buf, 0, page->get_size());
        page->unlock();
        server->get_page_cache()->unpin_page(page, true);
        btree->insert(key, page->get_id());
        page_ids.push_back(page->get_id());
    }

    /* update pages */
    for (auto&& p : page_ids) {
        auto page = server->get_page_cache()->fetch_page(p);
        assert(page != nullptr);

        uint64_t* buf = (uint64_t*)page->lock();
        buf[pid >> 6] |= (uint64_t)1 << (pid & 0x3f);
        page->unlock();
        server->get_page_cache()->unpin_page(page, true);
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
