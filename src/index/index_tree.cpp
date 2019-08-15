#include "tagtree/index/index_tree.h"
#include "bptree/heap_page_cache.h"
#include "bptree/mem_page_cache.h"
#include "tagtree/index/bitmap.h"
#include "tagtree/index/index_server.h"
#include "tagtree/series/series_manager.h"

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

template <> unsigned int IndexTree::get_segsel<uint64_t>(const uint64_t& key)
{
    return key & ((1 << (SEGSEL_BYTES * 8)) - 1);
}

template <>
unsigned int IndexTree::get_segsel<StringKey<IndexTree::KEY_WIDTH>>(
    const StringKey<IndexTree::KEY_WIDTH>& key)
{
    unsigned int segsel = 0;
    uint8_t buf[IndexTree::KEY_WIDTH];
    key.get_bytes(buf);

    for (int i = SEGSEL_BYTES - 1; i >= 0; i--) {
        segsel |= (buf[IndexTree::KEY_WIDTH - 1 - i] << (i << 3));
    }

    return segsel;
}

template <> void IndexTree::clear_key<uint64_t>(uint64_t& key) { key = 0; }
template <>
void IndexTree::clear_key<StringKey<IndexTree::KEY_WIDTH>>(
    StringKey<IndexTree::KEY_WIDTH>& key)
{
    key = StringKey<KEY_WIDTH>();
}

IndexTree::IndexTree(IndexServer* server, std::string_view dir,
                     size_t cache_size)
    : server(server), page_cache(std::make_unique<bptree::HeapPageCache>(
                          dir, true, cache_size)),
      btree(std::make_unique<BPTree>(page_cache.get()))
{
    postings_per_page =
        (page_cache->get_page_size() - 2 * sizeof(SymbolTable::Ref)) << 3;
}

void IndexTree::query_postings(
    const promql::LabelMatcher& matcher,
    std::map<unsigned int, std::unique_ptr<uint8_t[]>>& bitmaps)
{
    KeyType start_key, end_key, match_key, name_mask, name_value_mask;
    uint8_t key_buf[KEY_WIDTH];
    auto op = matcher.op;
    auto name = matcher.name;
    auto value = matcher.value;

    clear_key(start_key);
    match_key = make_key(name, value, 0);

    memset(key_buf, 0, sizeof(key_buf));
    memset(key_buf, 0xff, NAME_BYTES);
    pack_key(key_buf, name_mask);
    memset(key_buf, 0, sizeof(key_buf));
    memset(key_buf, 0xff, NAME_BYTES + VALUE_BYTES);
    pack_key(key_buf, name_value_mask);

    switch (op) {
    case MatchOp::EQL:
        /* key range: from   | hash(name) | hash(value)     | *
         *              to   | hash(name) | hash(value) + 1 | */
        start_key = make_key(name, value, 0);
        end_key = make_key(name, value, (1 << (SEGSEL_BYTES * 8)) - 1);
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
            start_key = make_key(name, value, 0);

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
            if (it->first > end_key) {
                /* no match */
                goto out;
            }
            break;
        case MatchOp::NEQ:
            if (it->first >= end_key) {
                goto out;
            } else if ((it->first & name_value_mask) == match_key) {
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
            if ((it->first & name_value_mask) > end_key) {
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

        unsigned int segsel = get_segsel(it->first);
        auto page_id = it->second;
        boost::upgrade_lock<bptree::Page> lock;
        auto page = page_cache->fetch_page(page_id, lock);
        const uint8_t* p = page->get_buffer(lock);

        promql::Label label;
        read_page_metadata(p, label);

        if (!matcher.match(label)) {
            page_cache->unpin_page(page, false, lock);
            it++;
            continue;
        }

        auto bmit = bitmaps.find(segsel);
        if (bmit == bitmaps.end()) {
            auto bmbuf = std::make_unique<uint8_t[]>(page->get_size());
            ::memcpy(bmbuf.get(), p, page->get_size());
            bitmaps.emplace(segsel, std::move(bmbuf));
        } else {
            auto* bmbuf = bmit->second.get();
            bitmap_or(bmbuf, p, bmbuf, page->get_size());
        }

        page_cache->unpin_page(page, false, lock);
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
    std::map<unsigned int, std::unique_ptr<uint8_t[]>> bitmaps;

    for (auto&& p : matchers) {
        std::map<unsigned int, std::unique_ptr<uint8_t[]>> tag_bitmaps;
        query_postings(p, tag_bitmaps);

        if (tag_bitmaps.empty()) {
            return;
        }

        if (first) {
            bitmaps = std::move(tag_bitmaps);
            first = false;
        } else {
            auto it1 = bitmaps.begin();
            auto it2 = tag_bitmaps.begin();

            while ((it1 != bitmaps.end()) && (it2 != tag_bitmaps.end())) {
                if (it1->first < it2->first) {
                    bitmaps.erase(it1++);
                } else if (it2->first < it1->first) {
                    ++it2;
                } else {
                    bitmap_and(it1->second.get(), it2->second.get(),
                               it1->second.get(), page_cache->get_page_size());
                    ++it1;
                    ++it2;
                }
            }
            bitmaps.erase(it1, bitmaps.end());
        }
    }

    // collect TSIDs
    tsids.clear();

    for (auto&& bm : bitmaps) {
        auto segsel = bm.first;
        uint8_t* buf = bm.second.get();
        uint8_t* lim = buf + page_cache->get_page_size();
        uint64_t* pbm = (uint64_t*)(buf + 2 * sizeof(SymbolTable::Ref));
        size_t start_index = 0;
        size_t seg_offset = segsel * postings_per_page;

        while (pbm < (uint64_t*)lim) {
            if (*pbm > 0) {
                for (uint64_t i = 0; i < 64; i++) {
                    if ((*pbm) & (1ULL << i)) {
                        tsids.insert(seg_offset + start_index + i);
                    }
                }
            }

            start_index += 64;
            pbm++;
        }
    }
}

void IndexTree::add_series(TSID tsid, const std::vector<promql::Label>& labels)
{
    for (auto&& label : labels) {
        insert_posting_id(label, tsid);
    }
}

size_t IndexTree::read_page_metadata(const uint8_t* buf, promql::Label& label)
{
    const uint8_t* start = buf;
    auto* sm = server->get_series_manager();

    uint32_t name_ref = *(uint32_t*)buf;
    buf += sizeof(uint32_t);
    uint32_t value_ref = *(uint32_t*)buf;
    buf += sizeof(uint32_t);

    label.name = sm->get_symbol(name_ref);
    label.value = sm->get_symbol(value_ref);

    return buf - start;
}

size_t IndexTree::write_page_metadata(uint8_t* buf, const promql::Label& label)
{
    uint8_t* start = buf;
    auto* sm = server->get_series_manager();
    auto name_ref = sm->add_symbol(label.name);
    auto value_ref = sm->add_symbol(label.value);

    *(uint32_t*)buf = (uint32_t)name_ref;
    buf += sizeof(uint32_t);
    *(uint32_t*)buf = (uint32_t)value_ref;
    buf += sizeof(uint32_t);

    return buf - start;
}

bptree::Page*
IndexTree::create_posting_page(const promql::Label& label,
                               boost::upgrade_lock<bptree::Page>& lock)
{
    auto page = page_cache->new_page(lock);
    boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
    uint8_t* buf = page->get_buffer(ulock);

    memset(buf, 0, page->get_size());
    write_page_metadata(buf, label);

    return page;
}

void IndexTree::insert_posting_id(const promql::Label& label, TSID tsid)
{
    /* lookup the first page for the label */
    bptree::Page* posting_page = nullptr;
    boost::upgrade_lock<bptree::Page> posting_page_lock;
    unsigned int segsel = tsid / postings_per_page;

    {
        std::lock_guard<std::mutex> guard(
            tree_mutex); // hold this lock until we obtain lock on the first
                         // page

        auto posting_key = make_key(label.name, label.value, segsel);
        std::vector<bptree::PageID> posting_page_ids;
        btree->get_value(posting_key, posting_page_ids);

        /* create page if no page is found*/
        if (posting_page_ids.empty()) {
            posting_page = create_posting_page(label, posting_page_lock);
            btree->insert(posting_key, posting_page->get_id());
        }

        /* find the real first page from probably multiple candidate pages */
        if (!posting_page) {
            for (auto&& pid : posting_page_ids) {
                boost::upgrade_lock<bptree::Page> plock;
                auto page = page_cache->fetch_page(pid, plock);
                assert(page != nullptr);

                const uint8_t* buf = page->get_buffer(plock);
                promql::Label page_label;

                read_page_metadata(buf, page_label);

                if (page_label.name != label.name ||
                    page_label.value != label.value) {
                    page_cache->unpin_page(page, false, plock);
                    continue;
                }

                posting_page = page;
                posting_page_lock = std::move(plock);
                break;
            }
        }

        if (!posting_page) {
            posting_page = create_posting_page(label, posting_page_lock);
            btree->insert(posting_key, posting_page->get_id());
        }
    }

    {
        // enter write lock
        boost::upgrade_to_unique_lock<bptree::Page> ulock(posting_page_lock);
        uint8_t* posting_buf = posting_page->get_buffer(ulock);
        uint8_t* lim = posting_buf + posting_page->get_size();
        uint64_t* bitmap = reinterpret_cast<uint64_t*>(
            posting_buf + 2 * sizeof(SymbolTable::Ref));

        size_t bitnum = tsid % postings_per_page;
        bitmap[bitnum >> 6] |= 1ULL << (bitnum & 0x3f);
    } // write lock of the posting page goes out of scope

    page_cache->unpin_page(posting_page, true, posting_page_lock);
    // read lock of the posting page goes out of scope
}

IndexTree::KeyType IndexTree::make_key(const std::string& name,
                                       const std::string& value,
                                       unsigned int segsel)
{
    uint8_t key_buf[KEY_WIDTH];
    memset(key_buf, 0, sizeof(key_buf));

    _hash_string_name(name, key_buf);
    _hash_string_value(value, &key_buf[NAME_BYTES]);
    _hash_segsel(segsel, &key_buf[NAME_BYTES + VALUE_BYTES]);

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

void IndexTree::_hash_segsel(unsigned int segsel, uint8_t* out)
{
    for (int i = SEGSEL_BYTES - 1; i >= 0; i--) {
        *out++ = (segsel >> (i << 3)) & 0xff;
    }
}

} // namespace tagtree
