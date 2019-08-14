#include "tagtree/index/index_tree.h"
#include "bptree/heap_page_cache.h"
#include "bptree/mem_page_cache.h"
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
{}

void IndexTree::query_postings(const promql::LabelMatcher& matcher,
                               std::unordered_set<TSID>& postings)
{
    KeyType start_key, end_key, match_key, name_mask, name_value_mask;
    uint8_t key_buf[KEY_WIDTH];
    auto op = matcher.op;
    auto name = matcher.name;
    auto value = matcher.value;

    postings.clear();
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

        if (segsel == 0) {
            /* first page, skip the segment count */
            p += sizeof(uint32_t);
        }

        promql::Label label;
        size_t num_postings;
        p += read_page_metadata(p, label, num_postings);

        if (!matcher.match(label)) {
            page_cache->unpin_page(page, false, lock);
            it++;
            continue;
        }

        while (num_postings--) {
            TSID tsid;
            tsid.deserialize(p);
            postings.insert(tsid);
            p += sizeof(TSID);
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

size_t IndexTree::read_page_metadata(const uint8_t* buf, promql::Label& label,
                                     size_t& num_postings)
{
    const uint8_t* start = buf;
    auto* sm = server->get_series_manager();

    uint32_t name_ref = *(uint32_t*)buf;
    buf += sizeof(uint32_t);
    uint32_t value_ref = *(uint32_t*)buf;
    buf += sizeof(uint32_t);
    num_postings = (size_t)(*(uint32_t*)buf);
    buf += sizeof(uint32_t);

    label.name = sm->get_symbol(name_ref);
    label.value = sm->get_symbol(value_ref);

    return buf - start;
}

size_t IndexTree::write_page_metadata(uint8_t* buf, const promql::Label& label,
                                      size_t num_postings)
{
    uint8_t* start = buf;
    auto* sm = server->get_series_manager();
    auto name_ref = sm->add_symbol(label.name);
    auto value_ref = sm->add_symbol(label.value);

    *(uint32_t*)buf = (uint32_t)name_ref;
    buf += sizeof(uint32_t);
    *(uint32_t*)buf = (uint32_t)value_ref;
    buf += sizeof(uint32_t);
    *(uint32_t*)buf = (uint32_t)num_postings;
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
    *(uint32_t*)buf = (uint32_t)1;
    buf += sizeof(uint32_t);
    write_page_metadata(buf, label, 0);

    return page;
}

void IndexTree::insert_posting_id(const promql::Label& label, const TSID& tsid)
{
    /* lookup the first page for the label */
    bptree::Page* first_page = nullptr;
    boost::upgrade_lock<bptree::Page> first_page_lock;

    {
        std::lock_guard<std::mutex> guard(
            tree_mutex); // hold this lock until we obtain lock on the first
                         // page

        auto first_key = make_key(label.name, label.value, 0);
        std::vector<bptree::PageID> first_page_ids;
        btree->get_value(first_key, first_page_ids);

        /* create page if no page is found*/
        if (first_page_ids.empty()) {
            first_page = create_posting_page(label, first_page_lock);
            btree->insert(first_key, first_page->get_id());
        }

        /* find the real first page from probably multiple candidate pages */
        if (!first_page) {
            for (auto&& pid : first_page_ids) {
                boost::upgrade_lock<bptree::Page> plock;
                auto page = page_cache->fetch_page(pid, plock);
                assert(page != nullptr);

                const uint8_t* buf = page->get_buffer(plock);
                size_t num_postings;
                promql::Label page_label;

                buf += sizeof(uint32_t); // skip the segment count
                read_page_metadata(buf, page_label, num_postings);

                if (page_label.name != label.name ||
                    page_label.value != label.value) {
                    page_cache->unpin_page(page, false, plock);
                    continue;
                }

                first_page = page;
                first_page_lock = std::move(plock);
                break;
            }
        }

        if (!first_page) {
            first_page = create_posting_page(label, first_page_lock);
            btree->insert(first_key, first_page->get_id());
        }
    }

    bool dirty = insert_first_page(label, tsid, first_page, first_page_lock);

    page_cache->unpin_page(first_page, dirty, first_page_lock);
    // read lock of the first page goes out of scope
}

bool IndexTree::insert_first_page(
    const promql::Label& label, const TSID& tsid, bptree::Page* first_page,
    boost::upgrade_lock<bptree::Page>& first_page_lock)
{
    boost::upgrade_to_unique_lock<bptree::Page> first_page_ulock(
        first_page_lock); // a new segment will be created if the last segment
                          // overflows so upgrade to write lock before we read
                          // the number of segments

    uint8_t* first_page_buf = first_page->get_buffer(first_page_ulock);
    unsigned int num_segments = *(uint32_t*)first_page_buf;
    size_t num_postings;
    size_t meta_offset = sizeof(uint32_t);
    bptree::Page* last_page = nullptr;

    {
        boost::upgrade_lock<bptree::Page> last_segment_lock;

        if (num_segments > 1) {
            /* read the last segment */
            boost::upgrade_lock<bptree::Page> segment_lock;
            auto last_key = make_key(label.name, label.value, num_segments - 1);
            std::vector<bptree::PageID> last_page_ids;
            btree->get_value(last_key, last_page_ids);

            for (auto&& pid : last_page_ids) {
                auto page = page_cache->fetch_page(pid, segment_lock);
                assert(page != nullptr);

                const uint8_t* buf = page->get_buffer(segment_lock);
                size_t num_postings;
                promql::Label page_label;

                read_page_metadata(buf, page_label, num_postings);

                if (page_label.name != label.name ||
                    page_label.value != label.value) {
                    page_cache->unpin_page(page, false, segment_lock);
                    continue;
                }

                last_page = page;
                last_segment_lock = std::move(segment_lock);
                break;
            }

            if (!last_page) {
                first_page_ulock.~upgrade_to_unique_lock();
                page_cache->unpin_page(first_page, false, first_page_lock);
                throw std::runtime_error(
                    "failed to find the last segment page");
            }

            meta_offset = 0;
        }

        {
            boost::upgrade_to_unique_lock<bptree::Page> ulock(
                last_page ? std::move(boost::upgrade_to_unique_lock(
                                last_segment_lock))
                          : std::move(first_page_ulock));
            uint8_t* buffer_start =
                (last_page ? last_page : first_page)->get_buffer(ulock);
            uint8_t* meta_start = buffer_start + meta_offset;
            uint32_t* num_postings_pos = (uint32_t*)meta_start + 2;
            promql::Label tmp;
            uint8_t* uuid_list_start =
                meta_start + read_page_metadata(meta_start, tmp, num_postings);

            uint8_t* new_uuid_pos =
                uuid_list_start + sizeof(TSID) * num_postings;
            bool overflow = new_uuid_pos + sizeof(TSID) - buffer_start >=
                            first_page->get_size();

            if (overflow) {
                insert_new_segment(label, tsid, num_segments);
                *(uint32_t*)first_page_buf = num_segments + 1;
            } else {
                tsid.serialize(new_uuid_pos);
                *num_postings_pos = num_postings + 1;
            }
        } // write lock goes out of scope

        if (last_page) {
            page_cache->unpin_page(last_page, true, last_segment_lock);
        }
    } // read lock of the last segment goes out of scope

    return !last_page;
}

void IndexTree::insert_new_segment(const promql::Label& label, const TSID& tsid,
                                   unsigned int segidx)
{
    boost::upgrade_lock<bptree::Page> lock;
    auto page = page_cache->new_page(lock);
    auto key = make_key(label.name, label.value, segidx);

    {
        boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
        uint8_t* buf = page->get_buffer(ulock);

        memset(buf, 0, page->get_size());
        buf += write_page_metadata(buf, label, 1);
        tsid.serialize(buf);
    }

    page_cache->unpin_page(page, true, lock);
    btree->insert(key, page->get_id());
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
