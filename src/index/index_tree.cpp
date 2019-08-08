#include "tagtree/index/index_tree.h"
#include "bptree/heap_page_cache.h"
#include "bptree/mem_page_cache.h"
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

template <> unsigned int IndexTree::get_segsel<uint64_t>(const uint64_t& key)
{
    return key & ((1 << (SEGSEL_BYTES * 8)) - 1);
}

template <>
unsigned int IndexTree::get_segsel<StringKey<IndexTree::KEY_WIDTH>>(
    const StringKey<IndexTree::KEY_WIDTH>& key)
{
    return 0;
}

template <> void IndexTree::clear_key<uint64_t>(uint64_t& key) { key = 0; }
template <>
void IndexTree::clear_key<StringKey<IndexTree::KEY_WIDTH>>(
    StringKey<IndexTree::KEY_WIDTH>& key)
{
    key = StringKey<KEY_WIDTH>();
}

static size_t read_page_metadata(const uint8_t* buf, promql::Label& label,
                                 size_t& num_postings)
{
    const uint8_t* start = buf;

    uint32_t name_len = *(uint32_t*)buf;
    buf += sizeof(uint32_t);
    uint32_t value_len = *(uint32_t*)buf;
    buf += sizeof(uint32_t);
    num_postings = (size_t)(*(uint32_t*)buf);
    buf += sizeof(uint32_t);

    std::string name(buf, buf + name_len);
    buf += name_len;
    std::string value(buf, buf + value_len);
    buf += value_len;

    label.name = std::move(name);
    label.value = std::move(value);

    return buf - start;
}

static size_t write_page_metadata(uint8_t* buf, const promql::Label& label,
                                  size_t num_postings)
{
    uint8_t* start = buf;

    *(uint32_t*)buf = (uint32_t)label.name.length();
    buf += sizeof(uint32_t);
    *(uint32_t*)buf = (uint32_t)label.value.length();
    buf += sizeof(uint32_t);
    *(uint32_t*)buf = (uint32_t)num_postings;
    buf += sizeof(uint32_t);
    ::memcpy(buf, label.name.c_str(), label.name.length());
    buf += label.name.length();
    ::memcpy(buf, label.value.c_str(), label.value.length());
    buf += label.value.length();

    return buf - start;
}

IndexTree::IndexTree(std::string_view dir, size_t cache_size)
    : page_cache(
          std::make_unique<bptree::HeapPageCache>(dir, true, cache_size)),
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
        auto page = page_cache->fetch_page(page_id);
        const uint8_t* p = page->read_lock();

        if (segsel == 0) {
            /* first page, skip the segment count */
            p += sizeof(uint32_t);
        }

        promql::Label label;
        size_t num_postings;
        p += read_page_metadata(p, label, num_postings);

        if (!matcher.match(label)) {
            page->read_unlock();
            page_cache->unpin_page(page, false);
            it++;
            continue;
        }

        while (num_postings--) {
            TSID tsid;
            tsid.deserialize(p);
            postings.insert(tsid);
            p += sizeof(TSID);
        }

        page->read_unlock();
        page_cache->unpin_page(page, false);

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

bptree::Page* IndexTree::create_posting_page(const promql::Label& label)
{
    auto page = page_cache->new_page();
    uint8_t* buf = page->write_lock();

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
    auto first_key = make_key(label.name, label.value, 0);
    std::vector<bptree::PageID> first_page_ids;
    btree->get_value(first_key, first_page_ids);

    /* create page if no page is found*/
    if (first_page_ids.empty()) {
        first_page = create_posting_page(label);
        btree->insert(first_key, first_page->get_id());
    }

    /* find the real first page from probably multiple candidate pages */
    if (!first_page) {
        for (auto&& pid : first_page_ids) {
            auto page = page_cache->fetch_page(pid);
            assert(page != nullptr);

            /* write lock because we cannot upgrade it */
            const uint8_t* buf = page->write_lock();
            size_t num_postings;
            promql::Label page_label;

            buf += sizeof(uint32_t); // skip the segment count
            read_page_metadata(buf, page_label, num_postings);

            if (page_label.name != label.name ||
                page_label.value != label.value) {
                page->write_unlock();
                page_cache->unpin_page(page, false);
                continue;
            }

            first_page = page;
            break;
        }
    }

    if (!first_page) {
        first_page = create_posting_page(label);
        btree->insert(first_key, first_page->get_id());
    }

    bool dirty = insert_first_page(label, tsid, first_page);

    first_page->write_unlock();
    page_cache->unpin_page(first_page, dirty);
}

bool IndexTree::insert_first_page(const promql::Label& label, const TSID& tsid,
                                  bptree::Page* first_page)
{
    uint8_t* first_page_buf = first_page->get_buffer_locked();
    unsigned int num_segments = *(uint32_t*)first_page_buf;
    uint8_t *buffer_start, *uuid_list_start;
    size_t num_postings;
    uint32_t* num_postings_pos;
    bptree::Page* last_page = nullptr;

    if (num_segments == 1) {
        buffer_start = first_page_buf;
        uint8_t* p = first_page_buf + sizeof(uint32_t);
        num_postings_pos = (uint32_t*)p + 2;
        promql::Label tmp;
        uuid_list_start = p + read_page_metadata(p, tmp, num_postings);
    } else {
        /* read the last segment */
        auto last_key = make_key(label.name, label.value, num_segments - 1);
        std::vector<bptree::PageID> last_page_ids;
        btree->get_value(last_key, last_page_ids);

        for (auto&& pid : last_page_ids) {
            auto page = page_cache->fetch_page(pid);
            assert(page != nullptr);

            const uint8_t* buf = page->write_lock();
            size_t num_postings;
            promql::Label page_label;

            read_page_metadata(buf, page_label, num_postings);

            if (page_label.name != label.name ||
                page_label.value != label.value) {
                page->write_unlock();
                page_cache->unpin_page(page, false);
                continue;
            }

            last_page = page;
            break;
        }

        if (!last_page) {
            throw std::runtime_error("failed to find the last segment page");
        }

        buffer_start = last_page->get_buffer_locked();
        num_postings_pos = (uint32_t*)buffer_start + 2;
        promql::Label tmp;
        uuid_list_start =
            buffer_start + read_page_metadata(buffer_start, tmp, num_postings);
    }

    uint8_t* new_uuid_pos = uuid_list_start + sizeof(TSID) * num_postings;
    bool overflow =
        new_uuid_pos + sizeof(TSID) - buffer_start >= first_page->get_size();

    if (overflow) {
        if (last_page) {
            last_page->write_unlock();
            page_cache->unpin_page(last_page, false);
        }

        insert_new_segment(label, tsid, num_segments);
        *(uint32_t*)first_page_buf = num_segments + 1;
        return true;
    }

    tsid.serialize(new_uuid_pos);
    *num_postings_pos = num_postings + 1;

    if (last_page) {
        last_page->write_unlock();
        page_cache->unpin_page(last_page, true);
    }

    return !last_page;
}

void IndexTree::insert_new_segment(const promql::Label& label, const TSID& tsid,
                                   unsigned int segidx)
{
    auto page = page_cache->new_page();
    auto key = make_key(label.name, label.value, segidx);
    uint8_t* buf = page->write_lock();

    memset(buf, 0, page->get_size());
    buf += write_page_metadata(buf, label, 1);
    tsid.serialize(buf);

    page->write_unlock();
    page_cache->unpin_page(page, true);
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
