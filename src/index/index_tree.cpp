#include "tagtree/index/index_tree.h"
#include "bptree/heap_page_cache.h"
#include "bptree/mem_page_cache.h"
#include "tagtree/index/bitmap.h"
#include "tagtree/index/index_server.h"
#include "tagtree/series/series_manager.h"

#include <cassert>

using promql::MatchOp;

namespace tagtree {

static void incr_buf(uint8_t* buf, size_t len)
{
    int i = len - 1;
    uint32_t carry = 1;

    while (carry && i >= 0) {
        uint32_t s = (uint32_t)buf[i] + carry;
        buf[i] = s & 0xff;
        carry = (s >> 8) & 0xff;
        i--;
    }
}

IndexTree::IndexTree(IndexServer* server, std::string_view filename,
                     size_t cache_size)
    : server(server), page_cache(std::make_unique<bptree::HeapPageCache>(
                          filename, true, cache_size)),
      cow_tree(page_cache.get())
{
    postings_per_page =
        (page_cache->get_page_size() - 2 * sizeof(SymbolTable::Ref)) << 3;
}

IndexTree::~IndexTree() {}

void IndexTree::query_postings(
    const promql::LabelMatcher& matcher, uint64_t start, uint64_t end,
    std::map<unsigned int, std::unique_ptr<uint8_t[]>>& bitmaps,
    const std::set<unsigned int>& seg_mask)
{
    KeyType start_key, end_key, match_key;
    uint8_t name_buf[NAME_BYTES];
    auto op = matcher.op;
    auto name = matcher.name;
    auto value = matcher.value;

    match_key = make_key(name, value, 0, 0);

    switch (op) {
    case MatchOp::EQL:
        /* key range: from   | hash(name) | hash(value)     | *
         *              to   | hash(name) | hash(value) + 1 | */
        start_key = make_key(name, value, 0, 0);
        end_key = make_key(name, value, end, (1 << (SEGSEL_BYTES * 8)) - 1);
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
            start_key = make_key(name, value, 0, 0);

            start_key.get_tag_name(name_buf);
            incr_buf(name_buf, NAME_BYTES);
            end_key.set_tag_name(name_buf);

            start_key.clear_tag_value();
            end_key.clear_tag_value();

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

    auto it = cow_tree.begin(start_key);
    while (it != cow_tree.end()) {
        switch (op) {
        case MatchOp::EQL:
            if (it->first > end_key) {
                /* no match */
                goto out;
            }
            break;
        case MatchOp::NEQ: {
            KeyType name_value_part = it->first;
            name_value_part.set_timestamp(0);
            name_value_part.set_segnum(0);

            if (it->first >= end_key) {
                goto out;
            } else if (name_value_part == match_key) {
                it++;
                continue;
            }
            break;
        }
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
        case MatchOp::LTE: {
            KeyType name_value_part = it->first;
            name_value_part.set_timestamp(0);
            name_value_part.set_segnum(0);

            if (name_value_part > end_key) {
                goto out;
            }
            break;
        }
        case MatchOp::EQL_REGEX:
        case MatchOp::NEQ_REGEX:
            if (it->first >= end_key) {
                goto out;
            }
            break;
        default:
            break;
        }

        if (it->first.get_timestamp() >= end) {
            it++;
            continue;
        }

        unsigned int segsel = it->first.get_segnum();

        if (!seg_mask.empty() && seg_mask.find(segsel) == seg_mask.end()) {
            it++;
            continue;
        }

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
    const std::vector<promql::LabelMatcher>& matchers, uint64_t start,
    uint64_t end, Roaring& postings)
{
    bool first = true;
    std::map<unsigned int, std::unique_ptr<uint8_t[]>> bitmaps;
    std::set<unsigned int> seg_mask;

    for (auto&& p : matchers) {
        seg_mask.clear();
        if (!first) {
            for (auto&& p : bitmaps) {
                seg_mask.insert(p.first);
            }
        }

        std::map<unsigned int, std::unique_ptr<uint8_t[]>> tag_bitmaps;
        query_postings(p, start, end, tag_bitmaps, seg_mask);

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
    postings = Roaring{};

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
                        postings.add(seg_offset + start_index + i);
                    }
                }
            }

            start_index += 64;
            pbm++;
        }
    }
}

void IndexTree::label_values(const std::string& label_name,
                             std::unordered_set<std::string>& values)
{
    KeyType start_key, end_key;
    uint8_t name_buf[NAME_BYTES];

    start_key = make_key(label_name, "", 0, 0);

    start_key.get_tag_name(name_buf);
    incr_buf(name_buf, NAME_BYTES);
    end_key.set_tag_name(name_buf);

    start_key.clear_tag_value();
    end_key.clear_tag_value();

    auto it = cow_tree.begin(start_key);
    while (it != cow_tree.end()) {
        if (it->first >= end_key) {
            break;
        }

        auto page_id = it->second;
        boost::upgrade_lock<bptree::Page> lock;
        auto page = page_cache->fetch_page(page_id, lock);
        const uint8_t* p = page->get_buffer(lock);

        promql::Label label;
        read_page_metadata(p, label);

        if (label.name == label_name) {
            values.insert(label.value);
        }

        page_cache->unpin_page(page, false, lock);
        it++;
    }
}

void IndexTree::write_postings(
    TSID limit, const std::vector<LabeledPostings>& labeled_postings)
{
    struct TreeEntry {
        KeyType key;
        bptree::PageID pid;
        bool updated;

        TreeEntry(KeyType key, bptree::PageID pid, bool updated)
            : key(key), pid(pid), updated(updated)
        {}
    };
    std::vector<TreeEntry> tree_entries;
    std::unordered_set<KeyType> keys;

    for (auto&& entry : labeled_postings) {
        auto& name = entry.label.name;
        auto& value = entry.label.value;
        auto& bitmap = entry.postings;
        auto min_timestamp = entry.min_timestamp;

        if (bitmap.isEmpty()) continue;

        auto left_it = bitmap.begin();
        auto left_segsel = tsid_segsel(*left_it);

        auto it = left_it;
        ++it;
        auto end_it = bitmap.begin();
        end_it.equalorlarger(limit);
        if (end_it != bitmap.end()) end_it++;

        bool updated;
        bptree::PageID pid;
        KeyType posting_key;
        for (; it != end_it; it++) {
            auto cur_segsel = tsid_segsel(*it);

            if (cur_segsel != left_segsel) {
                pid = write_posting_page(name, value, min_timestamp,
                                         left_segsel, left_it, it, updated);

                posting_key = make_key(name, value, min_timestamp, left_segsel);
                tree_entries.emplace_back(posting_key, pid, updated);

                left_segsel = cur_segsel;
                left_it = it;
            }
        }

        if (left_it != end_it) {
            pid = write_posting_page(name, value, min_timestamp, left_segsel,
                                     left_it, end_it, updated);

            posting_key = make_key(name, value, min_timestamp, left_segsel);
            tree_entries.emplace_back(posting_key, pid, updated);
        }
    }

    COWTreeType::Transaction txn;
    cow_tree.get_write_tree(txn);

    for (auto&& entry : tree_entries) {
        if (entry.updated) {
            cow_tree.update(entry.key, entry.pid, txn);
        } else {
            cow_tree.insert(entry.key, entry.pid, txn);
        }
    }

    cow_tree.commit(txn);
}

bptree::PageID IndexTree::write_posting_page(
    const std::string& name, const std::string& value, uint64_t start_time,
    unsigned int segsel, const RoaringSetBitForwardIterator& first,
    const RoaringSetBitForwardIterator& last, bool& updated)
{
    /* lookup the first page for the label */
    bptree::Page* posting_page = nullptr;
    boost::upgrade_lock<bptree::Page> posting_page_lock;

    auto posting_key = make_key(name, value, start_time, segsel);
    std::vector<bptree::PageID> posting_page_ids;
    cow_tree.get_value(posting_key, posting_page_ids);

    updated = false;
    if (posting_page_ids.empty()) {
        posting_page = create_posting_page({name, value}, posting_page_lock);
    }

    if (!posting_page) {
        for (auto&& pid : posting_page_ids) {
            boost::upgrade_lock<bptree::Page> plock;
            auto page = page_cache->fetch_page(pid, plock);
            assert(page != nullptr);

            const uint8_t* buf = page->get_buffer(plock);
            promql::Label page_label;

            read_page_metadata(buf, page_label);

            if (page_label.name != name || page_label.value != value) {
                page_cache->unpin_page(page, false, plock);
                continue;
            }

            posting_page = page_cache->new_page(posting_page_lock);
            boost::upgrade_to_unique_lock<bptree::Page> ulock(
                posting_page_lock);

            uint8_t* new_buf = posting_page->get_buffer(ulock);
            ::memcpy(new_buf, buf, page->get_size());

            page_cache->unpin_page(page, false, plock);
            updated = true;
            break;
        }
    }

    if (!posting_page) {
        posting_page = create_posting_page({name, value}, posting_page_lock);
    }

    {
        boost::upgrade_to_unique_lock<bptree::Page> ulock(posting_page_lock);
        uint8_t* posting_buf = posting_page->get_buffer(ulock);
        uint64_t* bitmap = reinterpret_cast<uint64_t*>(
            posting_buf + 2 * sizeof(SymbolTable::Ref));

        for (auto it = first; it != last; it++) {
            size_t bitnum = *it % postings_per_page;
            bitmap[bitnum >> 6] |= 1ULL << (bitnum & 0x3f);
        }
    }

    page_cache->unpin_page(posting_page, true, posting_page_lock);

    return posting_page->get_id();
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

IndexTree::KeyType IndexTree::make_key(const std::string& name,
                                       const std::string& value,
                                       uint64_t start_time, unsigned int segsel)
{
    KeyType key;
    uint8_t name_buf[NAME_BYTES];
    uint8_t value_buf[VALUE_BYTES];

    memset(name_buf, 0, sizeof(name_buf));
    memset(value_buf, 0, sizeof(value_buf));

    _hash_string_name(name, name_buf);
    _hash_string_value(value, value_buf);

    key.set_tag_name(name_buf);
    key.set_tag_value(value_buf);
    key.set_timestamp(start_time);
    key.set_segnum(segsel);

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
