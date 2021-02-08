#include "tagtree/index/index_tree.h"
#include "bptree/heap_page_cache.h"
#include "bptree/mem_page_cache.h"
#include "tagtree/index/bitmap.h"
#include "tagtree/index/index_server.h"
#include "tagtree/series/series_manager.h"
#include "tagtree/tree/sorted_list_page_view.h"

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

void IndexTree::copy_to_bitmaps(
    const Roaring& bitmap,
    std::map<unsigned int, std::unique_ptr<uint8_t[]>>& bitmaps,
    const std::set<unsigned int>& seg_mask)
{
    unsigned int skip = -1;
    unsigned int cur_seg = -1;
    uint8_t* page_buf = nullptr;

    for (auto it = bitmap.begin(); it != bitmap.end(); it++) {
        auto seg = tsid_segsel(*it);

        if (seg == skip) continue;

        if (cur_seg != seg) {
            if (!seg_mask.empty() && seg_mask.find(seg) == seg_mask.end()) {
                skip = seg;
                continue;
            }

            auto bmit = bitmaps.find(seg);
            if (bmit == bitmaps.end()) {
                auto bmbuf =
                    std::make_unique<uint8_t[]>(page_cache->get_page_size());
                page_buf = bmbuf.get();
                ::memset(page_buf, 0, page_cache->get_page_size());
                bitmaps.emplace(seg, std::move(bmbuf));
            } else {
                page_buf = bmit->second.get();
            }
        }

        {
            uint64_t* bm =
                reinterpret_cast<uint64_t*>(page_buf + BITMAP_PAGE_OFFSET);
            size_t bitnum = *it % postings_per_page;
            bm[bitnum >> 6] |= 1ULL << (bitnum & 0x3f);
        }

        cur_seg = seg;
    }
}

IndexTree::IndexTree(IndexServer* server, std::string_view filename,
                     size_t cache_size)
    : server(server), page_cache(std::make_unique<bptree::HeapPageCache>(
                          filename, true, cache_size)),
      cow_tree(page_cache.get())
{
    postings_per_page = (page_cache->get_page_size() - BITMAP_PAGE_OFFSET) << 3;
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

    query_postings_sorted_list(matcher, start, end, bitmaps, seg_mask);

    match_key = make_key(name, value, 0, 0);

    switch (op) {
    case MatchOp::EQL:
        /* key range: from   | hash(name) | hash(value)     | *
         *              to   | hash(name) | hash(value) + 1 | */
        start_key = make_key(name, value, 0, UINT32_MAX);
        end_key = make_key(name, value, end, UINT32_MAX);
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
            start_key = make_key(name, value, 0, UINT32_MAX);

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
        uint64_t end_timestamp;
        TreePageType type;
        read_page_metadata(p, label, end_timestamp, type);

        if (type != TreePageType::BITMAP) {
            page_cache->unpin_page(page, false, lock);
            it++;
            continue;
        }

        if (end_timestamp < start) {
            page_cache->unpin_page(page, false, lock);
            it++;
            continue;
        }

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

void IndexTree::query_postings_sorted_list(
    const promql::LabelMatcher& matcher, uint64_t start, uint64_t end,
    std::map<unsigned int, std::unique_ptr<uint8_t[]>>& bitmaps,
    const std::set<unsigned int>& seg_mask)
{
    Roaring bitmap;
    KeyType start_key, end_key;
    SymbolTable::Ref value_ref = 0;
    auto* sm = server->get_series_manager();

    if (matcher.op == promql::MatchOp::EQL)
        value_ref = sm->add_symbol(matcher.value);

    start_key = make_key(matcher.name, "", 0, UINT32_MAX);
    end_key = make_key(matcher.name, "", end, UINT32_MAX);
    start_key.clear_tag_value();
    end_key.clear_tag_value();

    auto it = cow_tree.begin(start_key);
    while (it != cow_tree.end()) {
        if (it->first >= end_key) {
            break;
        }

        if (it->first.get_timestamp() >= end) {
            it++;
            continue;
        }

        auto page_id = it->second;
        boost::upgrade_lock<bptree::Page> lock;
        auto page = page_cache->fetch_page(page_id, lock);
        const uint8_t* p = page->get_buffer(lock);

        promql::Label label;
        uint64_t end_timestamp;
        TreePageType type;
        read_page_metadata(p, label, end_timestamp, type);

        if (type != TreePageType::SORTED_LIST) {
            page_cache->unpin_page(page, false, lock);
            it++;
            continue;
        }

        if (end_timestamp < start) {
            page_cache->unpin_page(page, false, lock);
            it++;
            continue;
        }

        if (label.name != matcher.name) {
            page_cache->unpin_page(page, false, lock);
            it++;
            continue;
        }

        uint8_t* buf = const_cast<uint8_t*>(p + BITMAP_PAGE_OFFSET);
        SortedListPageView page_view(buf, page_cache->get_page_size() -
                                              BITMAP_PAGE_OFFSET);

        std::vector<TSID> series_list;
        if (matcher.op == promql::MatchOp::EQL) {
            page_view.get_values(value_ref, series_list);
        } else {
            auto name = matcher.name;

            page_view.scan_values(
                [this, sm, value_ref, &matcher, &name](SymbolTable::Ref ref) {
                    if (matcher.op == promql::MatchOp::NEQ && ref == value_ref)
                        return false;

                    return matcher.match({name, sm->get_symbol(ref)});
                },
                series_list);
        }

        for (auto&& p : series_list) {
            bitmap.add(p);
        }

        page_cache->unpin_page(page, false, lock);
        it++;
    }

    if (bitmap.cardinality()) copy_to_bitmaps(bitmap, bitmaps, seg_mask);
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
                    it1 = bitmaps.erase(it1);
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
        uint64_t* pbm = (uint64_t*)(buf + BITMAP_PAGE_OFFSET);
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

    start_key = make_key(label_name, "", 0, UINT32_MAX);

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
        uint64_t end_timestamp;
        TreePageType type;
        read_page_metadata(p, label, end_timestamp, type);

        if (label.name == label_name && type == TreePageType::BITMAP) {
            values.insert(label.value);
        }

        page_cache->unpin_page(page, false, lock);
        it++;
    }
}

void IndexTree::write_postings_bitmap(TSID limit, const std::string& name,
                                      const std::string& value,
                                      const Roaring& bitmap,
                                      uint64_t min_timestamp,
                                      uint64_t max_timestamp,
                                      std::vector<TreeEntry>& tree_entries)
{
    if (bitmap.isEmpty()) return;

    auto left_it = bitmap.begin();
    auto left_segsel = tsid_segsel(*left_it);

    auto it = left_it;
    ++it;
    auto end_it = bitmap.begin();
    end_it.equalorlarger(limit);
    if (end_it != bitmap.end() && *end_it == limit) end_it++;

    bool updated;
    bptree::PageID pid;
    KeyType posting_key;
    for (; it != end_it; it++) {
        auto cur_segsel = tsid_segsel(*it);

        if (cur_segsel != left_segsel) {
            pid = write_posting_page(name, value, min_timestamp, max_timestamp,
                                     left_segsel, left_it, it, updated);

            posting_key = make_key(name, value, min_timestamp, left_segsel);
            tree_entries.emplace_back(posting_key, pid, updated);

            left_segsel = cur_segsel;
            left_it = it;
        }
    }

    if (left_it != end_it) {
        pid = write_posting_page(name, value, min_timestamp, max_timestamp,
                                 left_segsel, left_it, end_it, updated);

        posting_key = make_key(name, value, min_timestamp, left_segsel);
        tree_entries.emplace_back(posting_key, pid, updated);
    }
}

bool IndexTree::get_sorted_list_initial_segment(
    const std::string& name, uint64_t start_time, uint64_t end_time,
    uint32_t& segsel, bptree::Page*& posting_page,
    boost::upgrade_lock<bptree::Page>& posting_page_lock)
{
    bool updated = false;

    auto start_key = make_key(name, "", start_time, UINT32_MAX);
    start_key.clear_tag_value();

    segsel = 0;
    posting_page = nullptr;

    auto it = cow_tree.begin(start_key);

    while (it != cow_tree.end()) {
        auto name_timestamp_part = it->first;
        name_timestamp_part.set_segnum(0);
        start_key.set_segnum(0);

        if (start_key != name_timestamp_part) break;

        boost::upgrade_lock<bptree::Page> plock;
        auto page = page_cache->fetch_page(it->second, plock);
        assert(page != nullptr);

        const uint8_t* buf = page->get_buffer(plock);
        promql::Label page_label;
        uint64_t page_end_timestamp;
        TreePageType page_type;

        read_page_metadata(buf, page_label, page_end_timestamp, page_type);
        end_time = std::max(end_time, page_end_timestamp);

        if (page_label.name != name || page_type != TreePageType::SORTED_LIST) {
            page_cache->unpin_page(page, false, plock);
            continue;
        }

        posting_page = page_cache->new_page(posting_page_lock);
        boost::upgrade_to_unique_lock<bptree::Page> ulock(posting_page_lock);

        uint8_t* new_buf = posting_page->get_buffer(ulock);
        ::memcpy(new_buf, buf, page->get_size());

        write_page_metadata(new_buf, {name, ""}, end_time,
                            TreePageType::SORTED_LIST);

        page_cache->unpin_page(page, false, plock);
        segsel = it->first.get_segnum();
        updated = true;
        break;
    }

    if (!posting_page) {
        posting_page = create_posting_page(
            {name, ""}, end_time, TreePageType::SORTED_LIST, posting_page_lock);
    }

    return updated;
}

void IndexTree::write_postings_sorted_list(
    TSID limit, const std::string& name,
    const std::vector<LabeledPostings>& entries,
    std::vector<TreeEntry>& tree_entries)
{
    if (entries.empty()) return;

    bptree::Page* posting_page = nullptr;
    boost::upgrade_lock<bptree::Page> posting_page_lock;
    uint64_t min_timestamp, max_timestamp;
    unsigned int segsel;
    bool updated, need_init;

    min_timestamp = entries.begin()->min_timestamp;
    max_timestamp = entries.begin()->max_timestamp;

    updated = get_sorted_list_initial_segment(name, min_timestamp,
                                              max_timestamp, segsel,
                                              posting_page, posting_page_lock);
    need_init = !updated;
    assert(posting_page);

    for (auto&& entry : entries) {
        auto& value = entry.value;
        auto& bitmap = entry.postings;
        auto value_ref = server->get_series_manager()->add_symbol(value);

        auto left_it = bitmap.begin();

        auto it = left_it;
        auto end_it = bitmap.begin();
        end_it.equalorlarger(limit);
        if (end_it != bitmap.end() && *end_it == limit) end_it++;

        max_timestamp = std::max(max_timestamp, entry.max_timestamp);

        for (; it != end_it; it++) {
            {
                boost::upgrade_to_unique_lock<bptree::Page> ulock(
                    posting_page_lock);
                uint8_t* page_buf = posting_page->get_buffer(ulock);
                uint8_t* buf =
                    reinterpret_cast<uint8_t*>(page_buf + BITMAP_PAGE_OFFSET);

                SortedListPageView page_view(buf, page_cache->get_page_size() -
                                                      BITMAP_PAGE_OFFSET);

                if (need_init) {
                    page_view.init_page();
                    need_init = false;
                }

                if (page_view.insert(value_ref, *it)) continue;

                write_page_metadata(page_buf, {name, ""}, max_timestamp,
                                    TreePageType::SORTED_LIST);
                auto posting_key = make_key(name, value, min_timestamp, segsel);
                posting_key.clear_tag_value();
                tree_entries.emplace_back(posting_key, posting_page->get_id(),
                                          updated);
                updated = false;
            }

            page_cache->unpin_page(posting_page, true, posting_page_lock);

            min_timestamp = entry.min_timestamp;
            segsel++;
            posting_page = create_posting_page({name, ""}, max_timestamp,
                                               TreePageType::SORTED_LIST,
                                               posting_page_lock);

            {
                boost::upgrade_to_unique_lock<bptree::Page> ulock(
                    posting_page_lock);
                uint8_t* page_buf = posting_page->get_buffer(ulock);
                uint8_t* buf =
                    reinterpret_cast<uint8_t*>(page_buf + BITMAP_PAGE_OFFSET);

                SortedListPageView page_view(buf, page_cache->get_page_size() -
                                                      BITMAP_PAGE_OFFSET);

                page_view.init_page();

                assert(page_view.insert(value_ref, *it));
            }
        }
    }

    {
        boost::upgrade_to_unique_lock<bptree::Page> ulock(posting_page_lock);
        uint8_t* page_buf = posting_page->get_buffer(ulock);
        uint8_t* buf =
            reinterpret_cast<uint8_t*>(page_buf + BITMAP_PAGE_OFFSET);

        SortedListPageView page_view(buf, page_cache->get_page_size() -
                                              BITMAP_PAGE_OFFSET);

        if (!need_init && page_view.get_item_count()) {
            write_page_metadata(page_buf, {name, ""}, max_timestamp,
                                TreePageType::SORTED_LIST);
            auto posting_key = make_key(name, "", min_timestamp, segsel);
            posting_key.clear_tag_value();
            tree_entries.emplace_back(posting_key, posting_page->get_id(),
                                      updated);
        }
    }

    page_cache->unpin_page(posting_page, true, posting_page_lock);
}

void IndexTree::write_postings(TSID limit, MemIndexSnapshot& snapshot)
{
    std::vector<TreeEntry> tree_entries;
    std::unordered_set<KeyType> keys;

    for (auto&& entries : snapshot) {
        auto& name = entries.first;
        auto type = choose_page_type(name, entries.second);

        switch (type) {
        case TreePageType::SORTED_LIST:
            std::sort(
                entries.second.begin(), entries.second.end(),
                [](const LabeledPostings& lhs, const LabeledPostings& rhs) {
                    return lhs.min_timestamp < rhs.min_timestamp;
                });

            write_postings_sorted_list(limit, name, entries.second,
                                       tree_entries);
            break;
        case TreePageType::BITMAP:
            for (auto&& entry : entries.second) {
                auto& value = entry.value;
                auto& bitmap = entry.postings;
                auto min_timestamp = entry.min_timestamp;
                auto max_timestamp = entry.max_timestamp;

                write_postings_bitmap(limit, name, value, bitmap, min_timestamp,
                                      max_timestamp, tree_entries);
            }

            break;
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

    page_cache->flush_all_pages();
    cow_tree.commit(txn);
}

bptree::PageID IndexTree::write_posting_page(
    const std::string& name, const std::string& value, uint64_t start_time,
    uint64_t end_time, unsigned int segsel,
    const RoaringSetBitForwardIterator& first,
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
        posting_page = create_posting_page(
            {name, value}, end_time, TreePageType::BITMAP, posting_page_lock);
    }

    if (!posting_page) {
        for (auto&& pid : posting_page_ids) {
            boost::upgrade_lock<bptree::Page> plock;
            auto page = page_cache->fetch_page(pid, plock);
            assert(page != nullptr);

            const uint8_t* buf = page->get_buffer(plock);
            promql::Label page_label;
            uint64_t page_end_timestamp;
            TreePageType page_type;

            read_page_metadata(buf, page_label, page_end_timestamp, page_type);
            end_time = std::max(end_time, page_end_timestamp);

            if (page_label.name != name || page_label.value != value ||
                page_type != TreePageType::BITMAP) {
                page_cache->unpin_page(page, false, plock);
                continue;
            }

            posting_page = page_cache->new_page(posting_page_lock);
            boost::upgrade_to_unique_lock<bptree::Page> ulock(
                posting_page_lock);

            uint8_t* new_buf = posting_page->get_buffer(ulock);
            ::memcpy(new_buf, buf, page->get_size());

            write_page_metadata(new_buf, {name, value}, end_time,
                                TreePageType::BITMAP);

            page_cache->unpin_page(page, false, plock);
            updated = true;
            break;
        }
    }

    if (!posting_page) {
        posting_page = create_posting_page(
            {name, value}, end_time, TreePageType::BITMAP, posting_page_lock);
    }

    {
        boost::upgrade_to_unique_lock<bptree::Page> ulock(posting_page_lock);
        uint8_t* posting_buf = posting_page->get_buffer(ulock);
        uint64_t* bitmap =
            reinterpret_cast<uint64_t*>(posting_buf + BITMAP_PAGE_OFFSET);

        for (auto it = first; it != last; it++) {
            assert(tsid_segsel(*it) == segsel);
            size_t bitnum = *it % postings_per_page;
            bitmap[bitnum >> 6] |= 1ULL << (bitnum & 0x3f);
        }
    }

    page_cache->unpin_page(posting_page, true, posting_page_lock);

    return posting_page->get_id();
}

IndexTree::TreePageType
IndexTree::choose_page_type(const std::string& tag_name,
                            const std::vector<LabeledPostings>& entry)
{
    size_t page_size = page_cache->get_page_size();
    size_t n_vals = entry.size();
    size_t bitmap_size = n_vals * page_size;
    size_t sum = 0;

    for (auto&& p : entry) {
        sum += p.postings.cardinality();
    }

    size_t sorted_size = sum * (sizeof(TSID) + sizeof(SymbolTable::Ref));
    if (sorted_size % page_size)
        sorted_size += page_size - (sorted_size % page_size);

    if (sorted_size <= bitmap_size) return TreePageType::SORTED_LIST;

    return TreePageType::BITMAP;
}

size_t IndexTree::read_page_metadata(const uint8_t* buf, promql::Label& label,
                                     uint64_t& end_timestamp,
                                     TreePageType& type)
{
    const uint8_t* start = buf;
    auto* sm = server->get_series_manager();

    uint32_t name_ref = *(uint32_t*)buf;
    buf += sizeof(uint32_t);
    uint32_t value_ref = *(uint32_t*)buf;
    buf += sizeof(uint32_t);
    end_timestamp = *(uint64_t*)buf;
    buf += sizeof(uint64_t);

    type = TreePageType::BITMAP;
    if (end_timestamp & (1ULL << 63)) type = TreePageType::SORTED_LIST;
    end_timestamp &= ~(1ULL << 63);

    label.name = sm->get_symbol(name_ref);
    label.value = sm->get_symbol(value_ref);

    return buf - start;
}

size_t IndexTree::write_page_metadata(uint8_t* buf, const promql::Label& label,
                                      uint64_t end_timestamp, TreePageType type)
{
    uint8_t* start = buf;
    auto* sm = server->get_series_manager();
    auto name_ref = sm->add_symbol(label.name);
    auto value_ref = sm->add_symbol(label.value);

    end_timestamp &= ~(1ULL << 63);
    if (type == TreePageType::SORTED_LIST) end_timestamp |= (1ULL << 63);

    *(uint32_t*)buf = (uint32_t)name_ref;
    buf += sizeof(uint32_t);
    *(uint32_t*)buf = (uint32_t)value_ref;
    buf += sizeof(uint32_t);
    *(uint64_t*)buf = end_timestamp;
    buf += sizeof(uint64_t);

    return buf - start;
}

bptree::Page*
IndexTree::create_posting_page(const promql::Label& label,
                               uint64_t end_timestamp, TreePageType type,
                               boost::upgrade_lock<bptree::Page>& lock)
{
    auto page = page_cache->new_page(lock);
    boost::upgrade_to_unique_lock<bptree::Page> ulock(lock);
    uint8_t* buf = page->get_buffer(ulock);

    memset(buf, 0, page->get_size());
    size_t offset = write_page_metadata(buf, label, end_timestamp, type);
    assert(offset == BITMAP_PAGE_OFFSET);

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
