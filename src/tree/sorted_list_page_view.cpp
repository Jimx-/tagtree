#include "tagtree/tree/sorted_list_page_view.h"

#include <cassert>
#include <cstring>

namespace tagtree {

#define SGN(x) (((x) > 0) ? 1 : (((x) < 0) ? -1 : 0))

std::tuple<SymbolTable::Ref, TSID>
SortedListPageView::extract_item(unsigned int offset) const
{
    assert(offset <= get_item_count());
    auto [buf, len] = get_item(offset);
    assert(len == sizeof(SymbolTable::Ref) + sizeof(TSID));

    auto key_len = sizeof(SymbolTable::Ref);
    auto key = *(SymbolTable::Ref*)buf;
    auto tsid = *(TSID*)&buf[key_len];

    return std::tie(key, tsid);
}

void SortedListPageView::serialize_item(SymbolTable::Ref key, TSID value,
                                        std::vector<uint8_t>& out)
{
    out.resize(sizeof(SymbolTable::Ref) + sizeof(TSID));
    ::memcpy(&out[0], &key, sizeof(SymbolTable::Ref));
    ::memcpy(&out[sizeof(SymbolTable::Ref)], &value, sizeof(TSID));
}

size_t SortedListPageView::binary_search_page(SymbolTable::Ref key, TSID value,
                                              bool next_key)
{
    auto low = FIRST_KEY_OFFSET;
    auto high = get_item_count() + 1;
    int cond = next_key ? 0 : 1;

    while (low < high) {
        auto mid = (high + low) >> 1;

        auto [mid_key, tsid] = extract_item(mid);

        int cmp = SGN((int32_t)key - (int32_t)mid_key);
        if (cmp == 0) cmp = SGN((int64_t)value - (int64_t)tsid);

        if (cmp >= cond)
            low = mid + 1;
        else
            high = mid;
    }

    return low;
}

void SortedListPageView::get_values(SymbolTable::Ref key,
                                    std::vector<TSID>& values)
{
    values.clear();

    auto [first_key, first_tsid] = extract_item(FIRST_KEY_OFFSET);
    if (first_key > key) return;

    auto [last_key, last_tsid] = extract_item(get_item_count());
    if (last_key < key) return;

    int i = binary_search_page(key, 0, false);
    for (; i <= get_item_count(); i++) {
        auto [item_key, tsid] = extract_item(i);

        if (item_key != key) break;

        values.push_back(tsid);
    }
}

void SortedListPageView::scan_values(std::function<bool(SymbolTable::Ref)> pred,
                                     std::vector<TSID>& values)
{
    values.clear();

    for (int i = 1; i <= get_item_count(); i++) {
        auto [item_key, tsid] = extract_item(i);

        if (pred(item_key)) values.push_back(tsid);
    }
}

bool SortedListPageView::insert(SymbolTable::Ref key, TSID value)
{
    std::vector<uint8_t> buf;

    serialize_item(key, value, buf);
    assert(buf.size() == sizeof(SymbolTable::Ref) + sizeof(TSID));

    if (buf.size() > get_free_space()) return false;

    int offset = binary_search_page(key, value, false);

    return put_item(&buf[0], buf.size(), offset, false) != -1;
}

std::ostream& operator<<(std::ostream& os, const SortedListPageView& self)
{
    os << "{";

    bool first = true;
    for (int i = SortedListPageView::FIRST_KEY_OFFSET;
         i <= self.get_item_count(); i++) {
        if (!first) os << ", ";

        auto [key, value] = self.extract_item(i);
        os << key << " -> " << value;
        first = false;
    }

    os << "}";

    return os;
}

} // namespace tagtree
