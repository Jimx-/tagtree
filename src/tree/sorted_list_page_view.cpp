#include "tagtree/tree/sorted_list_page_view.h"

#include <cstring>

namespace tagtree {

#define SGN(x) (((x) > 0) ? 1 : (((x) < 0) ? -1 : 0))

std::tuple<std::string, TSID>
SortedListPageView::extract_item(unsigned int offset) const
{
    auto [buf, len] = get_item(offset);
    auto key_len = len - sizeof(TSID);
    auto tsid = *(TSID*)&buf[key_len];
    std::string key{reinterpret_cast<const char*>(buf), key_len};

    return std::tie(key, tsid);
}

void SortedListPageView::serialize_item(const std::string& key, TSID value,
                                        std::vector<uint8_t>& out)
{
    auto key_len = key.length();
    out.resize(key_len + sizeof(TSID));
    ::memcpy(&out[0], key.c_str(), key_len);
    ::memcpy(&out[key_len], &value, sizeof(TSID));
}

size_t SortedListPageView::binary_search_page(const std::string& key,
                                              TSID value, bool next_key)
{
    auto low = FIRST_KEY_OFFSET;
    auto high = get_item_count() + 1;
    int cond = next_key ? 0 : 1;

    while (low < high) {
        auto mid = (high + low) >> 1;

        auto [mid_key, tsid] = extract_item(mid);

        int cmp = SGN(key.compare(mid_key));
        if (cmp == 0) cmp = SGN((int64_t)value - (int64_t)tsid);

        if (cmp >= cond)
            low = mid + 1;
        else
            high = mid;
    }

    return low;
}

void SortedListPageView::get_values(const std::string& key,
                                    std::vector<TSID>& values)
{
    values.clear();

    int i = binary_search_page(key, 0, false);
    for (; i <= get_item_count(); i++) {
        auto [item_key, tsid] = extract_item(i);

        if (item_key != key) break;

        values.push_back(tsid);
    }
}

void SortedListPageView::scan_values(
    std::function<bool(const std::string&)> pred, std::vector<TSID>& values)
{
    values.clear();

    for (int i = 1; i <= get_item_count(); i++) {
        auto [item_key, tsid] = extract_item(i);

        if (pred(item_key)) values.push_back(tsid);
    }
}

bool SortedListPageView::insert(const std::string& key, TSID value)
{
    std::vector<uint8_t> buf;

    serialize_item(key, value, buf);
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
