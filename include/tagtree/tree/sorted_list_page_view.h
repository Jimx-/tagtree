#ifndef _TAGTREE_SORTED_LIST_PAGE_VIEW_H_
#define _TAGTREE_SORTED_LIST_PAGE_VIEW_H_

#include "tagtree/tree/item_page_view.h"
#include "tagtree/tsid.h"

#include <iostream>
#include <string>

namespace tagtree {

class SortedListPageView : public ItemPageView {
public:
    SortedListPageView(uint8_t* buf, size_t size) : ItemPageView(buf, size) {}

    void get_values(const std::string& key, std::vector<TSID>& values);
    bool insert(const std::string& key, TSID value);

    friend std::ostream& operator<<(std::ostream& os,
                                    const SortedListPageView& self);

private:
    static const int FIRST_KEY_OFFSET = 1;

    std::tuple<std::string, TSID> extract_item(unsigned int offset) const;
    static void serialize_item(const std::string& key, TSID value,
                               std::vector<uint8_t>& out);

    size_t binary_search_page(const std::string& key, TSID value,
                              bool next_key);
};

} // namespace tagtree

#endif
