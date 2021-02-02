#ifndef _TAGTREE_SORTED_LIST_PAGE_VIEW_H_
#define _TAGTREE_SORTED_LIST_PAGE_VIEW_H_

#include "tagtree/series/symbol_table.h"
#include "tagtree/tree/item_page_view.h"
#include "tagtree/tsid.h"

#include <iostream>
#include <string>

namespace tagtree {

class SortedListPageView : public ItemPageView {
public:
    SortedListPageView(uint8_t* buf, size_t size) : ItemPageView(buf, size) {}

    void get_values(SymbolTable::Ref key, std::vector<TSID>& values);
    void scan_values(std::function<bool(SymbolTable::Ref)> pred,
                     std::vector<TSID>& values);
    bool insert(SymbolTable::Ref key, TSID value);

    friend std::ostream& operator<<(std::ostream& os,
                                    const SortedListPageView& self);

private:
    static const int FIRST_KEY_OFFSET = 1;

    std::tuple<SymbolTable::Ref, TSID> extract_item(unsigned int offset) const;
    static void serialize_item(SymbolTable::Ref key, TSID value,
                               std::vector<uint8_t>& out);

    size_t binary_search_page(SymbolTable::Ref key, TSID value, bool next_key);
};

} // namespace tagtree

#endif
