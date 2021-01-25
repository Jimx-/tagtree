#include "tagtree/tree/item_page_view.h"

#include <cassert>
#include <cstring>

namespace tagtree {

ItemPageView::ItemPageView(uint8_t* buf, size_t size) : buf(buf), size(size) {}

void ItemPageView::init_page()
{
    ::memset(buf, 0, size);
    set_lower(P_POINTERS);
    set_upper(size);
}

ItemPageView::LinePointer
ItemPageView::get_line_pointer(unsigned int offset) const
{
    uint8_t* ptr = &buf[P_POINTERS + (offset - 1) * LINE_POINTER_SIZE];
    auto off = *(uint16_t*)ptr;
    auto len = *(uint16_t*)&ptr[sizeof(uint16_t)];
    return LinePointer{off, len};
}

std::tuple<const uint8_t*, size_t>
ItemPageView::get_item(unsigned int offset) const
{
    auto lp = get_line_pointer(offset);
    auto ptr = &buf[lp.offset];
    return std::tie(ptr, lp.length);
}

void ItemPageView::put_line_pointer(unsigned int offset, const LinePointer& lp)
{
    uint16_t* ptr =
        (uint16_t*)&buf[P_POINTERS + (offset - 1) * LINE_POINTER_SIZE];
    *ptr = lp.offset;
    *(ptr + 1) = lp.length;
}

unsigned int ItemPageView::put_item(uint8_t* item, size_t length,
                                    unsigned int target, bool overwrite)
{
    auto lower = get_lower();
    auto upper = get_upper();

    assert(lower >= P_POINTERS);
    assert(lower <= upper);
    assert(upper <= length);

    upper -= length;
    auto lp = LinePointer{upper, (uint16_t)length};

    auto limit = num_line_pointers() + 1;
    auto offset = target > 0 ? target : limit;

    if (offset > limit) return -1;

    auto need_shuffle = !overwrite && offset < limit;
    if (need_shuffle) {
        uint8_t* ptr =
            (uint8_t*)&buf[P_POINTERS + (offset - 1) * LINE_POINTER_SIZE];
        ::memmove(ptr + LINE_POINTER_SIZE, ptr,
                  (limit - offset) * LINE_POINTER_SIZE);
    }

    put_line_pointer(offset, lp);
    if (offset == limit || need_shuffle) lower += LINE_POINTER_SIZE;

    ::memcpy(&buf[upper], item, length);

    set_lower(lower);
    set_upper(upper);

    return offset;
}

bool ItemPageView::set_item(unsigned int offset, uint8_t* item, size_t length)
{
    auto lp = get_line_pointer(offset);
    if (length != lp.length) return false;

    ::memcpy(&buf[lp.offset], item, length);
    return true;
}

} // namespace tagtree
