#ifndef _TAGTREE_ITEM_PAGE_VIEW_H_
#define _TAGTREE_ITEM_PAGE_VIEW_H_

#include <cstddef>
#include <cstdint>
#include <tuple>

namespace tagtree {

class ItemPageView {
public:
    ItemPageView(uint8_t* buf, size_t size);

    void init_page();

    inline size_t get_item_count() const { return num_line_pointers(); }

    std::tuple<const uint8_t*, size_t> get_item(unsigned int offset) const;

    static const unsigned int NO_TARGET = 0;
    unsigned int put_item(uint8_t* item, size_t length, unsigned int target,
                          bool overwrite);
    bool set_item(unsigned int offset, uint8_t* item, size_t length);

protected:
    inline size_t get_free_space() const
    {
        size_t size = get_upper() - get_lower();
        return size < LINE_POINTER_SIZE ? 0 : size;
    }

private:
    static const int P_LOWER = 0;
    static const int P_UPPER = P_LOWER + sizeof(uint16_t);
    static const int P_POINTERS = P_UPPER + sizeof(uint16_t);

    struct LinePointer {
        uint16_t offset;
        uint16_t length;
        LinePointer(uint16_t offset, uint16_t length)
            : offset(offset), length(length)
        {}
    };
    static const int LINE_POINTER_SIZE = 4;

    uint8_t* buf;
    size_t size;

    inline uint16_t get_lower() const { return *(uint16_t*)&buf[P_LOWER]; };
    inline uint16_t get_upper() const { return *(uint16_t*)&buf[P_UPPER]; };
    inline void set_lower(uint16_t lower)
    {
        *(uint16_t*)&buf[P_LOWER] = lower;
    };
    inline void set_upper(uint16_t upper)
    {
        *(uint16_t*)&buf[P_UPPER] = upper;
    };

    inline size_t num_line_pointers() const
    {
        auto lower = get_lower();
        return lower < P_POINTERS ? 0
                                  : (lower - P_POINTERS) / LINE_POINTER_SIZE;
    }

    LinePointer get_line_pointer(unsigned int offset) const;
    void put_line_pointer(unsigned int offset, const LinePointer& lp);
};

} // namespace tagtree

#endif
