#ifndef _TAGTREE_TUPLE_KEY_H_
#define _TAGTREE_TUPLE_KEY_H_

#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <ostream>

#include <emmintrin.h>
#include <smmintrin.h>

namespace tagtree {

namespace detail {

template <int N, int align> constexpr int roundup()
{
    if (N % align)
        return N + align - (N % align);
    else
        return N;
}

}; // namespace detail

template <size_t NB, size_t VB> class TupleKey {
    friend class std::hash<TupleKey<NB, VB>>;

public:
    static const size_t KEY_LENGTH = NB + VB + 8 + 4;

    TupleKey() { ::memset(buf, 0, sizeof(buf)); }
    TupleKey(const uint8_t* data)
    {
        ::memset(buf, 0, sizeof(buf));
        ::memcpy(buf, data, KEY_LENGTH);
    }

    void get_bytes(uint8_t* data) const { ::memcpy(data, buf, KEY_LENGTH); }

    void get_tag_name(uint8_t* data) const { ::memcpy(data, buf, NB); }
    void get_tag_value(uint8_t* data) const { ::memcpy(data, &buf[NB], VB); }
    uint64_t get_timestamp() const { return *(uint64_t*)&buf[NB + VB]; }
    unsigned int get_segnum() const
    {
        return *(unsigned int*)&buf[NB + VB + 8];
    }

    void clear_tag_name() { ::memset(buf, 0, NB); }
    void clear_tag_value() { ::memset(&buf[NB], 0, VB); }

    void set_tag_name(uint8_t* data) { ::memcpy(buf, data, NB); }
    void set_tag_value(uint8_t* data) { ::memcpy(&buf[NB], data, VB); }
    void set_timestamp(uint64_t timestamp)
    {
        *(uint64_t*)&buf[NB + VB] = timestamp;
    }
    void set_segnum(unsigned int seg)
    {
        *(unsigned int*)&buf[NB + VB + 8] = seg;
    }

    bool operator==(const TupleKey<NB, VB>& rhs) const
    {
        for (int i = 0; i < BUF_LENGTH; i += 16) {
            __m128i a, b;
            a = _mm_loadu_si128((__m128i*)&buf[i]);
            b = _mm_loadu_si128((__m128i*)&rhs.buf[i]);
            if (!_mm_test_all_ones(_mm_cmpeq_epi8(a, b))) return false;
        }

        return true;
    }

    bool operator!=(const TupleKey<NB, VB>& rhs) const
    {
        return !(*this == rhs);
    }

    bool operator<(const TupleKey<NB, VB>& rhs) const
    {
        auto name_cmp = memcmp(buf, rhs.buf, NB);
        auto value_cmp = memcmp(&buf[NB], &rhs.buf[NB], VB);

        if (name_cmp < 0) return true;
        if (name_cmp > 0) return false;

        if (value_cmp < 0) return true;
        if (value_cmp > 0) return false;

        if (get_timestamp() < rhs.get_timestamp()) return true;
        if (get_timestamp() > rhs.get_timestamp()) return false;

        return get_segnum() > rhs.get_segnum();
    }

    bool operator>(const TupleKey<NB, VB>& rhs) const
    {
        auto name_cmp = memcmp(buf, rhs.buf, NB);
        auto value_cmp = memcmp(&buf[NB], &rhs.buf[NB], VB);

        if (name_cmp > 0) return true;
        if (name_cmp < 0) return false;

        if (value_cmp > 0) return true;
        if (value_cmp < 0) return false;

        if (get_timestamp() > rhs.get_timestamp()) return true;
        if (get_timestamp() < rhs.get_timestamp()) return false;

        return get_segnum() < rhs.get_segnum();
    }

    bool operator>=(const TupleKey<NB, VB>& rhs) const
    {
        return !(*this < rhs);
    }
    bool operator<=(const TupleKey<NB, VB>& rhs) const
    {
        return !(*this > rhs);
    }

    std::string to_string() const
    {
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for (int i = 0; i < KEY_LENGTH; ++i) {
            ss << std::setw(2) << static_cast<unsigned>(buf[i]);
        }
        return ss.str();
    }

    friend std::ostream& operator<<(std::ostream& out,
                                    const TupleKey<NB, VB>& k)
    {
        out << k.to_string();
        return out;
    }

private:
    static const size_t BUF_LENGTH = detail::roundup<KEY_LENGTH, 16>();

    uint8_t buf[BUF_LENGTH];
};

} // namespace tagtree

namespace std {

template <size_t NB, size_t VB> class hash<tagtree::TupleKey<NB, VB>> {
public:
    size_t operator()(const tagtree::TupleKey<NB, VB>& sk) const
    {
        size_t h1 = std::hash<std::string>()(
            std::string(reinterpret_cast<const char*>(&sk.buf),
                        tagtree::TupleKey<NB, VB>::KEY_LENGTH));
        return h1;
    }
};

} // namespace std

#endif
