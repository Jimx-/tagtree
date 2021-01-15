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

template <size_t KN, size_t VN> class TupleKey {
    friend class std::hash<TupleKey<KN, VN>>;

public:
    static const size_t KEY_LENGTH = KN + 8 + VN;

    TupleKey() { ::memset(buf, 0, sizeof(buf)); }
    TupleKey(const uint8_t* data)
    {
        ::memset(buf, 0, sizeof(buf));
        ::memcpy(buf, data, KEY_LENGTH);
    }

    void get_bytes(uint8_t* data) const { ::memcpy(data, buf, KEY_LENGTH); }

    void get_tag_key(uint8_t* data) const { ::memcpy(data, buf, KN); }
    void get_tag_value(uint8_t* data) const
    {
        ::memcpy(data, &buf[KN + 8], VN);
    }
    uint64_t get_timestamp() const { return *(uint64_t*)&buf[KN]; }

    void set_tag_key(uint8_t* data) { ::memcpy(buf, data, KN); }
    void set_tag_value(uint8_t* data) { ::memcpy(&buf[KN + 8], data, VN); }
    void set_timestamp(uint64_t timestamp) { *(uint64_t*)&buf[KN] = timestamp; }

    bool operator==(const TupleKey<KN, VN>& rhs) const
    {
        for (int i = 0; i < BUF_LENGTH; i += 16) {
            __m128i a, b;
            a = _mm_loadu_si128((__m128i*)&buf[i]);
            b = _mm_loadu_si128((__m128i*)&rhs.buf[i]);
            if (!_mm_test_all_ones(_mm_cmpeq_epi8(a, b))) return false;
        }

        return true;
    }

    bool operator!=(const TupleKey<KN, VN>& rhs) const
    {
        return !(*this == rhs);
    }

    bool operator<(const TupleKey<KN, VN>& rhs) const
    {
        if (memcmp(buf, rhs.buf, KN) < 0) return true;
        if (get_timestamp() > rhs.get_timestamp()) return true;
        return memcpy(&buf[KN + 8], &rhs.buf[KN + 8], VN) < 0;
    }

    bool operator>(const TupleKey<KN, VN>& rhs) const
    {
        if (memcmp(buf, rhs.buf, KN) > 0) return true;
        if (get_timestamp() < rhs.get_timestamp()) return true;
        return memcpy(&buf[KN + 8], &rhs.buf[KN + 8], VN) > 0;
    }

    bool operator>=(const TupleKey<KN, VN>& rhs) const
    {
        return !(*this < rhs);
    }
    bool operator<=(const TupleKey<KN, VN>& rhs) const
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
                                    const TupleKey<KN, VN>& k)
    {
        out << k.to_string();
        return out;
    }

private:
    static const size_t BUF_LENGTH = detail::roundup<KEY_LENGTH, 16>();

    static const uint8_t buf[BUF_LENGTH];
};

} // namespace tagtree

namespace std {

template <size_t KN, size_t VN> class hash<tagtree::TupleKey<KN, VN>> {
public:
    size_t operator()(const tagtree::TupleKey<KN, VN>& sk) const
    {
        size_t h1 = std::hash<std::string>()(
            std::string(reinterpret_cast<const char*>(&sk.buf),
                        tagtree::TupleKey<KN, VN>::KEY_LENGTH));
        return h1;
    }
};

} // namespace std

#endif
