#ifndef _TAGTREE_STRING_KEY_AVX_H_
#define _TAGTREE_STRING_KEY_AVX_H_

#include <array>
#include <cstdint>
#include <cstring>
#include <ostream>

#include <emmintrin.h>
#include <smmintrin.h>

namespace tagtree {

template <size_t N> class StringKey {
    friend class std::hash<StringKey<N>>;

public:
    StringKey() { ::memset(buf, 0, 16); }
    StringKey(const uint8_t* data)
    {
        ::memset(buf, 0, 16);
        ::memcpy(buf, data, N);
    }

    void get_bytes(uint8_t* data) const { ::memcpy(data, buf, N); }

    bool operator==(const StringKey<N>& rhs) const
    {
        __m128i a, b;
        a = _mm_loadu_si128((__m128i*)buf);
        b = _mm_loadu_si128((__m128i*)rhs.buf);
        return _mm_test_all_ones(_mm_cmpeq_epi8(a, b));
    }

    bool operator!=(const StringKey<N>& rhs) const { return !(*this == rhs); }

    bool operator<(const StringKey<N>& rhs) const
    {
        return memcmp(&buf, &rhs.buf, N) < 0;
    }

    bool operator>(const StringKey<N>& rhs) const
    {
        return memcmp(&buf, &rhs.buf, N) > 0;
    }

    bool operator>=(const StringKey<N>& rhs) const { return !(*this < rhs); }
    bool operator<=(const StringKey<N>& rhs) const { return !(*this > rhs); }

    StringKey<N> operator+(const StringKey<N>& rhs) const
    {
        StringKey<N> result;
        __m128i a, b, c;
        a = _mm_loadu_si128((__m128i*)buf);
        b = _mm_loadu_si128((__m128i*)rhs.buf);
        c = _mm_adds_epu8(a, b);
        _mm_storeu_si128((__m128i*)result.buf, c);

        return result;
    }

    StringKey<N> operator&(const StringKey<N>& rhs) const
    {
        StringKey<N> result;
        __m128i a, b, c;
        a = _mm_loadu_si128((__m128i*)buf);
        b = _mm_loadu_si128((__m128i*)rhs.buf);
        c = _mm_and_si128(a, b);
        _mm_storeu_si128((__m128i*)result.buf, c);

        return result;
    }

    friend std::ostream& operator<<(std::ostream& out, const StringKey& k)
    {
        return out;
    }

private:
    uint8_t buf[16];
};

} // namespace tagtree

namespace std {

template <size_t N> class hash<tagtree::StringKey<N>> {
public:
    size_t operator()(const tagtree::StringKey<N>& sk) const
    {
        size_t h1 = std::hash<std::string>()(
            std::string(reinterpret_cast<const char*>(&sk.buf), N));
        return h1;
    }
};

} // namespace std

#endif
