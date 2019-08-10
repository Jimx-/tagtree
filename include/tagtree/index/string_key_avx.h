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
    StringKey() { m128 = _mm_setzero_si128(); }
    StringKey(const uint8_t* buf)
    {
        uint8_t __buf[16] __attribute__((aligned(16)));
        ::memset(__buf, 0, 16);
        ::memcpy(__buf, buf, N);
        m128 = _mm_loadu_si128((__m128i*)buf);
    }

    void get_bytes(uint8_t* buf) const
    {
        uint8_t __buf[16] __attribute__((aligned(16)));
        _mm_storeu_si128((__m128i*)__buf, m128);
        ::memcpy(buf, __buf, N);
    }

    bool operator==(const StringKey<N>& rhs) const
    {
        return _mm_test_all_ones(_mm_cmpeq_epi8(m128, rhs.m128));
    }

    bool operator!=(const StringKey<N>& rhs) const { return !(*this == rhs); }

    bool operator<(const StringKey<N>& rhs) const
    {
        return memcmp(&m128, &rhs.m128, N) < 0;
    }

    bool operator>(const StringKey<N>& rhs) const
    {
        return memcmp(&m128, &rhs.m128, N) > 0;
    }

    bool operator>=(const StringKey<N>& rhs) const { return !(*this < rhs); }
    bool operator<=(const StringKey<N>& rhs) const { return !(*this > rhs); }

    StringKey<N> operator+(const StringKey<N>& rhs) const
    {
        StringKey<N> result;
        result.m128 = _mm_adds_epu8(m128, rhs.m128);

        return result;
    }

    StringKey<N> operator&(const StringKey<N>& rhs) const
    {
        StringKey<N> result;
        result.m128 = _mm_and_si128(m128, rhs.m128);

        return result;
    }

    friend std::ostream& operator<<(std::ostream& out, const StringKey& k)
    {
        return out;
    }

private:
    __m128i m128;
};

} // namespace tagtree

namespace std {

template <size_t N> class hash<tagtree::StringKey<N>> {
public:
    size_t operator()(const tagtree::StringKey<N>& sk) const
    {
        size_t h1 = std::hash<std::string>()(
            std::string(reinterpret_cast<const char*>(&sk.m128), N));
        return h1;
    }
};

} // namespace std

#endif
