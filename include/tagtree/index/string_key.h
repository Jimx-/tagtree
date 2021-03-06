#ifndef _TAGTREE_STRING_KEY_H_
#define _TAGTREE_STRING_KEY_H_

#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <ostream>
#include <sstream>

namespace tagtree {

template <size_t N> class StringKey {
    friend class std::hash<StringKey<N>>;

public:
    StringKey() { memset(buffer.begin(), 0, N); }
    StringKey(const uint8_t* buf) { memcpy(buffer.begin(), buf, N); }

    void get_bytes(uint8_t* buf) const { memcpy(buf, buffer.begin(), N); }

    bool operator==(const StringKey<N>& rhs) const
    {
        return memcmp(buffer.begin(), rhs.buffer.begin(), N) == 0;
    }

    bool operator!=(const StringKey<N>& rhs) const { return !(*this == rhs); }

    bool operator<(const StringKey<N>& rhs) const
    {
        return memcmp(buffer.begin(), rhs.buffer.begin(), N) < 0;
    }

    bool operator>(const StringKey<N>& rhs) const
    {
        return memcmp(buffer.begin(), rhs.buffer.begin(), N) > 0;
    }

    bool operator>=(const StringKey<N>& rhs) const { return !(*this < rhs); }
    bool operator<=(const StringKey<N>& rhs) const { return !(*this > rhs); }

    StringKey<N> operator+(const StringKey<N>& rhs) const
    {
        StringKey<N> result;
        uint32_t carry = 0;

        for (size_t i = 0; i < N; i++) {
            uint32_t s = (uint32_t)buffer[i] + (uint32_t)rhs.buffer[i] + carry;

            result.buffer[i] = s & 0xff;
            carry = (s >> 8) & 0xff;
        }

        return result;
    }

    StringKey<N> operator&(const StringKey<N>& rhs) const
    {
        StringKey<N> result;

        for (size_t i = 0; i < N; i++) {
            result.buffer[i] = (buffer[i] & rhs.buffer[i]);
        }

        return result;
    }

    std::string to_string() const
    {
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for (int i = 0; i < N; ++i) {
            ss << std::setw(2) << static_cast<unsigned>(buffer[i]);
        }
        return ss.str();
    }

    friend std::ostream& operator<<(std::ostream& out, const StringKey& k)
    {
        out << k.to_string();
        return out;
    }

private:
    std::array<uint8_t, N> buffer;
};

} // namespace tagtree

namespace std {

template <size_t N> class hash<tagtree::StringKey<N>> {
public:
    size_t operator()(const tagtree::StringKey<N>& sk) const
    {
        size_t h1 = std::hash<std::string>()(
            std::string(reinterpret_cast<const char*>(sk.buffer.begin()), N));
        return h1;
    }
};

} // namespace std

#endif
