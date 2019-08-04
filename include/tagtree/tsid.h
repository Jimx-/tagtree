#ifndef _TAGTREE_TSID_H_
#define _TAGTREE_TSID_H_

#include <functional>
#include <string>
#include <uuid/uuid.h>

namespace tagtree {

/* time series identifier type */
class TSID {
public:
    TSID() { uuid_generate(uuid); }
    TSID(const char* uuid_in) { uuid_parse(uuid_in, uuid); }
    TSID(std::string_view uuid_in) { uuid_parse(uuid_in.data(), uuid); }
    TSID(const TSID& rhs) { uuid_copy(uuid, rhs.uuid); }

    bool operator==(const TSID& rhs) const
    {
        return !uuid_compare(uuid, rhs.uuid);
    }

    bool operator!=(const TSID& rhs) const { return !(*this == rhs); }

    bool operator<(const TSID& rhs) const
    {
        return uuid_compare(uuid, rhs.uuid) < 0;
    }

    std::string to_string() const
    {
        char buf[36];
        uuid_unparse(uuid, buf);

        return std::string(buf);
    }

    void serialize(char* buf) const
    {
        uuid_copy(reinterpret_cast<unsigned char*>(buf), uuid);
    }

    void deserialize(const char* buf)
    {
        uuid_copy(uuid, reinterpret_cast<const unsigned char*>(buf));
    }

private:
    uuid_t uuid;
};

} // namespace tagtree

namespace std {

template <> class hash<tagtree::TSID> {
public:
    size_t operator()(const tagtree::TSID& tsid) const
    {
        return std::hash<std::string>()(tsid.to_string());
    }
};

} // namespace std

#endif
