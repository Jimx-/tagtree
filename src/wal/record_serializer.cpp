#include "tagtree/wal/record_serializer.h"

#include <cstring>

namespace tagtree {

LogRecordType RecordSerializer::get_record_type(const std::vector<uint8_t>& buf)
{
    uint32_t type = *(uint32_t*)&buf[0];

    switch (type) {
    case LRT_SERIES:
        return (LogRecordType)type;
    default:
        return LRT_NONE;
    }
}

void RecordSerializer::serialize_series(const std::vector<SeriesRef>& series,
                                        std::vector<uint8_t>& buf)
{
    size_t bufsize = 0;
    bufsize += sizeof(uint32_t);
    for (auto&& p : series) {
        bufsize += sizeof(TSID) + sizeof(uint16_t) +
                   (p.labels.size() * 2 * sizeof(uint16_t));
        for (auto&& label : p.labels) {
            bufsize += label.name.size() + label.value.size();
        }
    }

    buf.resize(bufsize);
    uint8_t* pb = &buf[0];

    *(uint32_t*)pb = (uint32_t)LRT_SERIES;
    pb += sizeof(uint32_t);

    for (auto&& p : series) {
        *(TSID*)pb = p.tsid;
        pb += sizeof(TSID);
        *(uint16_t*)pb = p.labels.size();
        pb += sizeof(uint16_t);

        for (auto&& label : p.labels) {
            *(uint16_t*)pb = label.name.length();
            pb += sizeof(uint16_t);
            ::memcpy(pb, label.name.c_str(), label.name.length());
            pb += label.name.length();

            *(uint16_t*)pb = label.value.length();
            pb += sizeof(uint16_t);
            ::memcpy(pb, label.value.c_str(), label.value.length());
            pb += label.value.length();
        }
    }
}

void RecordSerializer::deserialize_series(const std::vector<uint8_t>& buf,
                                          std::vector<SeriesRef>& series)
{
    const uint8_t* pb = &buf[0];
    const uint8_t* lim = &buf[buf.size()];

    pb += sizeof(uint32_t);
    series.clear();

    while (pb < lim) {
        TSID tsid = *(TSID*)pb;
        pb += sizeof(TSID);
        uint16_t length = *(uint16_t*)pb;
        pb += sizeof(uint16_t);

        std::vector<promql::Label> labels;

        while (length--) {
            uint16_t slen;
            slen = *(uint16_t*)pb;
            pb += sizeof(uint16_t);
            std::string name((char*)pb, slen);
            pb += slen;
            slen = *(uint16_t*)pb;
            pb += sizeof(uint16_t);
            std::string value((char*)pb, slen);
            pb += slen;

            labels.emplace_back(name, value);
        }

        series.emplace_back(tsid, std::move(labels));
    }
}

} // namespace tagtree
