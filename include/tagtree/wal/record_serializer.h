#ifndef _TAGTREE_RECORD_SERIALIZER_H_
#define _TAGTREE_RECORD_SERIALIZER_H_

#include "tagtree/wal/records.h"

#include <cstdint>
#include <vector>

namespace tagtree {

class RecordSerializer {
public:
    static LogRecordType get_record_type(const std::vector<uint8_t>& buf);
    static void serialize_series(const std::vector<SeriesRef>& series,
                                 std::vector<uint8_t>& buf);
    static void deserialize_series(const std::vector<uint8_t>& buf,
                                   std::vector<SeriesRef>& series);
};

} // namespace tagtree

#endif
