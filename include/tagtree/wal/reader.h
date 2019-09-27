#ifndef _TAGTREE_WAL_READER_H_
#define _TAGTREE_WAL_READER_H_

#include <memory>
#include <string>
#include <vector>

namespace tagtree {

class WALReader {
public:
    WALReader(std::string_view filename);
    ~WALReader();

    bool get_next(std::vector<uint8_t>& record);

private:
    std::string filename;
    int fd;
    std::unique_ptr<uint8_t[]> buf;

    size_t segment_offset;
    size_t page_offset;
    bool eof;

    void read_page();
};

} // namespace tagtree

#endif
