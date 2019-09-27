#include "tagtree/wal/reader.h"
#include "tagtree/wal/wal.h"

#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include <iostream>

namespace tagtree {

WALReader::WALReader(std::string_view filename) : filename(filename)
{
    fd = ::open(this->filename.c_str(), O_RDONLY);
    buf = std::make_unique<uint8_t[]>(WAL::PAGE_SIZE);
    eof = false;

    read_page();
}

WALReader::~WALReader()
{
    if (fd != -1) {
        ::close(fd);
    }
}

void WALReader::read_page()
{
    ssize_t retval;

    page_offset = 0;

    do {
        retval = ::read(fd, buf.get(), WAL::PAGE_SIZE);
    } while (retval == -1 && errno == EINTR);

    if (retval == -1) {
        throw std::runtime_error("failed to read WAL page");
    }

    eof = !retval;

    if (retval < WAL::PAGE_SIZE) {
        ::memset(&buf[retval], 0, WAL::PAGE_SIZE - retval);
    }
}

bool WALReader::get_next(std::vector<uint8_t>& record)
{
    if (eof) return false;

    record.clear();
    while (true) {
        if (page_offset + WAL::RECORD_HEADER_SIZE >= WAL::PAGE_SIZE) {
            read_page();

            if (eof) {
                return false;
            }
        }

        uint8_t record_type = buf[page_offset++];

        if (record_type == WAL::LR_NONE) {
            page_offset = WAL::PAGE_SIZE;
            continue;
        }

        uint16_t length;
        memcpy(&length, &buf[page_offset], sizeof(uint16_t));
        length = be16toh(length);
        page_offset += sizeof(uint16_t);
        uint32_t crc = *(uint32_t*)&buf[page_offset];
        page_offset += sizeof(uint32_t);

        size_t reclen = record.size();
        record.resize(reclen + length);
        ::memcpy(&record[reclen], &buf[page_offset], length);

        page_offset += length;

        if (record_type == WAL::LR_FULL || record_type == WAL::LR_LAST)
            return true;
    }
}

} // namespace tagtree
