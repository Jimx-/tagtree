#include "tagtree/series/series_file.h"

#include "CRC.h"

#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace tagtree {

SeriesFile::SeriesFile(std::string_view filename, bool create,
                       size_t segment_size)
    : filename(filename), segment_size(segment_size), last_page(nullptr)
{
    fd = -1;

    offset_table = std::make_unique<uint32_t[]>(segment_size);
    open(create);
}

SeriesFile::~SeriesFile()
{
    if (is_open()) {
        close();
    }
}

size_t SeriesFile::get_header_size() const
{
    size_t hdr_size = (3 + segment_size) * sizeof(uint32_t);
    if (hdr_size % PAGE_SIZE) {
        hdr_size = (hdr_size / PAGE_SIZE + 1) * PAGE_SIZE;
    }
    return hdr_size;
}

bool SeriesFile::read_entry(unsigned int i, RefSeriesEntry* entry)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto offset = offset_table[i];

    if (!offset) {
        return false;
    }

    auto pg_offset = offset - (offset % PAGE_SIZE);

    const uint8_t* page_buf = nullptr;
    auto it = page_cache.find(pg_offset);
    if (it != page_cache.end()) {
        page_buf = it->second.get();
    } else {
        it = write_pages.find(pg_offset);
        if (it != write_pages.end()) {
            page_buf = it->second.get();
        } else {
            page_buf = read_page(pg_offset);
        }
    }

    page_buf += (offset % PAGE_SIZE);

    uint16_t num_labels = *(uint16_t*)page_buf;
    uint32_t crc = CRC::Calculate(
        page_buf, num_labels * 2 * sizeof(SymbolTable::Ref) + sizeof(uint16_t),
        CRC::CRC_32());
    page_buf += sizeof(uint16_t);

    while (num_labels--) {
        SymbolTable::Ref name_ref = *(SymbolTable::Ref*)page_buf;
        page_buf += sizeof(SymbolTable::Ref);
        SymbolTable::Ref value_ref = *(SymbolTable::Ref*)page_buf;
        page_buf += sizeof(SymbolTable::Ref);
        entry->labels.emplace_back(name_ref, value_ref);
    }

    uint32_t crc_file = *(uint32_t*)page_buf;
    if (crc != crc_file) {
        throw std::runtime_error("series entry corrupted(bad checksum)");
    }

    return true;
}

void SeriesFile::write_entry(unsigned int i, RefSeriesEntry* entry)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (!last_page) open_page();
    size_t entry_size = sizeof(uint16_t) +
                        sizeof(SymbolTable::Ref) * 2 * entry->labels.size() +
                        sizeof(uint32_t);

    if (PAGE_SIZE - page_alloc < entry_size) {
        page_offset += PAGE_SIZE;
        open_page();
    }

    offset_table[i] = page_offset + page_alloc;

    auto buf = last_page + page_alloc;
    auto p = buf;

    *(uint16_t*)p = (uint16_t)entry->labels.size();
    p += sizeof(uint16_t);
    for (auto&& label : entry->labels) {
        *(SymbolTable::Ref*)p = label.first;
        p += sizeof(SymbolTable::Ref);
        *(SymbolTable::Ref*)p = label.second;
        p += sizeof(SymbolTable::Ref);
    }

    uint32_t crc = CRC::Calculate(buf, p - buf, CRC::CRC_32());
    *(uint32_t*)p = crc;

    page_alloc += entry_size;
}

void SeriesFile::create()
{
    fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_EXCL,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0) {
        fd = -1;
        throw std::runtime_error("unable to create series file");
    }

    int err = ftruncate(fd, get_header_size());
    if (err != 0) {
        fd = -1;
        throw std::runtime_error("unable to resize series file");
    }

    page_offset = get_header_size();
    ::memset(offset_table.get(), 0, sizeof(uint32_t) * segment_size);
    write_header();
}

void SeriesFile::open(bool create)
{
    struct stat sbuf;
    int err = ::stat(filename.c_str(), &sbuf);

    if (err < 0 && errno == ENOENT) {
        if (create) {
            this->create();
            return;
        }
    }

    if (err < 0) {
        fd = -1;
        throw std::runtime_error("unable to get series file status");
    }

    fd = ::open(filename.c_str(), O_RDWR);
    if (fd < 0) {
        fd = -1;
        throw std::runtime_error("unable to open series file");
    }

    page_offset = lseek(fd, 0, SEEK_END);

    if (page_offset % PAGE_SIZE) {
        if (page_offset < get_header_size()) {
            throw std::runtime_error("series file corrupted(bad header)");
        }

        std::vector<char> zero_pad(PAGE_SIZE - (page_offset % PAGE_SIZE), 0);
        lseek(fd, 0, SEEK_END);
        write(fd, &zero_pad[0], zero_pad.size());

        page_offset += zero_pad.size();
    }

    read_header();
}

void SeriesFile::close()
{
    ::close(fd);
    fd = -1;
}

void SeriesFile::read_header()
{
    uint32_t magic;
    size_t offset_table_size = sizeof(uint32_t) * segment_size;

    lseek(fd, 0, SEEK_SET);
    read(fd, &magic, sizeof(magic));

    if (magic != MAGIC) {
        throw std::runtime_error("series file corrupted(bad magic)");
    }

    read(fd, offset_table.get(), sizeof(uint32_t) * segment_size);
    uint32_t crc = CRC::Calculate(
        offset_table.get(), sizeof(uint32_t) * segment_size, CRC::CRC_32());
    uint32_t crc_file;
    read(fd, &crc_file, sizeof(crc_file));

    if (crc_file != crc) {
        throw std::runtime_error("series file corrupted(bad checksum)");
    }
}

void SeriesFile::write_header()
{
    const uint32_t magic = MAGIC;
    size_t offset_table_size = sizeof(uint32_t) * segment_size;

    lseek(fd, 0, SEEK_SET);
    write(fd, &magic, sizeof(magic));
    write(fd, offset_table.get(), sizeof(uint32_t) * segment_size);
    uint32_t crc = CRC::Calculate(
        offset_table.get(), sizeof(uint32_t) * segment_size, CRC::CRC_32());
    write(fd, &crc, sizeof(crc));
}

void SeriesFile::open_page()
{
    auto page = std::make_unique<uint8_t[]>(PAGE_SIZE);
    last_page = page.get();
    write_pages[page_offset] = std::move(page);
    page_alloc = 0;
}

const uint8_t* SeriesFile::read_page(off_t page_offset)
{
    auto page = std::make_unique<uint8_t[]>(PAGE_SIZE);
    auto pp = page.get();

    lseek(fd, page_offset, SEEK_SET);
    read(fd, page.get(), PAGE_SIZE);
    page_cache[page_offset] = std::move(page);

    return pp;
}

void SeriesFile::flush()
{
    std::lock_guard<std::mutex> lock(mutex);

    if (write_pages.empty()) return;

    int err = ftruncate(fd, write_pages.crbegin()->first + PAGE_SIZE);
    if (err != 0) {
        fd = -1;
        throw std::runtime_error("unable to resize series file");
    }

    for (auto&& p : write_pages) {
        lseek(fd, p.first, SEEK_SET);

        ssize_t retval;
        retval = write(fd, p.second.get(), PAGE_SIZE);

        if (retval != PAGE_SIZE) {
            throw std::runtime_error("failed to write series file");
        }
    }

    write_header();
    fsync(fd);

    last_page = nullptr;

    for (auto&& p : write_pages) {
        page_cache[p.first] = std::move(p.second);
    }

    write_pages.clear();
}

} // namespace tagtree
