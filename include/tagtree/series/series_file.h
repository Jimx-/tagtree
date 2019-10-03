#ifndef _TAGTREE_SERIES_FILE_H_
#define _TAGTREE_SERIES_FILE_H_

#include "series_manager.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace tagtree {

class SeriesFile {
public:
    explicit SeriesFile(std::string_view filename, bool create,
                        size_t segment_size);
    ~SeriesFile();

    bool is_open() const { return fd != -1; }

    bool read_entry(unsigned int i, RefSeriesEntry* entry);
    void write_entry(unsigned int i, RefSeriesEntry* entry);

    void flush();

private:
    static const uint32_t MAGIC = 0xDEADBEEF;
    static const size_t PAGE_SIZE = 4096;

    int fd;
    std::string filename;
    std::mutex mutex;
    size_t segment_size;
    std::unique_ptr<uint32_t[]> offset_table;
    off_t page_offset;
    size_t page_alloc;
    std::map<unsigned int, std::unique_ptr<uint8_t[]>> write_pages;
    std::map<unsigned int, std::unique_ptr<uint8_t[]>> page_cache;
    uint8_t* last_page;

    size_t get_header_size() const;

    void create();
    void open(bool create);
    void close();

    void read_header();
    void write_header();

    void open_page();
    const uint8_t* read_page(off_t page_offset);
};

} // namespace tagtree

#endif
