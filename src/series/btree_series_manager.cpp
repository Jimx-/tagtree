#include "tagtree/series/btree_series_manager.h"
#include "bptree/heap_page_cache.h"

#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>

namespace tagtree {

BTreeSeriesManager::BTreeSeriesManager(size_t cache_size,
                                       std::string_view filename,
                                       std::string_view index_file)
    : AbstractSeriesManager(cache_size), filename(filename),
      page_cache(std::make_unique<bptree::HeapPageCache>(index_file, true,
                                                         cache_size)),
      btree(std::make_unique<BPTree>(page_cache.get()))
{
    open_db();
}

bool BTreeSeriesManager::read_entry(RefSeriesEntry* entry)
{
    std::vector<off_t> entry_offsets;
    btree->get_value(entry->tsid, entry_offsets);

    if (entry_offsets.size() == 0) {
        return false;
    }

    assert(entry_offsets.size() == 1);

    off_t entry_offset = entry_offsets.front();
    lseek(fd, entry_offset, SEEK_SET);
    uint32_t entry_len;
    read(fd, &entry_len, sizeof(entry_len));

    auto buf = std::make_unique<uint8_t[]>(entry_len);
    read(fd, buf.get(), entry_len);
    auto* p = buf.get();

    entry->tsid = *(TSID*)p;
    p += sizeof(TSID);
    entry_len -= sizeof(TSID);

    size_t num_labels = entry_len / (2 * sizeof(SymbolTable::Ref));
    while (num_labels--) {
        SymbolTable::Ref name_ref = *(SymbolTable::Ref*)p;
        p += sizeof(SymbolTable::Ref);
        SymbolTable::Ref value_ref = *(SymbolTable::Ref*)p;
        p += sizeof(SymbolTable::Ref);
        entry->labels.emplace_back(name_ref, value_ref);
    }

    return true;
}

void BTreeSeriesManager::write_entry(RefSeriesEntry* entry)
{
    size_t entry_len =
        sizeof(TSID) + entry->labels.size() * 2 * sizeof(SymbolTable::Ref);
    auto buf = std::make_unique<uint8_t[]>(entry_len + sizeof(uint32_t));
    auto* p = buf.get();

    *(uint32_t*)p = (uint32_t)entry_len;
    p += sizeof(uint32_t);
    *(TSID*)p = entry->tsid;
    p += sizeof(TSID);
    for (auto&& label : entry->labels) {
        *(SymbolTable::Ref*)p = label.first;
        p += sizeof(SymbolTable::Ref);
        *(SymbolTable::Ref*)p = label.second;
        p += sizeof(SymbolTable::Ref);
    }

    off_t addr = lseek(fd, 0, SEEK_END);
    size_t written = write(fd, buf.get(), entry_len + sizeof(uint32_t));

    if (written != sizeof(uint32_t) + entry_len) {
        throw std::runtime_error("failed to write series entry");
    }

    btree->insert(entry->tsid, addr);
}

void BTreeSeriesManager::open_db()
{
    struct stat sbuf;
    int err = ::stat(filename.c_str(), &sbuf);

    if (err < 0 && errno == ENOENT) {
        create_db();
        return;
    }

    if (err < 0) {
        fd = -1;
        throw std::runtime_error("unable to get series entry file status");
    }

    fd = ::open(filename.c_str(), O_RDWR);
    if (fd < 0) {
        fd = -1;
        throw std::runtime_error("unable to open series entry file");
    }

    lseek(fd, 0, SEEK_SET);
    uint32_t magic = 0;
    read(fd, &magic, sizeof(magic));
    if (magic != MAGIC) {
        throw std::runtime_error("series entry file corrupted");
    }
}

void BTreeSeriesManager::create_db()
{
    fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_EXCL,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0) {
        fd = -1;
        throw std::runtime_error("unable to create series entry database");
    }

    lseek(fd, 0, SEEK_SET);
    uint32_t magic = MAGIC;
    write(fd, &magic, sizeof(magic));
}

} // namespace tagtree
