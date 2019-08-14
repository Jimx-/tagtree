#include "tagtree/series/symbol_table.h"

#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace tagtree {

SymbolTable::SymbolTable(std::string_view filename) : filename(filename)
{
    fd = -1;

    open_symtab();
    load_symtab();
}

SymbolTable::Ref SymbolTable::add_symbol(std::string_view symbol)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    auto it = symbol_map.find(symbol.data());

    if (it == symbol_map.end()) {
        Ref ref = symbols.size();
        symbols.push_back(symbol.data());
        symbol_map.emplace(symbol.data(), ref);
        write_symbol(symbol, lock);
        return ref;
    }

    return it->second;
}

const std::string& SymbolTable::get_symbol(Ref ref)
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    return symbols[ref];
}

void SymbolTable::open_symtab()
{
    struct stat sbuf;
    int err = ::stat(filename.c_str(), &sbuf);

    if (err < 0 && errno == ENOENT) {
        create_symtab();
        return;
    }

    if (err < 0) {
        fd = -1;
        throw std::runtime_error("unable to get symbol table file status");
    }

    fd = ::open(filename.c_str(), O_RDWR);
    if (fd < 0) {
        fd = -1;
        throw std::runtime_error("unable to open symbol table file");
    }
}

void SymbolTable::create_symtab()
{
    fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_EXCL,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0) {
        fd = -1;
        throw std::runtime_error("unable to create symbol table file");
    }

    lseek(fd, 0, SEEK_SET);
    uint32_t magic = MAGIC;
    write(fd, &magic, sizeof(magic));
}

void SymbolTable::load_symtab()
{
    char buf[4096];
    const char* p;
    size_t buflen;
    uint32_t magic;
    off_t offset = 0;

    lseek(fd, 0, SEEK_SET);
    buflen = read(fd, buf, sizeof(buf));
    p = buf;

    if (buflen < sizeof(magic)) {
        throw std::runtime_error("symbol table file corrupted");
    }

    magic = *(const uint32_t*)p;
    if (magic != MAGIC) {
        throw std::runtime_error("symbol table file corrupted");
    }

    p += sizeof(uint32_t);
    offset += sizeof(uint32_t);

    while (p < &buf[buflen]) {
        size_t remaining = &buf[buflen] - p;
        if (remaining < sizeof(uint32_t)) {
            lseek(fd, offset, SEEK_SET);
            buflen = read(fd, buf, sizeof(buf));
            if (buflen < sizeof(uint32_t)) {
                throw std::runtime_error("symbol table file corrupted");
            }
            p = buf;
            continue;
        }

        size_t length = *(const uint32_t*)p;
        p += sizeof(uint32_t);
        remaining -= sizeof(uint32_t);
        if (remaining < length) {
            lseek(fd, offset, SEEK_SET);
            buflen = read(fd, buf, sizeof(buf));
            if (buflen < sizeof(uint32_t) + length) {
                throw std::runtime_error("symbol table file corrupted");
            }
            p = buf;
            continue;
        }

        offset += sizeof(uint32_t);
        std::string symbol(p, p + length);

        size_t idx = symbols.size();
        symbols.push_back(symbol);
        symbol_map.emplace(symbol, idx);

        p += length;
        offset += length;
    }
}

void SymbolTable::write_symbol(std::string_view symbol,
                               std::unique_lock<std::shared_mutex>&)
{
    auto buf = std::make_unique<uint8_t[]>(symbol.length() + sizeof(uint32_t));
    auto* p = buf.get();

    *(uint32_t*)p = (uint32_t)symbol.length();
    p += sizeof(uint32_t);

    ::memcpy(p, symbol.data(), symbol.length());
    lseek(fd, 0, SEEK_END);
    size_t written = write(fd, buf.get(), symbol.length() + sizeof(uint32_t));

    if (written != sizeof(uint32_t) + symbol.length()) {
        throw std::runtime_error("failed to write symbol entry");
    }
}

} // namespace tagtree
