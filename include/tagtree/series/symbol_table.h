#ifndef _TAGTREE_SYMBOL_TABLE_H_
#define _TAGTREE_SYMBOL_TABLE_H_

#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace tagtree {

class SymbolTable {
public:
    using Ref = uint32_t;

    SymbolTable(std::string_view filename);
    ~SymbolTable();

    Ref add_symbol(std::string_view symbol);
    const std::string& get_symbol(Ref ref);

    void flush();

private:
    static const uint32_t MAGIC = 0x5453594d;
    int fd;
    std::string filename;
    std::vector<std::string> symbols;
    std::unordered_map<std::string, Ref> symbol_map;
    std::shared_mutex mutex;
    size_t last_flushed_ref;

    void open_symtab();
    void create_symtab();
    void load_symtab();

    void write_symbol(std::string_view symbol,
                      std::unique_lock<std::shared_mutex>&);
};

} // namespace tagtree

#endif
