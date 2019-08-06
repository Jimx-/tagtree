#ifndef _TAGTREE_MONGODB_SERIES_MANAGER_H_
#define _TAGTREE_MONGODB_SERIES_MANAGER_H_

#include "tagtree/series/series_manager.h"

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <unordered_map>

namespace tagtree {

class MongoDBSeriesManager : public AbstractSeriesManager {
public:
    MongoDBSeriesManager(size_t cache_size, bool create_instance = true,
                         std::string_view url = "",
                         std::string_view database_name = DATABASE_NAME);

private:
    static const std::string DATABASE_NAME;
    static const std::string COLLECTION_NAME;

    mongocxx::client client;
    std::unique_ptr<mongocxx::instance> inst;

    std::string database_name;

    virtual bool read_entry(SeriesEntry* entry);
    virtual void write_entry(SeriesEntry* entry);
};

} // namespace tagtree

#endif
