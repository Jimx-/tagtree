#include "tagtree/series/mongodb_series_manager.h"

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/types.hpp>

#include <iostream>

using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;

namespace tagtree {

const std::string MongoDBSeriesManager::DATABASE_NAME = "tagtree";
const std::string MongoDBSeriesManager::COLLECTION_NAME = "series";

MongoDBSeriesManager::MongoDBSeriesManager(bool create_instance,
                                           std::string_view uri,
                                           std::string_view database_name)
    : client{uri == "" ? mongocxx::uri() : mongocxx::uri(uri.data())},
      database_name(database_name)
{
    if (create_instance) {
        inst = std::make_unique<mongocxx::instance>();
    }
}

bool MongoDBSeriesManager::read_entry(SeriesEntry* entry)
{
    auto collection = client[database_name][COLLECTION_NAME];

    auto filter_builder = bsoncxx::builder::stream::document{};
    filter_builder << "tsid" << entry->tsid.to_string();

    bsoncxx::stdx::optional<bsoncxx::document::value> maybe_result =
        collection.find_one(filter_builder.view());

    if (!maybe_result) return false;

    auto result = maybe_result->view();

    for (auto&& attr : result) {
        auto key = attr.key().to_string();
        if (key == "_id" || key == "tsid") continue;

        entry->labels.emplace_back(key, attr.get_utf8().value.to_string());
    }

    return true;
}

void MongoDBSeriesManager::write_entry(SeriesEntry* entry)
{
    auto builder = bsoncxx::builder::stream::document{};
    auto collection = client[database_name][COLLECTION_NAME];

    auto doc_value = builder << "tsid" << entry->tsid.to_string();
    for (auto&& p : entry->labels) {
        doc_value = doc_value << p.name << p.value;
    }

    auto doc = doc_value << finalize;

    collection.insert_one(std::move(doc));
}

} // namespace tagtree
