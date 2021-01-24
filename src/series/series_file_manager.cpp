#include "tagtree/series/series_file_manager.h"

#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>

namespace tagtree {

SeriesFileManager::SeriesFileManager(size_t cache_size,
                                     std::string_view series_dir,
                                     size_t segment_size)
    : AbstractSeriesManager(cache_size, series_dir), segment_size(segment_size)
{}

std::string SeriesFileManager::get_series_filename(size_t seg)
{
    std::stringstream ss;
    ss << series_dir << '/';
    ss.width(8);
    ss.fill('0');
    ss << seg;

    return ss.str();
}

std::pair<unsigned int, unsigned int>
SeriesFileManager::get_series_seg_index(TSID tsid)
{
    return std::make_pair(tsid / segment_size, tsid % segment_size);
}

SeriesFile* SeriesFileManager::get_series_file(size_t seg, bool create)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto it = series_files.find(seg);
    if (it != series_files.end()) {
        return it->second.get();
    }

    if (create) {
        auto sf = std::make_unique<SeriesFile>(get_series_filename(seg), true,
                                               segment_size);
        auto sfp = sf.get();
        series_files.emplace(seg, std::move(sf));

        return sfp;
    }

    return nullptr;
}

bool SeriesFileManager::read_entry(RefSeriesEntry* entry)
{
    auto seg_index = get_series_seg_index(entry->tsid);
    auto sf = get_series_file(seg_index.first);

    if (!sf) return false;

    return sf->read_entry(seg_index.second, entry);
}

void SeriesFileManager::write_entry(RefSeriesEntry* entry)
{
    auto seg_index = get_series_seg_index(entry->tsid);
    auto sf = get_series_file(seg_index.first);

    sf->write_entry(seg_index.second, entry);
}

void SeriesFileManager::flush()
{
    std::lock_guard<std::mutex> lock(mutex);

    AbstractSeriesManager::flush();

    for (auto&& p : series_files) {
        p.second->flush();
    }
}

} // namespace tagtree
