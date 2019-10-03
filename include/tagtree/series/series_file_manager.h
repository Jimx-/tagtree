#ifndef _TAGTREE_SERIES_FILE_MANAGER_H_
#define _TAGTREE_SERIES_FILE_MANAGER_H_

#include "tagtree/series/series_file.h"
#include "tagtree/series/series_manager.h"

#include <unordered_map>

namespace tagtree {

class SeriesFileManager : public AbstractSeriesManager {
public:
    SeriesFileManager(size_t cache_size, std::string_view series_dir,
                      size_t segment_size);

    virtual void flush();

private:
    std::string series_dir;
    size_t segment_size;
    std::mutex mutex;
    std::unordered_map<unsigned int, std::unique_ptr<SeriesFile>> series_files;

    void init_series_dir();
    std::string get_series_filename(size_t seg);
    std::pair<unsigned int, unsigned int> get_series_seg_index(TSID tsid);

    SeriesFile* get_series_file(size_t seg, bool create = true);
    virtual bool read_entry(RefSeriesEntry* entry);
    virtual void write_entry(RefSeriesEntry* entry);
};

} // namespace tagtree

#endif
