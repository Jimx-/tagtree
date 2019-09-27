#ifndef _TAGTREE_WAL_H_
#define _TAGTREE_WAL_H_

#include "tagtree/tsid.h"
#include "tagtree/wal/reader.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace tagtree {

struct CheckpointStats {
    unsigned int last_segment;
    TSID low_watermark;
};

/* write-ahead logger */
class WAL {
    friend class WALReader;

public:
    explicit WAL(const std::string& log_dir);

    void log_record(uint8_t* rec, size_t length, bool flush);
    void close_segment();

    void get_segment_range(size_t& start, size_t& end);

    void write_checkpoint(TSID watermark);
    void last_checkpoint(CheckpointStats& stats);

    std::unique_ptr<WALReader> get_segment_reader(size_t seg);

private:
    enum LogRecordType {
        LR_NONE = 0,
        LR_FULL,
        LR_FIRST,
        LR_MIDDLE,
        LR_LAST,
    };

    static const size_t MAX_SEGMENT_SIZE = 128 * 1024 * 1024;
    static const size_t PAGE_SIZE = 0x1000;
    static const size_t RECORD_HEADER_SIZE = 7;

    std::string log_dir;
    std::string checkpoint_path;
    std::unique_ptr<uint8_t[]> page;
    std::mutex mutex;
    size_t page_start, page_end;
    size_t segment_start;

    size_t last_segment;
    int last_segment_fd;

    void init_log_dir();

    void get_segments(std::vector<size_t>& refs);

    std::string get_seg_filename(size_t seg);
    void create_segment(size_t seg);
    void open_write_segment(size_t seg);

    void get_next_segment();
    void flush_page(bool reset);
};

} // namespace tagtree

#endif
