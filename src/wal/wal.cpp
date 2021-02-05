#include "tagtree/wal/wal.h"

#include "CRC.h"

#include <algorithm>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <filesystem>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>

namespace tagtree {

WAL::WAL(const std::string& log_dir)
    : log_dir(log_dir), last_segment_fd(-1), page_start(0), page_end(0)
{
    checkpoint_path = log_dir + "/checkpoint.meta";

    init_log_dir();

    page = std::make_unique<uint8_t[]>(PAGE_SIZE);
    memset(page.get(), 0, PAGE_SIZE);

    size_t start, end;
    get_segment_range(start, end);

    if (end == 0) {
        /* no segments */
        create_segment(1);
        end = 1;
    }

    open_write_segment(end);
    last_segment = end;
}

void WAL::init_log_dir()
{
    struct stat sbuf;
    int ret = ::stat(log_dir.c_str(), &sbuf);

    if (ret == -1 && errno == ENOENT) {
        ret = ::mkdir(log_dir.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP |
                                           S_IXGRP | S_IROTH | S_IXOTH);

        if (ret == -1) {
            throw std::runtime_error("failed to create WAL directory " +
                                     log_dir);
        }
    }
}

void WAL::get_segment_range(size_t& start, size_t& end)
{
    std::vector<size_t> refs;

    get_segments(refs);

    if (refs.empty()) {
        start = end = 0;
        return;
    }

    start = SIZE_MAX;
    end = 0;
    for (auto&& ref : refs) {
        if (start > ref) {
            start = ref;
        }

        if (end < ref) {
            end = ref;
        }
    }
}

void WAL::get_segments(std::vector<size_t>& refs)
{

    DIR* dirp;
    struct dirent* entry;
    dirp = opendir(log_dir.c_str());

    while ((entry = readdir(dirp))) {
        if (entry->d_type == DT_REG) {
            int ref = atoi(entry->d_name);
            refs.push_back(ref);
        }
    }

    closedir(dirp);
}

std::string WAL::get_seg_filename(size_t seg)
{
    std::stringstream ss;
    ss << log_dir << '/';
    ss.width(8);
    ss.fill('0');
    ss << seg;

    return ss.str();
}

std::unique_ptr<WALReader> WAL::get_segment_reader(size_t seg)
{
    return std::make_unique<WALReader>(get_seg_filename(seg));
}

void WAL::create_segment(size_t seg)
{
    auto filename = get_seg_filename(seg);

    int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_EXCL,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    if (fd == -1) {
        if (errno == EEXIST) {
            throw std::runtime_error("segment already created");
        } else {
            throw std::runtime_error("unable to create segment");
        }
    }

    ::close(fd);
}

void WAL::open_write_segment(size_t seg)
{
    static uint8_t zero_buf[PAGE_SIZE];
    static bool first = true;

    if (first) {
        memset(zero_buf, 0, PAGE_SIZE);
        first = false;
    }

    if (last_segment_fd != -1) {
        ::close(last_segment_fd);
    }

    std::string filename = get_seg_filename(seg);

    last_segment_fd = ::open(filename.c_str(), O_WRONLY);

    if (last_segment_fd == -1) {
        throw std::runtime_error("failed to open segment");
    }

    off_t offset = lseek(last_segment_fd, 0, SEEK_END);
    if (offset == -1) {
        throw std::runtime_error("failed to seek the segment");
    }

    if (offset % PAGE_SIZE) {
        /* zero padding to page size */
        size_t bytes = PAGE_SIZE - (offset % PAGE_SIZE);
        int n = ::write(last_segment_fd, zero_buf, bytes);

        if (n != bytes) {
            throw std::runtime_error("failed to pad segment file");
        }

        offset += bytes;
    }

    segment_start = offset;
}

void WAL::log_record(uint8_t* rec, size_t length, bool flush)
{
    std::lock_guard<std::mutex> lock(mutex);

    size_t remaining = PAGE_SIZE - page_end;
    remaining += (PAGE_SIZE - RECORD_HEADER_SIZE) *
                 ((MAX_SEGMENT_SIZE - segment_start) / PAGE_SIZE - 1);

    if (remaining < length) {
        get_next_segment();
    }

    LogRecordType type = LR_NONE;

    while (length) {
        if (PAGE_SIZE - page_end <= RECORD_HEADER_SIZE) {
            flush_page(true);
        }

        size_t chunk =
            std::min(length, PAGE_SIZE - page_end - RECORD_HEADER_SIZE);

        if (type == LR_NONE) {
            if (chunk == length)
                type = LR_FULL;
            else
                type = LR_FIRST;
        } else if (type == LR_FIRST || type == LR_MIDDLE) {
            if (chunk == length)
                type = LR_LAST;
            else
                type = LR_MIDDLE;
        }

        page[page_end++] = (uint8_t)type;
        uint16_t chunk_length = (uint16_t)chunk;
        chunk_length = htobe16(chunk_length);
        memcpy(&page[page_end], &chunk_length, sizeof(uint16_t));
        page_end += sizeof(uint16_t);
        uint32_t crc = CRC::Calculate((const void*)rec, chunk, CRC::CRC_32());
        memcpy(&page[page_end], &crc, sizeof(uint32_t));
        page_end += sizeof(uint32_t);
        memcpy(&page[page_end], rec, chunk);
        page_end += chunk;

        length -= chunk;
        rec += chunk;

        if (flush || PAGE_SIZE <= page_end + RECORD_HEADER_SIZE) {
            flush_page(false);
        }
    }
}

size_t WAL::get_next_segment()
{
    if (page_end > 0) {
        flush_page(true);
    }

    last_segment++;

    create_segment(last_segment);
    open_write_segment(last_segment);

    return last_segment;
}

void WAL::flush_page(bool reset)
{
    if (PAGE_SIZE <= page_end + RECORD_HEADER_SIZE) {
        reset = true;
    }

    if (reset) {
        page_end = PAGE_SIZE;
    }

    ssize_t ret;
    size_t to_write = page_end - page_start;
    do {
        ret = ::write(last_segment_fd, &page[page_start], to_write);
    } while (ret == -1 && errno == EINTR);

    if (ret != to_write) {
        throw std::runtime_error("failed to write page");
    }

    page_start = page_end;

    if (reset) {
        memset(page.get(), 0, PAGE_SIZE);
        page_end = page_start = 0;
        segment_start += PAGE_SIZE;
    }
}

size_t WAL::close_segment()
{
    std::lock_guard<std::mutex> lock(mutex);

    return get_next_segment();
}

void WAL::write_checkpoint(TSID watermark, size_t segment)
{
    std::lock_guard<std::mutex> lock(mutex);

    std::string cp_dir_tmp = checkpoint_path + ".tmp";
    int fd = ::open(cp_dir_tmp.c_str(), O_WRONLY | O_CREAT,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    uint32_t buf[3];
    buf[0] = segment;
    buf[1] = watermark;
    buf[2] = CRC::Calculate(buf, 2 * sizeof(uint32_t), CRC::CRC_32());

    ssize_t retval;
    do {
        retval = ::write(fd, buf, sizeof(buf));
    } while (retval == -1 && errno == EINTR);

    if (retval != sizeof(buf)) {
        throw std::runtime_error("failed to write checkpoint");
    }

    ::close(fd);

    std::filesystem::rename(cp_dir_tmp, checkpoint_path);
}

void WAL::last_checkpoint(CheckpointStats& stats)
{
    ::memset(&stats, 0, sizeof(CheckpointStats));
    stats.last_segment = 1;

    struct stat sbuf;
    int ret = ::stat(checkpoint_path.c_str(), &sbuf);

    if (ret == -1 && errno == ENOENT) {
        return;
    }

    int fd = ::open(checkpoint_path.c_str(), O_RDONLY);
    uint32_t buf[3];

    ssize_t retval;
    do {
        retval = ::read(fd, buf, sizeof(buf));
    } while (retval == -1 && errno == EINTR);

    if (retval != sizeof(buf)) {
        throw std::runtime_error("failed to read last checkpoint");
    }

    stats.last_segment = buf[0];
    stats.low_watermark = buf[1];

    uint32_t crc = CRC::Calculate(buf, 2 * sizeof(uint32_t), CRC::CRC_32());
    if (crc != buf[2]) {
        throw std::runtime_error(
            "failed to read last checkpoint (checksum error)");
    }
}

} // namespace tagtree
