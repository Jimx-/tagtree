#include "tagtree/index/index_server.h"
#include "tagtree/series/series_manager.h"
#include "tagtree/wal/record_serializer.h"

#include <sstream>
#include <thread>
#include <unordered_set>

using promql::MatchOp;

namespace tagtree {

IndexServer::IndexServer(std::string_view index_dir, size_t cache_size,
                         AbstractSeriesManager* sm)
    : index_tree(this, std::string(index_dir) + "index.db", cache_size),
      wal(std::string(index_dir) + "/wal")
{
    series_manager = sm;
    id_counter.store(sm->get_size() + 1);
    compacting.store(false, std::memory_order_relaxed);
    last_compaction_wm = 0;

    replay_wal();
}

IndexServer::~IndexServer() {}

TSID IndexServer::add_series(const std::vector<promql::Label>& labels)
{
    TSID new_id;

    do {
        new_id = get_tsid();
    } while (!mem_index.add(labels, new_id));

    series_manager->add(new_id, labels);

    return new_id;
}

void IndexServer::exists(const std::vector<promql::Label>& labels,
                         MemPostingList& tsids)
{
    /* check if series matching given matchers already exists */
    auto entry = series_manager->get_by_label_set(labels);

    if (entry) {
        tsids.add(entry->tsid);
        entry->unlock();
        return;
    }

    std::vector<promql::LabelMatcher> matchers;
    for (auto&& p : labels) {
        matchers.emplace_back(MatchOp::EQL, p.name, p.value);
    }

    mem_index.resolve_label_matchers(matchers, tsids);

    if (!tsids.isEmpty()) {
        return;
    }

    index_tree.resolve_label_matchers(matchers, tsids);
}

bool IndexServer::get_labels(TSID tsid, std::vector<promql::Label>& labels)
{
    auto* entry = series_manager->get(tsid);
    if (!entry) return false;
    labels.clear();
    std::copy(entry->labels.begin(), entry->labels.end(),
              std::back_inserter(labels));
    entry->unlock();
    return true;
}

void IndexServer::label_values(const std::string& label_name,
                               std::unordered_set<std::string>& values)
{
    // std::vector<TSID> tsids;
    // std::vector<promql::Label> labels;
    // index_tree.resolve_label_matchers({{MatchOp::NEQ, label_name, ""}},
    // tsids);

    // for (auto&& tsid : tsids) {
    //     labels.clear();
    //     auto* entry = series_manager->get(tsid);

    //     for (auto&& p : entry->labels) {
    //         if (p.name == label_name) {
    //             values.insert(p.value);
    //             break;
    //         }
    //     }

    //     entry->unlock();
    // }
}

void IndexServer::commit(const std::vector<SeriesRef>& series)
{
    std::vector<uint8_t> buf;
    RecordSerializer::serialize_series(series, buf);

    wal.log_record(&buf[0], buf.size(), true);

    if (compactable(id_counter.load()) &&
        !compacting.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(compaction_mutex);

        TSID current_id = id_counter.load(std::memory_order_relaxed);
        if (compactable(current_id) &&
            !compacting.load(std::memory_order_relaxed)) {
            compacting.store(true, std::memory_order_relaxed);
            last_compaction_wm = current_id;

            auto t = std::thread([this, current_id] { compact(current_id); });
            t.detach();
        }
    }
}

bool IndexServer::compactable(TSID current_id)
{
    return (current_id >= last_compaction_wm + 30000);
}

void IndexServer::compact(TSID current_id)
{
    std::vector<LabeledPostings> snapshot;

    wal.close_segment();

    mem_index.set_low_watermark(current_id);
    mem_index.snapshot(current_id, snapshot);

    index_tree.write_postings(snapshot);

    mem_index.gc();

    wal.write_checkpoint(current_id);

    compacting.store(false, std::memory_order_release);
}

void IndexServer::replay_wal()
{
    CheckpointStats stats;
    wal.last_checkpoint(stats);

    size_t start, end;
    wal.get_segment_range(start, end);

    TSID high_watermark = stats.low_watermark;

    for (auto seg = stats.last_segment; seg <= end; seg++) {
        auto reader = wal.get_segment_reader(seg);
        std::vector<uint8_t> recbuf;

        while (reader->get_next(recbuf)) {
            switch (RecordSerializer::get_record_type(recbuf)) {
            case LRT_SERIES: {
                std::vector<SeriesRef> series;
                RecordSerializer::deserialize_series(recbuf, series);

                for (auto&& p : series) {
                    if (p.tsid <= stats.low_watermark) {
                        continue;
                    }

                    if (high_watermark < p.tsid) {
                        high_watermark = p.tsid;
                    }

                    MemPostingList postings;
                    exists(p.labels, postings);

                    if (postings.isEmpty()) {
                        mem_index.add(p.labels, p.tsid);
                        series_manager->add(p.tsid, p.labels);
                    }
                }
                break;
            }
            default:
                continue;
            }
        }
    }

    last_compaction_wm = high_watermark;
    mem_index.set_low_watermark(high_watermark);
}

} // namespace tagtree
