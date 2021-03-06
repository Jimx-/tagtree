#include "tagtree/index/index_server.h"
#include "tagtree/series/series_manager.h"
#include "tagtree/wal/record_serializer.h"

#include <sstream>
#include <thread>
#include <unordered_set>

using promql::MatchOp;

namespace tagtree {

IndexServer::IndexServer(std::string_view index_dir, size_t cache_size,
                         AbstractSeriesManager* sm, bool bitmap_only)
    : index_tree(this, std::string(index_dir) + "/index.db", cache_size,
                 bitmap_only),
      wal(std::string(index_dir) + "/wal")
{
    series_manager = sm;
    id_counter.store(0);
    compacting.store(false, std::memory_order_relaxed);
    last_compaction_wm = 0;

    replay_wal();
}

std::pair<TSID, bool>
IndexServer::add_series(uint64_t t, const std::vector<promql::Label>& labels)
{
    TSID new_id;
    bool ok;

    MemPostingList tsids;
    exists(labels, tsids, true);
    assert(tsids.cardinality() <= 1);

    if (tsids.cardinality()) {
        return std::make_pair(*tsids.begin(), false);
    }

    do {
        new_id = get_tsid();
        auto inserted_id = new_id;

        ok = mem_index.add(labels, inserted_id, t);

        if (inserted_id != new_id) {
            return std::make_pair(inserted_id, false);
        }
    } while (!ok);

    series_manager->add(new_id, labels);

    return std::make_pair(new_id, true);
}

void IndexServer::exists(const std::vector<promql::Label>& labels,
                         MemPostingList& tsids, bool skip_tree)
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

    if (!tsids.isEmpty() || skip_tree) {
        return;
    }

    index_tree.resolve_label_matchers(matchers, 0, UINT64_MAX, tsids);

    if (tsids.cardinality() == 1) {
        /* if found also add it to the cache to speed up the next lookup */
        series_manager->add(*tsids.begin(), labels, false);
    }
}

void IndexServer::resolve_label_matchers(
    const std::vector<promql::LabelMatcher>& matchers, uint64_t start,
    uint64_t end, MemPostingList& tsids)
{
    std::vector<promql::Label> labels;
    MemPostingList tree_postings, mem_postings;

    bool equal_pred = true;
    for (auto&& matcher : matchers) {
        if (matcher.op != promql::MatchOp::EQL) {
            equal_pred = false;
            break;
        }

        labels.push_back({matcher.name, matcher.value});
    }

    if (equal_pred) {
        auto entry = series_manager->get_by_label_set(labels);

        if (entry) {
            tsids = MemPostingList();
            tsids.add(entry->tsid);
            entry->unlock();
            return;
        }
    }

    mem_index.resolve_label_matchers(matchers, mem_postings);
    index_tree.resolve_label_matchers(matchers, start, end, tree_postings);

    tsids = tree_postings | mem_postings;

    if (tsids.cardinality() == 1) {
        // touch the series entry to load it into cache
        auto entry = series_manager->get(*tsids.begin());
        if (entry) {
            entry->unlock();
        }
    }
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
    values.clear();

    mem_index.label_values(label_name, values);
    index_tree.label_values(label_name, values);
}

void IndexServer::commit(const std::vector<SeriesRef>& series)
{
    std::vector<uint8_t> buf;
    RecordSerializer::serialize_series(series, buf);

    wal.log_record(&buf[0], buf.size(), true);

    try_compact(false, true);
}

bool IndexServer::try_compact(bool force, bool detach)
{
    if ((compactable(id_counter.load()) || force) &&
        !compacting.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(compaction_mutex);

        TSID current_id = id_counter.load(std::memory_order_relaxed);
        if ((compactable(current_id) || force) &&
            !compacting.load(std::memory_order_relaxed)) {
            compacting.store(true, std::memory_order_relaxed);
            last_compaction_wm = current_id;

            if (detach) {
                auto t =
                    std::thread([this, current_id] { compact(current_id); });
                t.detach();
            } else {
                compact(current_id);
            }

            return true;
        }
    }

    return false;
}

void IndexServer::manual_compact() { try_compact(true, false); }

bool IndexServer::compactable(TSID current_id)
{
    return (current_id >= last_compaction_wm + 50000);
}

void IndexServer::compact(TSID current_id)
{
    MemIndexSnapshot snapshot;
    size_t last_segment;

    last_segment = wal.close_segment();

    mem_index.set_low_watermark(current_id, true);
    mem_index.snapshot(current_id, snapshot);

    index_tree.write_postings(current_id, snapshot);

    series_manager->flush();

    mem_index.gc();

    wal.write_checkpoint(current_id, last_segment);

    compacting.store(false, std::memory_order_release);
}

void IndexServer::replay_wal()
{
    CheckpointStats stats;
    wal.last_checkpoint(stats);

    size_t start, end;
    wal.get_segment_range(start, end);
    start = stats.last_segment;

    TSID high_watermark = stats.low_watermark;

    for (auto seg = start; seg <= end; seg++) {
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
                        mem_index.add(p.labels, p.tsid, p.timestamp);
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
    id_counter.store(high_watermark);
}

} // namespace tagtree
