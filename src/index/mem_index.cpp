#include "tagtree/index/mem_index.h"

#include <iostream>

namespace tagtree {

MemIndex::MemIndex(size_t capacity) : low_watermark(0), current_limit(NO_LIMIT)
{
    map.reserve(capacity);
}

bool MemIndex::add(const std::vector<promql::Label>& labels, TSID tsid,
                   uint64_t timestamp)
{
    std::vector<promql::LabelMatcher> matchers;
    for (auto&& p : labels) {
        matchers.emplace_back(promql::MatchOp::EQL, p.name, p.value);
    }

    {
        std::unique_lock<std::shared_mutex> lock(mutex);

        if (tsid <= low_watermark) {
            return false;
        }

        /* double-checked locking */
        MemPostingList tsids;
        resolve_label_matchers_unsafe(matchers, tsids);

        if (!tsids.isEmpty()) {
            tsid = *tsids.begin();
            return true;
        }

        for (auto&& p : labels) {
            add_label(p, tsid, timestamp);
        }
    }

    return true;
}

void MemIndex::set_low_watermark(TSID wm, bool force)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    low_watermark = wm;
    if (force) current_limit = wm;
}

void MemIndex::resolve_label_matchers(
    const std::vector<promql::LabelMatcher>& matchers, MemPostingList& tsids)
{
    std::shared_lock<std::shared_mutex> lock(mutex);

    resolve_label_matchers_unsafe(matchers, tsids);
}

void MemIndex::resolve_label_matchers_unsafe(
    const std::vector<promql::LabelMatcher>& matchers, MemPostingList& tsids)
{
    bool first = true;
    MemPostingList exclude;
    int positive_matchers = 0;

    tsids = MemPostingList{};

    for (auto&& p : matchers) {
        if (p.op != promql::MatchOp::NEQ) positive_matchers++;
    }

    for (auto&& p : matchers) {
        if (p.op == promql::MatchOp::EQL) {
            auto name_it = map.find(p.name);
            if (name_it == map.end()) {
                tsids = MemPostingList{};
                return;
            }

            auto& value_map = name_it->second;
            auto value_it = value_map.find(p.value);
            if (value_it == value_map.end()) {
                tsids = MemPostingList{};
                return;
            }

            if (first) {
                tsids = value_it->second.bitmap;
            } else {
                tsids &= value_it->second.bitmap;
            }

            if (tsids.isEmpty()) return;

            first = false;
        } else if (p.op == promql::MatchOp::NEQ) {
            auto name_it = map.find(p.name);
            if (name_it == map.end()) {
                continue;
            }

            auto& value_map = name_it->second;

            if (!positive_matchers) {
                for (auto&& val : value_map) {
                    if (val.first != p.value) {
                        tsids |= val.second.bitmap;
                    }
                }
            } else {
                auto value_it = value_map.find(p.value);
                if (value_it == value_map.end()) {
                    continue;
                }

                exclude |= value_it->second.bitmap;
            }
        } else {
            MemPostingList postings;

            get_matcher_postings(p, postings);

            if (first) {
                tsids = std::move(postings);
            } else {
                tsids &= postings;
            }

            if (tsids.isEmpty()) return;

            first = false;
        }
    }

    if (!exclude.isEmpty()) {
        tsids = tsids - exclude;
    }
}

void MemIndex::get_matcher_postings(const promql::LabelMatcher& matcher,
                                    MemPostingList& tsids)
{
    tsids = {};

    auto name_it = map.find(matcher.name);
    if (name_it == map.end()) {
        return;
    }

    auto& value_map = name_it->second;
    for (auto&& p : value_map) {
        if (!matcher.match_value(p.first)) continue;

        tsids |= p.second.bitmap;
    }
}

void MemIndex::label_values(const std::string& label_name,
                            std::unordered_set<std::string>& values)
{
    std::shared_lock<std::shared_mutex> lock(mutex);

    auto it = map.find(label_name);
    if (it != map.end()) {
        for (auto&& value : it->second) {
            values.insert(value.first);
        }
    }
}

void MemIndex::snapshot(TSID limit, MemIndexSnapshot& snapshot)
{
    std::shared_lock<std::shared_mutex> lock(mutex);

    snapshot.clear();

    for (auto&& name : map) {
        std::vector<LabeledPostings> entries;

        for (auto&& value : name.second) {
            auto& bitmap = value.second.bitmap;

            if (bitmap.isEmpty()) continue;

            if (bitmap.minimum() > limit) continue;

            entries.emplace_back(value.first, value.second.min_timestamp,
                                 value.second.max_timestamp);
            auto& new_bitmap = entries.back().postings;
            new_bitmap = bitmap;
            new_bitmap.runOptimize();
        }

        snapshot[name.first] = entries;
    }

    current_limit = NO_LIMIT;
}

void MemIndex::add_label(const promql::Label& label, TSID tsid,
                         uint64_t timestamp)
{
    bool set_next = current_limit != NO_LIMIT && tsid > current_limit;
    map[label.name][label.value].add(tsid, timestamp, set_next);
}

void MemIndex::gc()
{
    /* clear postings up to low watermark */
    std::unique_lock<std::shared_mutex> lock(mutex);

    for (auto name_it = map.begin(); name_it != map.end();) {
        auto& name_map = name_it->second;

        for (auto value_it = name_map.begin(); value_it != name_map.end();) {
            auto& bitmap = value_it->second.bitmap;

            auto last_it = bitmap.begin();
            last_it.equalorlarger(low_watermark);

            if (last_it == bitmap.end()) {
                value_it = name_map.erase(value_it);
                continue;
            }

            MemPostingList new_bitmap;
            for (; last_it != bitmap.end(); last_it++) {
                new_bitmap.add(*last_it);
            }

            value_it->second.bitmap = std::move(new_bitmap);

            value_it->second.min_timestamp = value_it->second.next_timestamp;
            value_it->second.next_timestamp = UINT64_MAX;

            value_it++;
        }

        if (name_map.empty()) {
            name_it = map.erase(name_it);
        } else {
            name_it++;
        }
    }
}

} // namespace tagtree
