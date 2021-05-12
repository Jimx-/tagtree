#include "tagtree/index/mem_index.h"

#include "xxhash.h"

#include <iostream>

namespace tagtree {

void MemStripe::add(const promql::Label& label, TSID tsid, uint64_t timestamp,
                    bool set_next)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    map[label.name][label.value].add(tsid, timestamp, set_next);
}

void MemStripe::touch(const promql::Label& label, uint64_t timestamp)
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    auto name_it = map.find(label.name);
    if (name_it == map.end()) return;
    auto& value_map = name_it->second;
    auto value_it = value_map.find(label.value);
    if (value_it == value_map.end()) return;

    return value_it->second.touch(timestamp);
}

void MemStripe::get_matcher_postings(const promql::LabelMatcher& matcher,
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

MemIndex::MemIndex(size_t capacity) : low_watermark(0), current_limit(NO_LIMIT)
{
    // map.reserve(capacity);
}

MemStripe& MemIndex::get_stripe(const promql::Label& label)
{
    auto& name = label.name;
    auto hash = XXH64(name.c_str(), name.length(), 0);
    return stripes[hash & STRIPE_MASK];
}

bool MemIndex::add(const std::vector<promql::Label>& labels, TSID tsid,
                   uint64_t timestamp)
{
    std::vector<promql::LabelMatcher> matchers;
    for (auto&& p : labels) {
        matchers.emplace_back(promql::MatchOp::EQL, p.name, p.value);
    }

    {
        std::shared_lock<std::shared_mutex> lock(mutex);

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

        bool set_next = current_limit != NO_LIMIT && tsid > current_limit;

        for (auto&& label : labels) {
            get_stripe(label).add(label, tsid, timestamp, set_next);
        }
    }

    return true;
}

bool MemStripe::contains(const promql::Label& label, TSID tsid)
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    auto name_it = map.find(label.name);
    if (name_it == map.end()) return false;
    auto& value_map = name_it->second;
    auto value_it = value_map.find(label.value);
    if (value_it == value_map.end()) return false;

    return value_it->second.bitmap.contains(tsid);
}

void MemIndex::touch(const std::vector<promql::Label>& labels, TSID tsid,
                     uint64_t timestamp)
{
    assert(!labels.empty());
    std::shared_lock<std::shared_mutex> lock(mutex);

    if (get_stripe(labels.front()).contains(labels.front(), tsid)) {
        for (auto&& p : labels) {
            get_stripe(p).touch(p, timestamp);
        }
        return;
    }

    for (auto&& p : labels) {
        get_stripe(p).add(p, tsid, timestamp, false);
    }
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
        promql::Label label{p.name, p.value};
        auto& stripe = get_stripe(label);

        stripe.resolve_label_matcher(
            p, tsids, positive_matchers ? &exclude : nullptr, first);

        if (tsids.isEmpty()) return;

        if (p.op != promql::MatchOp::NEQ) first = false;
    }

    if (!exclude.isEmpty()) {
        tsids = tsids - exclude;
    }
}

void MemStripe::resolve_label_matcher(const promql::LabelMatcher& matcher,
                                      MemPostingList& tsids,
                                      MemPostingList* exclude, bool first)
{
    std::shared_lock<std::shared_mutex> lock(mutex);

    if (matcher.op == promql::MatchOp::EQL) {
        auto name_it = map.find(matcher.name);
        if (name_it == map.end()) {
            tsids = MemPostingList{};
            return;
        }

        auto& value_map = name_it->second;
        auto value_it = value_map.find(matcher.value);
        if (value_it == value_map.end()) {
            tsids = MemPostingList{};
            return;
        }

        if (first) {
            tsids = value_it->second.bitmap;
        } else {
            tsids &= value_it->second.bitmap;
        }
    } else if (matcher.op == promql::MatchOp::NEQ) {
        auto name_it = map.find(matcher.name);
        if (name_it == map.end()) {
            return;
        }

        auto& value_map = name_it->second;

        if (!exclude) {
            for (auto&& val : value_map) {
                if (val.first != matcher.value) {
                    tsids |= val.second.bitmap;
                }
            }
        } else {
            auto value_it = value_map.find(matcher.value);
            if (value_it == value_map.end()) {
                return;
            }

            *exclude |= value_it->second.bitmap;
        }
    } else {
        MemPostingList postings;

        get_matcher_postings(matcher, postings);

        if (first) {
            tsids = std::move(postings);
        } else {
            tsids &= postings;
        }
    }
}

void MemIndex::label_values(const std::string& label_name,
                            std::unordered_set<std::string>& values)
{
    promql::Label label{label_name, ""};
    get_stripe(label).label_values(label_name, values);
}

void MemStripe::label_values(const std::string& label_name,
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

uint64_t MemIndex::snapshot(TSID limit, MemIndexSnapshot& snapshot)
{
    uint64_t max_time = 0;
    snapshot.clear();

    for (auto& stripe : stripes)
        max_time = std::max(max_time, stripe.snapshot(limit, snapshot));

    current_limit = NO_LIMIT;

    return max_time;
}

uint64_t MemStripe::snapshot(TSID limit, MemIndexSnapshot& snapshot)
{
    std::shared_lock<std::shared_mutex> lock(mutex);

    uint64_t max_time = 0;

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

            max_time = std::max(max_time, value.second.max_timestamp.load());
        }

        snapshot[name.first] = entries;
    }

    return max_time;
}

void MemIndex::gc()
{
    /* clear postings up to low watermark */
    std::shared_lock<std::shared_mutex> lock(mutex);

    for (auto&& stripe : stripes)
        stripe.gc(low_watermark);
}

void MemStripe::gc(TSID low_watermark)
{
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
