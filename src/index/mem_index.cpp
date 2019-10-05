#include "tagtree/index/mem_index.h"

#include <iostream>

namespace tagtree {

MemIndex::MemIndex(size_t capacity) : low_watermark(0)
{
    map.reserve(capacity);
}

bool MemIndex::add(const std::vector<promql::Label>& labels, TSID tsid)
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

        if (!tsids.isEmpty()) return true;

        for (auto&& p : labels) {
            add_label(p, tsid);
        }
    }

    return true;
}

void MemIndex::set_low_watermark(TSID wm)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    low_watermark = wm;
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
    tsids = MemPostingList{};

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
                tsids = value_it->second;
            } else {
                tsids &= value_it->second;
            }

            if (tsids.isEmpty()) return;

            first = false;
        } else if (p.op == promql::MatchOp::NEQ) {
            auto name_it = map.find(p.name);
            if (name_it == map.end()) {
                continue;
            }

            auto& value_map = name_it->second;
            auto value_it = value_map.find(p.value);
            if (value_it == value_map.end()) {
                continue;
            }

            exclude |= value_it->second;
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

        tsids |= p.second;
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

void MemIndex::snapshot(TSID limit,
                        std::vector<LabeledPostings>& labeled_postings)
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    for (auto&& name : map) {
        for (auto&& value : name.second) {
            auto& bitmap = value.second;

            if (bitmap.isEmpty()) continue;

            if (bitmap.minimum() > limit) continue;

            labeled_postings.emplace_back(name.first, value.first);
            auto& new_bitmap = labeled_postings.back().postings;
            new_bitmap = bitmap;
            new_bitmap.runOptimize();
        }
    }
}

void MemIndex::add_label(const promql::Label& label, TSID tsid)
{
    map[label.name][label.value].add(tsid);
}

void MemIndex::gc()
{
    /* clear postings up to low watermark */
    std::unique_lock<std::shared_mutex> lock(mutex);

    for (auto name_it = map.begin(); name_it != map.end();) {
        auto& name_map = name_it->second;

        for (auto value_it = name_map.begin(); value_it != name_map.end();) {
            auto& posting = value_it->second;

            auto last_it = posting.begin();
            last_it.equalorlarger(low_watermark);

            if (last_it == posting.end()) {
                value_it = name_map.erase(value_it);
                continue;
            }

            MemPostingList new_posting;
            for (; last_it != posting.end(); last_it++) {
                new_posting.add(*last_it);
            }

            posting = std::move(new_posting);

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
