// Encoded Operator Synthesis prototype: one generic evaluator over tagged encoded
// column trees. It accepts a query specification (JSON), decides which columns must be
// decoded, evaluates COUNT/SUM/MIN/MAX with optional dictionary-code WHERE and HAVING,
// and writes the canonical typed result. No per-query C++ adapter is generated.
//
// This is the P1 mechanism prototype: the same binary executes many queries over the
// encoded representation; value columns are decoded only when an aggregate needs them
// (count-only queries decode the group column alone).
#include "../src_patch_fusion/fullsql_reference.hpp"
#include "../src_fullsql_ig2_fast_20261029/streaming.hpp"
#include "fls/json/nlohmann/json.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

using namespace fastlanes;
using json = nlohmann::json;

enum class AggOp { SUM, COUNT, MIN, MAX };
static constexpr U CODE_BIAS = 0x80000000U;

struct AggSpec {
    AggOp op;
    U id;
    int64_t bias;
    double having_min, having_max;
    bool has_having;
};

struct Dict {
    // Zero-copy dictionary view: the file bytes stay alive in `storage` and entries point
    // into them; no per-string allocation (large dictionaries such as IG/16's 25,863
    // cities otherwise dominate setup).
    std::shared_ptr<std::string> storage;
    std::vector<std::string_view> entries;
};

static std::string read_all(const std::string& path) {
    // Bulk read: the istreambuf_iterator form costs ~1.5 ms on the 496 KB IG/16
    // dictionary, dominating its setup.
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) throw std::runtime_error("open " + path);
    const auto size = f.tellg();
    std::string data(size_t(size), '\0');
    f.seekg(0);
    f.read(data.data(), size);
    if (!f) throw std::runtime_error("read " + path);
    return data;
}

static U rd_u32(const char*& p) {
    U v;
    std::memcpy(&v, p, sizeof(U));
    p += sizeof(U);
    return v;
}

static Dict load_dict(const std::string& path, const std::string& binding) {
    auto storage = std::make_shared<std::string>(read_all(path));
    const std::string& data = *storage;
    if (data.size() < 8 + 40 || data.compare(0, 8, "BSSTR01\n") != 0)
        throw std::runtime_error("dictionary magic");
    if (data.compare(8, 40, binding) != 0) throw std::runtime_error("dictionary binding");
    const char* p = data.data() + 48;
    const char* end = data.data() + data.size();
    U k = rd_u32(p);
    Dict d;
    d.storage = storage;
    d.entries.reserve(k);
    for (U i = 0; i < k; ++i) {
        if (p + 4 > end) throw std::runtime_error("dictionary truncation");
        U n = rd_u32(p);
        if (p + n > end) throw std::runtime_error("dictionary bytes");
        d.entries.emplace_back(p, n);
        p += n;
    }
    return d;
}

static std::string json_escape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (unsigned char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default: out.push_back(char(c));
        }
    }
    return out;
}

static std::string i128_str(__int128 v) {
    if (v == 0) return "0";
    bool neg = v < 0;
    unsigned __int128 x = neg ? (unsigned __int128)(-v) : (unsigned __int128)v;
    std::string s;
    while (x) { s.push_back(char('0' + int(x % 10))); x /= 10; }
    if (neg) s.push_back('-');
    std::reverse(s.begin(), s.end());
    return s;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: generic_eval SPEC.json OUT.json\n";
        return 2;
    }
    json spec = json::parse(read_all(argv[1]));
    const std::string path_mode = spec.value("path", std::string("auto"));
    const bool force_perrow = path_mode == "perrow";
    const bool force_run = path_mode == "run";
    const std::string encoded = spec.at("encoded");
    const std::string prep = spec.at("prep");
    const std::string binding = read_all(prep + "/binding.txt").substr(0, 40);
    const std::string dict_path = prep + "/" + spec.at("dict").get<std::string>();
    const U group_id = spec.at("group_id").get<U>();
    std::vector<AggSpec> aggs;
    for (const auto& a : spec.at("aggs")) {
        std::string op = a.at("op");
        AggSpec s{AggOp::SUM, a.value("id", 0u), a.value("bias", int64_t(0)), 0, 0, false};
        if (op == "sum") s.op = AggOp::SUM;
        else if (op == "count") s.op = AggOp::COUNT;
        else if (op == "min") s.op = AggOp::MIN;
        else if (op == "max") s.op = AggOp::MAX;
        else throw std::runtime_error("unknown op " + op);
        if (a.contains("having")) {
            s.has_having = true;
            s.having_min = a["having"].value("min", -1e300);
            s.having_max = a["having"].value("max", 1e300);
        }
        aggs.push_back(s);
    }
    // WHERE: include/exclude by dictionary string (translated to codes after dict is loaded).
    std::vector<std::string> where_in, where_not_in;
    if (spec.contains("where_in")) where_in = spec["where_in"].get<std::vector<std::string>>();
    if (spec.contains("where_not_in")) where_not_in = spec["where_not_in"].get<std::vector<std::string>>();
    struct NumFilterSpec { U id; int64_t min_v; int64_t max_v; };
    std::vector<NumFilterSpec> num_filter_specs;
    if (spec.contains("numeric_filters")) {
        for (const auto& f : spec["numeric_filters"]) {
            NumFilterSpec fs;
            fs.id = f.at("id").get<U>();
            fs.min_v = f.value("min", int64_t(INT64_MIN));
            fs.max_v = f.value("max", int64_t(INT64_MAX));
            num_filter_specs.push_back(fs);
        }
    }
    struct FilterSpec { U id; std::string dict_file; std::vector<std::string> values; };
    std::vector<FilterSpec> filter_specs;
    if (spec.contains("filters")) {
        for (const auto& f : spec["filters"]) {
            FilterSpec fs;
            fs.id = f.at("id").get<U>();
            fs.dict_file = f.at("dict").get<std::string>();
            fs.values = f.at("values").get<std::vector<std::string>>();
            filter_specs.push_back(std::move(fs));
        }
    }

    auto t0 = std::chrono::steady_clock::now();
    Dict dict = load_dict(dict_path, binding);
    auto t_dict = std::chrono::steady_clock::now();
    bool parsed = false;
    StreamTable table = stream_load(encoded, binding, parsed);
    if (!parsed) throw std::runtime_error("encoded structure rejected");
    auto t_stream = std::chrono::steady_clock::now();
    const StreamColumn* gcol = nullptr;
    std::vector<const StreamColumn*> acol(aggs.size(), nullptr);
    for (auto& c : table.columns) {
        if (c.id == group_id) gcol = &c;
        for (size_t i = 0; i < aggs.size(); ++i)
            if (aggs[i].op != AggOp::COUNT && c.id == aggs[i].id) acol[i] = &c;
    }
    if (!gcol) throw std::runtime_error("group column missing");
    for (size_t i = 0; i < aggs.size(); ++i)
        if (aggs[i].op != AggOp::COUNT && !acol[i]) throw std::runtime_error("agg column missing");
    const U n = gcol->n;
    struct Filter { const StreamColumn* col; std::vector<uint8_t> allow; };
    std::vector<Filter> filters;
    for (auto& fs : filter_specs) {
        Dict fd = load_dict(prep + "/" + fs.dict_file, binding);
        Filter f;
        f.col = nullptr;
        for (auto& c : table.columns) if (c.id == fs.id) f.col = &c;
        if (!f.col) throw std::runtime_error("filter column missing");
        f.allow.assign(fd.entries.size(), 0);
        for (auto& v : fs.values) {
            auto it = std::find(fd.entries.begin(), fd.entries.end(), std::string_view(v));
            if (it != fd.entries.end()) f.allow[size_t(it - fd.entries.begin())] = 1;
        }
        filters.push_back(std::move(f));
    }
    struct NumFilter { const StreamColumn* col; int64_t min_v, max_v; U id; };
    std::vector<NumFilter> num_filters;
    for (auto& fs : num_filter_specs) {
        NumFilter f{nullptr, fs.min_v, fs.max_v, fs.id};
        for (auto& c : table.columns) if (c.id == fs.id) f.col = &c;
        if (!f.col) throw std::runtime_error("numeric filter column missing");
        if (f.col->family != 2) throw std::runtime_error("numeric filter family");
        num_filters.push_back(f);
    }
    const bool has_value_filters = !filters.empty() || !num_filters.empty();
    const bool need_values = std::any_of(aggs.begin(), aggs.end(),
                                         [](const AggSpec& a) { return a.op != AggOp::COUNT; });
    auto t1 = std::chrono::steady_clock::now();

    std::vector<int> allow(dict.entries.size(), 1);
    for (auto& s : where_not_in) {
        auto it = std::find(dict.entries.begin(), dict.entries.end(), std::string_view(s));
        if (it != dict.entries.end()) allow[size_t(it - dict.entries.begin())] = 0;
    }
    if (!where_in.empty()) {
        std::fill(allow.begin(), allow.end(), 0);
        for (auto& s : where_in) {
            auto it = std::find(dict.entries.begin(), dict.entries.end(), std::string_view(s));
            if (it != dict.entries.end()) allow[size_t(it - dict.entries.begin())] = 1;
        }
    }
    struct Group {
        std::vector<__int128> sum;
        std::vector<uint64_t> count;
        std::vector<int64_t> min, max;
        std::vector<uint8_t> has;
        Group(size_t a, size_t k) : sum(a, 0), count(a, 0), min(a, INT64_MAX), max(a, INT64_MIN), has(a, 0) {
            (void)k;
        }
    };
    const bool count_only_single = !need_values && aggs.size() == 1 && aggs[0].op == AggOp::COUNT;
    std::vector<Group> groups;
    const bool flat_count = count_only_single && filters.empty();
    if (!flat_count) groups.assign(dict.entries.size(), Group(aggs.size(), dict.entries.size()));
    std::vector<uint64_t> count0;
    if (flat_count) count0.assign(dict.entries.size(), 0);
    const bool no_filter = where_in.empty() && where_not_in.empty();
    Scratch gdec;
    std::vector<Scratch> vdec(aggs.size());
    const bool flat_single = n >= 200000 && aggs.size() == 1 && aggs[0].op != AggOp::COUNT &&
                             filters.empty() &&
                             (num_filters.empty() ||
                              (num_filters.size() == 1 && num_filters[0].id == acol[0]->id)) &&
                             !acol[0]->nullable;
    std::vector<__int128> fsum;
    std::vector<int64_t> fmin, fmax;
    std::vector<uint64_t> fcnt;
    if (flat_single) {
        fsum.assign(dict.entries.size(), 0);
        fmin.assign(dict.entries.size(), INT64_MAX);
        fmax.assign(dict.entries.size(), INT64_MIN);
        fcnt.assign(dict.entries.size(), 0);
    }
    const size_t blocks = gcol->leaves.size();
    bool sampled_order = false;
    bool auto_perrow = false;
    for (U b = 0; b < blocks; ++b) {
        const U off = b * V;
        const U take = std::min(V, n - off);
        stream_decode_block(*gcol, b, gdec);
        std::vector<Scratch> fdec(filters.size());
        for (size_t fi = 0; fi < filters.size(); ++fi)
            stream_decode_block(*filters[fi].col, b, fdec[fi]);
        std::vector<Scratch> nfdec(num_filters.size());
        for (size_t fi = 0; fi < num_filters.size(); ++fi)
            stream_decode_block(*num_filters[fi].col, b, nfdec[fi]);
        auto row_numeric_ok = [&](U i) {
            for (size_t fi = 0; fi < num_filters.size(); ++fi) {
                const int64_t v = int64_t(nfdec[fi].d[i]) - int64_t(CODE_BIAS);
                if (v < num_filters[fi].min_v || v > num_filters[fi].max_v) return false;
            }
            return true;
        };
        auto row_filters_ok = [&](U i) {
            if (!row_numeric_ok(i)) return false;
            for (size_t fi = 0; fi < filters.size(); ++fi) {
                const U code = fdec[fi].d[i] - CODE_BIAS;
                if (code >= filters[fi].allow.size() || !filters[fi].allow[code]) return false;
            }
            return true;
        };
        if (!sampled_order && n >= 200000 && !gcol->nullable && !force_perrow && !force_run) {
            // Path selection feature: average run length on the first block. A nearly
            // all-distinct block means run detection pays for itself twice; switch the
            // whole query to the per-row operator.
            U runs = 1;
            for (U i = 1; i < take; ++i) if (gdec.d[i] != gdec.d[i - 1]) ++runs;
            // Size-aware rule: run detection is cheap at 65k rows even when runs are
            // short, so small relations always keep run-splitting; near-distinct order
            // only switches to per-row at larger sizes where run detection doubles work.
            auto_perrow = (n >= 200000) && (runs > take / 4);
            sampled_order = true;
        }
        if (need_values) {
            // Predicate pushdown before payload decode: if no row in this block can
            // pass the group-domain filter, skip decoding every value column for the
            // block. On ordered/clustered filtered queries this removes most payload
            // decodes without changing semantics.
            bool any_selected = false;
            if (gcol->nullable) {
                for (U i = 0; i < take; ++i) {
                    if (!stream_valid_bit(gcol->valid, off + i)) continue;
                    const U code = gdec.d[i] - CODE_BIAS;
                    if (code < dict.entries.size() && allow[code]) { any_selected = true; break; }
                }
            } else {
                for (U i = 0; i < take; ++i) {
                    const U code = gdec.d[i] - CODE_BIAS;
                    if (code < dict.entries.size() && allow[code]) { any_selected = true; break; }
                }
            }
            if (!any_selected) continue;
            for (size_t i = 0; i < aggs.size(); ++i)
                if (acol[i]) stream_decode_block(*acol[i], b, vdec[i]);
        }
        if (flat_single && (gcol->nullable || auto_perrow || force_perrow)) {
            const U* vals = vdec[0].d.data();
            const int64_t bias = aggs[0].bias;
            const bool value_filter = !num_filters.empty();
            const int64_t flo = value_filter ? num_filters[0].min_v : INT64_MIN;
            const int64_t fhi = value_filter ? num_filters[0].max_v : INT64_MAX;
            if (gcol->nullable) {
                const U words = (take + 63) / 64;
                for (U w = 0; w < words; ++w) {
                    uint64_t bits = gcol->valid[(off / 64) + w];
                    if (w + 1 == words && (take % 64)) bits &= (1ULL << (take % 64)) - 1ULL;
                    while (bits) {
                        const U i = w * 64 + U(__builtin_ctzll(bits));
                        bits &= bits - 1;
                        const U code = gdec.d[i] - CODE_BIAS;
                        if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                        if (!allow[code]) continue;
                        const int64_t v = int64_t(vals[i]) - bias;
                        if (value_filter && (v < flo || v > fhi)) continue;
                        if (aggs[0].op == AggOp::SUM) {
                            fsum[code] += int64_t(vals[i]);
                        } else if (aggs[0].op == AggOp::MIN) {
                            if (v < fmin[code]) fmin[code] = v;
                        } else {
                            if (v > fmax[code]) fmax[code] = v;
                        }
                        ++fcnt[code];
                    }
                }
                continue;
            }
            switch (aggs[0].op) {
                case AggOp::SUM:
                    for (U i = 0; i < take; ++i) {
                        const U code = gdec.d[i] - CODE_BIAS;
                        if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                        if (!allow[code]) continue;
                        const int64_t v = int64_t(vals[i]) - bias;
                        if (value_filter && (v < flo || v > fhi)) continue;
                        fsum[code] += int64_t(vals[i]);
                        ++fcnt[code];
                    }
                    break;
                case AggOp::MIN:
                    for (U i = 0; i < take; ++i) {
                        const U code = gdec.d[i] - CODE_BIAS;
                        if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                        if (!allow[code]) continue;
                        const int64_t v = int64_t(vals[i]) - bias;
                        if (value_filter && (v < flo || v > fhi)) continue;
                        if (v < fmin[code]) fmin[code] = v;
                        ++fcnt[code];
                    }
                    break;
                default:
                    for (U i = 0; i < take; ++i) {
                        const U code = gdec.d[i] - CODE_BIAS;
                        if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                        if (!allow[code]) continue;
                        const int64_t v = int64_t(vals[i]) - bias;
                        if (value_filter && (v < flo || v > fhi)) continue;
                        if (v > fmax[code]) fmax[code] = v;
                        ++fcnt[code];
                    }
                    break;
            }
            continue;
        }
        // count-only fast path: whole runs of equal group code
        const bool count_only = !need_values;
        if (count_only) {
            if (flat_count) {
                auto* cnt = count0.data();
                if (!num_filters.empty()) {
                    const int64_t lo = num_filters[0].min_v, hi = num_filters[0].max_v;
                    const U* fv = nfdec[0].d.data();
                    if (gcol->nullable) {
                        const U words = (take + 63) / 64;
                        for (U w = 0; w < words; ++w) {
                            uint64_t bits = gcol->valid[(off / 64) + w];
                            if (w + 1 == words && (take % 64)) bits &= (1ULL << (take % 64)) - 1ULL;
                            while (bits) {
                                const U i = w * 64 + U(__builtin_ctzll(bits));
                                bits &= bits - 1;
                                const int64_t v = int64_t(fv[i]) - int64_t(CODE_BIAS);
                                if (v < lo || v > hi) continue;
                                const U code = gdec.d[i] - CODE_BIAS;
                                if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                                ++cnt[code];
                            }
                        }
                    } else {
                        for (U i = 0; i < take; ++i) {
                            const int64_t v = int64_t(fv[i]) - int64_t(CODE_BIAS);
                            if (v < lo || v > hi) continue;
                            const U code = gdec.d[i] - CODE_BIAS;
                            if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                            ++cnt[code];
                        }
                    }
                    continue;
                }
                if (gcol->nullable) {
                    // Iterate only set validity bits: for a nullable group column with a
                    // significant null fraction this skips null rows without a per-row
                    // validity branch (CMC Observed has 33% null groups).
                    const U words = (take + 63) / 64;
                    for (U w = 0; w < words; ++w) {
                        uint64_t bits = gcol->valid[(off / 64) + w];
                        if (w + 1 == words && (take % 64)) bits &= (1ULL << (take % 64)) - 1ULL;
                        while (bits) {
                            const U i = w * 64 + U(__builtin_ctzll(bits));
                            bits &= bits - 1;
                            const U code = gdec.d[i] - CODE_BIAS;
                            if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                            if (no_filter || allow[code]) ++cnt[code];
                        }
                    }
                } else {
                    for (U i = 0; i < take; ++i) {
                        const U code = gdec.d[i] - CODE_BIAS;
                        if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                        if (no_filter || allow[code]) ++cnt[code];
                    }
                }
                continue;
            }
            U i = 0;
            if (gcol->nullable) {
                for (; i < take; ++i) {
                    const U row = off + i;
                    if (!stream_valid_bit(gcol->valid, row)) continue;
                    if (!row_filters_ok(i)) continue;
                    const U code = gdec.d[i] - CODE_BIAS;
                    if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                    if (allow[code]) ++groups[code].count[0];
                }
                continue;
            }
            while (i < take) {
                const U code = gdec.d[i] - CODE_BIAS;
                if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                U j = i + 1;
                while (j < take && gdec.d[j] == gdec.d[i]) ++j;
                // Non-group filters break run amortization; force per-row when present.
                if (filters.empty() && num_filters.empty()) {
                    if (allow[code]) groups[code].count[0] += (j - i);
                } else {
                    for (U k = i; k < j; ++k) {
                        if (!row_filters_ok(k)) continue;
                        if (allow[code]) ++groups[code].count[0];
                    }
                }
                i = j;
            }
            continue;
        }
        if (!gcol->nullable && !force_perrow && filters.empty() && num_filters.empty() && (force_run || !auto_perrow)) {
            // Run-splitting operator: one group lookup per contiguous equal-code run;
            // the value aggregate still scans the run, but dictionary lookup, sentinel
            // checks and filter checks are amortized over the run.
            U i = 0;
            while (i < take) {
                const U code = gdec.d[i] - CODE_BIAS;
                if (code >= dict.entries.size()) throw std::runtime_error("group code domain");
                U j = i + 1;
                while (j < take && gdec.d[j] == gdec.d[i]) ++j;
                if (!allow[code]) { i = j; continue; }
                auto& g = groups[code];
                const U len = j - i;
                for (size_t a = 0; a < aggs.size(); ++a) {
                    if (aggs[a].op == AggOp::COUNT) { g.count[a] += len; continue; }
                    const U* vals = vdec[a].d.data();
                    const bool vnull = acol[a]->nullable != 0;
                    if (!vnull && aggs[a].op == AggOp::SUM) {
                        int64_t acc = 0;
                        for (U k = i; k < j; ++k) acc += int64_t(vals[k]);
                        g.sum[a] += acc - aggs[a].bias * int64_t(len);
                        g.count[a] += len;
                        continue;
                    }
                    if (!vnull && (aggs[a].op == AggOp::MIN || aggs[a].op == AggOp::MAX)) {
                        // Run-local extrema: one group-memory update per run instead of
                        // one compare against the group array per value.
                        int64_t best = int64_t(vals[i]) - aggs[a].bias;
                        if (aggs[a].op == AggOp::MIN) {
                            for (U k = i + 1; k < j; ++k) {
                                const int64_t v = int64_t(vals[k]) - aggs[a].bias;
                                if (v < best) best = v;
                            }
                            if (best < g.min[a]) g.min[a] = best;
                        } else {
                            for (U k = i + 1; k < j; ++k) {
                                const int64_t v = int64_t(vals[k]) - aggs[a].bias;
                                if (v > best) best = v;
                            }
                            if (best > g.max[a]) g.max[a] = best;
                        }
                        g.count[a] += len;
                        continue;
                    }
                    for (U k = i; k < j; ++k) {
                        if (vnull && !stream_valid_bit(acol[a]->valid, off + k)) continue;
                        const int64_t v = int64_t(vals[k]) - aggs[a].bias;
                        switch (aggs[a].op) {
                            case AggOp::SUM: g.sum[a] += v; g.count[a]++; break;
                            case AggOp::MIN:
                                if (v < g.min[a]) g.min[a] = v;
                                g.has[a] = 1; ++g.count[a]; break;
                            case AggOp::MAX:
                                if (v > g.max[a]) g.max[a] = v;
                                g.has[a] = 1; ++g.count[a]; break;
                            default: break;
                        }
                    }
                }
                i = j;
            }
        } else {
            for (U i = 0; i < take; ++i) {
                const U row = off + i;
                if (!stream_valid_bit(gcol->valid, row)) continue;
                const U code = gdec.d[i] - CODE_BIAS;
                if (code == dict.entries.size()) continue;
                if (code > dict.entries.size()) throw std::runtime_error("group code domain");
                if (!allow[code]) continue;
                if (!row_filters_ok(i)) continue;
                for (size_t a = 0; a < aggs.size(); ++a) {
                    auto& g = groups[code];
                    if (aggs[a].op == AggOp::COUNT) { ++g.count[a]; continue; }
                    if (acol[a]->nullable && !stream_valid_bit(acol[a]->valid, row)) continue;
                    const int64_t v = int64_t(vdec[a].d[i]) - aggs[a].bias;
                    switch (aggs[a].op) {
                        case AggOp::SUM: g.sum[a] += v; g.count[a]++; break;
                        case AggOp::MIN:
                            if (v < g.min[a]) g.min[a] = v;
                            g.has[a] = 1; ++g.count[a]; break;
                        case AggOp::MAX:
                            if (v > g.max[a]) g.max[a] = v;
                            g.has[a] = 1; ++g.count[a]; break;
                        default: break;
                    }
                }
            }
        }
    }
    if (flat_single) {
        for (U code = 0; code < dict.entries.size(); ++code) {
            if (!fcnt[code]) continue;
            auto& g = groups[code];
            g.count[0] = fcnt[code];
            if (aggs[0].op == AggOp::SUM) {
                g.sum[0] = fsum[code] - __int128(aggs[0].bias) * fcnt[code];
            } else if (aggs[0].op == AggOp::MIN) {
                g.min[0] = fmin[code];
            } else {
                g.max[0] = fmax[code];
            }
        }
    }
    auto t2 = std::chrono::steady_clock::now();

    // Serialize the selected groups in dictionary order without materializing a
    // vector of Row strings: the output path is the dominant cost for high-cardinality
    // groupings (e.g. 4,536 CMC cities), so this is deliberately one reserved buffer.
    std::vector<U> out_codes;
    out_codes.reserve(dict.entries.size());
    for (U code = 0; code < dict.entries.size(); ++code) {
        bool any = false;
        if (flat_count) {
            any = count0[code] != 0;
        } else {
            for (auto c : groups[code].count) any = any || (c != 0);
        }
        if (!any) continue;
        bool keep = true;
        for (size_t a = 0; a < aggs.size() && keep; ++a) {
            if (!aggs[a].has_having) continue;
            long double v = 0;
            if (flat_count) v = (long double)count0[code];
            else if (aggs[a].op == AggOp::COUNT) v = (long double)groups[code].count[a];
            else if (aggs[a].op == AggOp::SUM) v = (long double)(long long)groups[code].sum[a];
            else if (aggs[a].op == AggOp::MIN) v = (long double)groups[code].min[a];
            else v = (long double)groups[code].max[a];
            if (v < aggs[a].having_min || v > aggs[a].having_max) keep = false;
        }
        if (keep) out_codes.push_back(code);
    }
    std::sort(out_codes.begin(), out_codes.end(), [&](U a, U b) {
        return dict.entries[a] < dict.entries[b];
    });
    std::string out;
    out.reserve(out_codes.size() * 32 + 160);
    out += "{\"columns\":[{\"name\":\"group\",\"type\":\"VARCHAR\"}";
    for (size_t a = 0; a < aggs.size(); ++a)
        out += ",{\"name\":\"agg" + std::to_string(a) + "\",\"type\":\"HUGEINT\"}";
    out += "],\"rows\":[";
    char num[64];
    auto append_u = [&](uint64_t v) {
        auto r = std::to_chars(num, num + sizeof(num), v);
        out.append(num, size_t(r.ptr - num));
    };
    auto append_i = [&](int64_t v) {
        auto r = std::to_chars(num, num + sizeof(num), v);
        out.append(num, size_t(r.ptr - num));
    };
    for (size_t i = 0; i < out_codes.size(); ++i) {
        if (i) out += ',';
        const U code = out_codes[i];
        out += "[\"";
        out += json_escape(dict.entries[code]);
        out += '"';
        for (size_t a = 0; a < aggs.size(); ++a) {
            out += ",\"";
            switch (aggs[a].op) {
                case AggOp::COUNT: append_u(flat_count ? count0[code] : groups[code].count[a]); break;
                case AggOp::SUM: out += i128_str(groups[code].sum[a]); break;
                case AggOp::MIN: append_i(groups[code].min[a]); break;
                case AggOp::MAX: append_i(groups[code].max[a]); break;
            }
            out += '"';
        }
        out += ']';
    }
    out += "]}\n";
    int fd = ::open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("output open");
    size_t off = 0;
    while (off < out.size()) {
        ssize_t w = ::write(fd, out.data() + off, out.size() - off);
        if (w <= 0) throw std::runtime_error("output write");
        off += size_t(w);
    }
    ::close(fd);
    auto t3 = std::chrono::steady_clock::now();
    const double setup_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    const double query_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    const double out_ms = std::chrono::duration<double, std::milli>(t3 - t2).count();
    std::ofstream met(std::string(argv[2]) + ".metrics.json");
    met << "{\"codec\":\"generic_encoded\",\"rows\":" << n << ",\"groups\":" << out_codes.size()
        << ",\"setup_ms\":" << setup_ms << ",\"query_ms\":" << query_ms
        << ",\"output_ms\":" << out_ms << ",\"stored_ms\":" << (setup_ms + query_ms + out_ms)
        << ",\"dict_ms\":" << std::chrono::duration<double,std::milli>(t_dict-t0).count()
        << ",\"stream_ms\":" << std::chrono::duration<double,std::milli>(t_stream-t_dict).count()
        << ",\"auto_perrow\":" << (auto_perrow ? "true" : "false")
        << ",\"decoded_value_columns\":" << (need_values ? "true" : "false")
        << ",\"aggregates\":" << aggs.size() << "}\n";
    std::cout << "generic_encoded stored_ms=" << (setup_ms + query_ms + out_ms)
              << " rows=" << n << " groups=" << out_codes.size()
              << " value_columns=" << (need_values ? 1 : 0) << std::endl;
    return 0;
}
