// Exact block summaries for encoded columns: per-block, per-group COUNT/SUM/MIN/MAX.
// Build once over the encoded tree; query reduces summaries instead of decoding payloads.
#include "../src_patch_fusion/fullsql_reference.hpp"
#include "../src_fullsql_ig2_fast_20261029/streaming.hpp"
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

static constexpr U CODE_BIAS = 0x80000000U;
struct Header {
    char magic[8];
    uint64_t nblocks, ngroups, rows;
    uint32_t group_id, value_id;
    char binding[40];
};
struct Entry { int64_t sum; int64_t count; int32_t min, max; uint8_t has; };
struct Summary { Header h; int fd; size_t bytes; Entry* e; };

static std::string read40(const std::string& p) {
    std::ifstream f(p, std::ios::binary); std::string s(41, '\0'); f.read(s.data(), 40);
    s.resize(40); return s;
}
static U dict_count(const std::string& p) {
    std::ifstream f(p, std::ios::binary); std::string d((std::istreambuf_iterator<char>(f)), {});
    if (d.size() < 52 || d.compare(0,8,"BSSTR01\n")) throw std::runtime_error("dict");
    U k; std::memcpy(&k, d.data() + 48, 4); return k;
}
static Summary open_summary(const char* path) {
    int fd = open(path, O_RDONLY); if (fd < 0) throw std::runtime_error("open summary");
    struct stat st{}; if (fstat(fd, &st)) throw std::runtime_error("stat");
    void* m = mmap(nullptr, size_t(st.st_size), PROT_READ, MAP_PRIVATE, fd, 0);
    if (m == MAP_FAILED) throw std::runtime_error("mmap");
    Summary s; s.fd = fd; s.bytes = size_t(st.st_size);
    std::memcpy(&s.h, m, sizeof(Header));
    s.e = reinterpret_cast<Entry*>(reinterpret_cast<char*>(m) + sizeof(Header));
    return s;
}
static void build(const std::string& encoded, const std::string& prep, U gid, U vid, const std::string& out) {
    auto t0 = std::chrono::steady_clock::now();
    std::string binding = read40(prep + "/binding.txt");
    bool parsed = false; StreamTable t = stream_load(encoded, binding, parsed);
    if (!parsed) throw std::runtime_error("stream admission");
    const StreamColumn* g = nullptr; const StreamColumn* v = nullptr;
    for (auto& c : t.columns) { if (c.id == gid) g = &c; if (c.id == vid) v = &c; }
    if (!g || !v) throw std::runtime_error("columns");
    const U k = dict_count(prep + "/state.dict");
    const U n = g->n; const U blocks = g->leaves.size();
    Header h{}; std::memcpy(h.magic, "BSSUM01\n", 8); h.nblocks = blocks; h.ngroups = k;
    h.rows = n; h.group_id = gid; h.value_id = vid; std::memcpy(h.binding, binding.data(), 40);
    std::ofstream o(out, std::ios::binary); o.write(reinterpret_cast<const char*>(&h), sizeof(h));
    std::vector<Entry> ent(k);
    Scratch gd, vd;
    for (U b = 0; b < blocks; ++b) {
        for (auto& e : ent) { e = {0,0,INT32_MAX,INT32_MIN,0}; }
        const U off = b * V; const U take = std::min(V, n - off);
        stream_decode_block(*g, b, gd); stream_decode_block(*v, b, vd);
        for (U i = 0; i < take; ++i) {
            if (!stream_valid_bit(g->valid, off + i)) continue;
            U code = gd.d[i] - CODE_BIAS; if (code >= k) throw std::runtime_error("group domain");
            int64_t val = int64_t(vd.d[i]) - int64_t(CODE_BIAS);
            Entry& e = ent[code]; e.sum += val; e.count++;
            if (val < e.min) e.min = int32_t(val);
            if (val > e.max) e.max = int32_t(val);
            e.has = 1;
        }
        o.write(reinterpret_cast<const char*>(ent.data()), ent.size() * sizeof(Entry));
    }
    o.close();
    auto t1 = std::chrono::steady_clock::now();
    std::cerr << "build_ms=" << std::chrono::duration<double,std::milli>(t1-t0).count()
              << " bytes=" << (sizeof(Header) + uint64_t(blocks)*k*sizeof(Entry)) << "\n";
}
static bool parse_having(const std::string& c, const std::vector<__int128>& sum,
                          const std::vector<uint64_t>& cnt, const std::vector<int32_t>& mn,
                          const std::vector<int32_t>& mx, U g) {
    auto pos = c.find_first_of("<>=");
    if (pos == std::string::npos) return true;
    const std::string lhs = c.substr(0, pos);
    std::string op = c.substr(pos, (c.compare(pos,2,">=")==0 || c.compare(pos,2,"<=")==0) ? 2 : 1);
    const std::string rhs = c.substr(pos + op.size());
    long double v = std::strtold(rhs.c_str(), nullptr);
    long double x;
    if (lhs == "sum") x = (long double)sum[g];
    else if (lhs == "count") x = (long double)cnt[g];
    else if (lhs == "min") x = (long double)mn[g];
    else if (lhs == "max") x = (long double)mx[g];
    else return true;
    if (op == ">=") return x >= v;
    if (op == "<=") return x <= v;
    if (op == ">") return x > v;
    if (op == "<") return x < v;
    if (op == "=") return x == v;
    return true;
}
static void query(const char* summary_path, const std::string& dict_path, const char* out_path,
                  int argc, char** argv) {
    std::vector<std::string> in_names, not_names, having;
    for (int i = 4; i < argc; ++i) {
        std::string a = argv[i];
        auto split = [](const std::string& s) {
            std::vector<std::string> v; size_t p = 0;
            while (p <= s.size()) { size_t q = s.find(',', p); if (q == std::string::npos) q = s.size(); v.push_back(s.substr(p, q-p)); p = q+1; }
            return v;
        };
        if (a.rfind("where_in=", 0) == 0) in_names = split(a.substr(9));
        else if (a.rfind("where_not_in=", 0) == 0) not_names = split(a.substr(13));
        else if (a.rfind("having=", 0) == 0) having = split(a.substr(7));
    }
    auto t0 = std::chrono::steady_clock::now();
    Summary s = open_summary(summary_path);
    std::ifstream f(dict_path, std::ios::binary); std::string data((std::istreambuf_iterator<char>(f)), {});
    if (data.size() < 52 || data.compare(0,8,"BSSTR01\n")) throw std::runtime_error("dict");
    const char* p = data.data() + 48; U k; std::memcpy(&k,p,4); p+=4;
    std::vector<std::string> names; names.reserve(k);
    for (U i=0;i<k;++i){ U len; std::memcpy(&len,p,4); p+=4; names.emplace_back(p,len); p+=len; }
    std::vector<char> allowed(k, 1);
    auto in_list = [&](const std::vector<std::string>& v, const std::string& name) {
        for (auto& x : v) if (x == name) return true; return false;
    };
    for (U g = 0; g < k; ++g) {
        if (!in_names.empty() && !in_list(in_names, names[g])) allowed[g] = 0;
        if (!not_names.empty() && in_list(not_names, names[g])) allowed[g] = 0;
    }
    std::vector<__int128> sum(k,0); std::vector<uint64_t> cnt(k,0); std::vector<int32_t> mn(k,INT32_MAX), mx(k,INT32_MIN);
    const uint64_t nb = s.h.nblocks, ng = s.h.ngroups;
    for (uint64_t b=0;b<nb;++b) for (uint64_t g=0;g<ng&&g<k;++g) {
        if (!allowed[g]) continue; const Entry& e = s.e[b*ng+g]; if (!e.has) continue;
        sum[g] += e.sum; cnt[g] += e.count;
        if (e.min < mn[g]) mn[g]=e.min;
        if (e.max > mx[g]) mx[g]=e.max;
    }
    auto t1 = std::chrono::steady_clock::now();
    std::ofstream o(out_path); o << "{\"rows\":[";
    bool first=true;
    for (U g=0;g<k && g<names.size();++g) {
        if (!allowed[g] || !cnt[g]) continue;
        bool ok = true; for (auto& h : having) if (!parse_having(h, sum, cnt, mn, mx, g)) { ok = false; break; }
        if (!ok) continue;
        if(!first) o<<","; first=false;
        o << "[\"" << names[g] << "\",\"" << (long long)sum[g] << "\",\"" << cnt[g]
          << "\",\"" << mn[g] << "\",\"" << mx[g] << "\"]";
    }
    o << "]}\n"; o.close();
    auto t2 = std::chrono::steady_clock::now();
    std::cerr << "setup_ms=" << std::chrono::duration<double,std::milli>(t1-t0).count()
              << " query_ms=" << std::chrono::duration<double,std::milli>(t2-t1).count()
              << " total_ms=" << std::chrono::duration<double,std::milli>(t2-t0).count() << "\n";
}
int main(int argc, char** argv) {
    try {
        if (argc >= 7 && std::string(argv[1]) == "build") build(argv[2], argv[3], U(std::stoul(argv[4])), U(std::stoul(argv[5])), argv[6]);
        else if (argc >= 5 && std::string(argv[1]) == "query") query(argv[2], argv[3], argv[4], argc, argv);
        else { std::cerr << "usage: build ENC PREP GID VID OUT | query SUMMARY DICT OUT\n"; return 2; }
    } catch (std::exception& e) { std::cerr << "ERR " << e.what() << "\n"; return 1; }
    return 0;
}
