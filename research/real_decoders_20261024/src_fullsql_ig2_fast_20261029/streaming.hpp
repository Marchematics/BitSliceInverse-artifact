#pragma once
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
// Fused zero-copy reader for the frozen BSCOH03 encoded table format.
//
// The encoded file is read once into one buffer. The parser walks only the
// structure this adapter's frozen builder emits (CHUNKS -> PATCH -> PACK leaves
// plus a validity bitmap) and keeps pointers into that buffer: packed FFOR
// words, PATCH positions/values and validity words are never copied or turned
// into Node objects. Each physical block is then decoded straight into the
// aggregation kernel.
//
// This is a new source member, not a frozen file. Every structural deviation or
// payload misalignment is reported as "not parsed" so the caller can fall back
// to the frozen loader. Decoded values, validity semantics, PATCH overwrite
// semantics and the signed-128 accumulator are identical to the frozen decoder.

struct StreamLeaf {
    const U* packed = nullptr;
    const U* pos = nullptr;
    const U* vals = nullptr;
    U bw = 0, base = 0, npatch = 0;
};

struct StreamColumn {
    U id = 0, width = 32, nullable = 0, family = 0, n = 0;
    const uint64_t* valid = nullptr;
    std::vector<uint64_t> valid_owned;
    std::vector<StreamLeaf> leaves;
};

struct StreamMapping {
    void* address = nullptr;
    size_t bytes = 0;
    ~StreamMapping() {
        if (address) munmap(address, bytes);
    }
};

struct StreamTable {
    std::string binding;
    std::shared_ptr<StreamMapping> mapping;
    std::shared_ptr<std::string> owned;
    const char* data = nullptr;
    size_t size = 0;
    bool empty = false;
    std::vector<StreamColumn> columns;
};

struct StreamReader {
    const uint8_t* p;
    const uint8_t* end;
    bool aligned = true;
    template <class T> T get() {
        need(size_t(end - p) >= sizeof(T), "stream truncated");
        T value{};
        std::memcpy(&value, p, sizeof(T));
        p += sizeof(T);
        return value;
    }
    // Returns a pointer to `count` T values and advances past them. A payload
    // that is not naturally aligned marks the reader as not parseable; the
    // caller then falls back to the frozen loader rather than risking an
    // unaligned SIMD word load.
    template <class T> const T* span(U count, const char* what, U max = 10000000) {
        need(count <= max, "stream vector length bound");
        const size_t bytes = size_t(count) * sizeof(T);
        need(size_t(end - p) >= bytes, what);
        const T* data = reinterpret_cast<const T*>(p);
        if (reinterpret_cast<uintptr_t>(p) % alignof(T)) {
            aligned = false;
            p += bytes;
            return nullptr;
        }
        p += bytes;
        return data;
    }
};

static inline bool stream_valid_bit(const uint64_t* words, U i) {
    return (words[i >> 6] >> (i & 63)) & 1ULL;
}

// Parse CHUNKS -> PATCH -> PACK with the exact vector layout written by save3.
static bool stream_parse_column(StreamReader& r, StreamColumn& column) {
    U kind = r.get<U>();
    U n = r.get<U>();
    U bw = r.get<U>();
    U base = r.get<U>();
    U width = r.get<U>();
    (void)bw; (void)base; (void)width;
    U vals_n = r.get<U>();
    r.span<U>(vals_n, "stream vals");
    U pos_n = r.get<U>();
    r.span<U>(pos_n, "stream pos");
    U lens_n = r.get<U>();
    r.span<U>(lens_n, "stream lens");
    U p32_n = r.get<U>();
    r.span<U>(p32_n, "stream p32");
    U p16_n = r.get<U>();
    r.span<uint16_t>(p16_n, "stream p16");
    U bases_n = r.get<U>();
    r.span<uint16_t>(bases_n, "stream bases");
    U children = r.get<U>();
    if (kind != 7 /*CHUNKS*/ || n != column.n || children != (column.n + V - 1) / V) return false;
    if (vals_n || pos_n || p32_n) return false;
    column.leaves.reserve(children);
    for (U b = 0; b < children; ++b) {
        U patch_kind = r.get<U>();
        U patch_n = r.get<U>();
        U patch_bw = r.get<U>();
        U patch_base = r.get<U>();
        U patch_width = r.get<U>();
        (void)patch_bw; (void)patch_base;
        U pv_n = r.get<U>();
        const U* pv = r.span<U>(pv_n, "stream patch vals");
        U pp_n = r.get<U>();
        const U* pp = r.span<U>(pp_n, "stream patch pos");
        U pl_n = r.get<U>();
        r.span<U>(pl_n, "stream patch lens");
        U pp32_n = r.get<U>();
        r.span<U>(pp32_n, "stream patch p32");
        U pp16_n = r.get<U>();
        r.span<uint16_t>(pp16_n, "stream patch p16");
        U pb_n = r.get<U>();
        r.span<uint16_t>(pb_n, "stream patch bases");
        U patch_children = r.get<U>();
        if (patch_kind != 2 /*PATCH*/ || patch_children != 1) return false;
        if (pp32_n || patch_n != V || patch_width != column.width) return false;
        if (pv_n != pp_n) return false;
        if (!r.aligned) return false;
        for (U j = 0; j < pp_n; ++j) {
            if (j && pp[j] <= pp[j - 1]) return false;
            if (pp[j] >= patch_n) return false;
        }
        U pack_kind = r.get<U>();
        U pack_n = r.get<U>();
        U pack_bw = r.get<U>();
        U pack_base = r.get<U>();
        U pack_width = r.get<U>();
        U pkv_n = r.get<U>();
        r.span<U>(pkv_n, "stream pack vals");
        U pkp_n = r.get<U>();
        r.span<U>(pkp_n, "stream pack pos");
        U pkl_n = r.get<U>();
        r.span<U>(pkl_n, "stream pack lens");
        U words = r.get<U>();
        const U* packed = r.span<U>(words, "stream pack payload");
        U p16c = r.get<U>();
        r.span<uint16_t>(p16c, "stream pack p16");
        U basesc = r.get<U>();
        r.span<uint16_t>(basesc, "stream pack bases");
        U pack_children = r.get<U>();
        if (pack_kind != 0 /*PACK*/ || pack_children != 0) return false;
        if (pack_n != V || pack_width != column.width || column.width != 32) return false;
        if (pkv_n || pkp_n || p16c || basesc) return false;
        if (pack_bw > column.width) return false;
        const size_t expected = size_t(32) * pack_bw;
        if (words != expected && !(pack_bw == 0 && words == 0)) return false;
        if (!r.aligned) return false;
        StreamLeaf leaf;
        leaf.packed = packed;
        leaf.pos = pp;
        leaf.vals = pv;
        leaf.bw = pack_bw;
        leaf.base = pack_base;
        leaf.npatch = pp_n;
        column.leaves.push_back(leaf);
    }
    return true;
}

// Returns a parsed table when the frozen file structure and alignment are
// admitted; `parsed=false` means the caller must use the frozen loader.
static StreamTable stream_load(const std::string& path, const std::string& binding, bool& parsed) {
    StreamTable table;
    table.binding = binding;
    int fd = ::open(path.c_str(), O_RDONLY);
    need(fd >= 0, "stream encoded input");
    struct stat st{};
    need(::fstat(fd, &st) == 0 && st.st_size > 0 && st.st_size <= 1000000000, "stream encoded length");
    void* mapped = ::mmap(nullptr, size_t(st.st_size), PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    if (mapped != MAP_FAILED) {
        table.mapping = std::make_shared<StreamMapping>();
        table.mapping->address = mapped;
        table.mapping->bytes = size_t(st.st_size);
        table.data = reinterpret_cast<const char*>(mapped);
        table.size = size_t(st.st_size);
    } else {
        std::ifstream f(path, std::ios::binary);
        need(bool(f), "stream encoded fallback input");
        table.owned = std::make_shared<std::string>(size_t(st.st_size), '\0');
        f.read(table.owned->data(), std::streamsize(st.st_size));
        need(bool(f), "stream encoded fallback read");
        table.data = table.owned->data();
        table.size = table.owned->size();
    }
    parsed = false;
    const char* data = table.data;
    const size_t size = table.size;
    if (size >= 8 && std::memcmp(data, "BSQEMP1\n", 8) == 0) {
        need(size == 48 && std::memcmp(data + 8, binding.data(), 40) == 0, "stream empty binding");
        table.empty = true;
        parsed = true;
        return table;
    }
    need(size >= 8 && std::memcmp(data, "BSCOH03\n", 8) == 0, "stream magic");
    StreamReader r{reinterpret_cast<const uint8_t*>(data) + 8,
                   reinterpret_cast<const uint8_t*>(data) + size};
    need(size_t(r.end - r.p) >= 40, "stream binding");
    need(std::memcmp(r.p, binding.data(), 40) == 0, "stream source binding");
    r.p += 40;
    U columns = r.get<U>();
    need(columns > 0 && columns <= 100, "stream column count");
    table.columns.resize(columns);
    for (U j = 0; j < columns; ++j) {
        StreamColumn& c = table.columns[j];
        c.id = r.get<U>();
        c.width = r.get<U>();
        c.nullable = r.get<U>();
        c.family = r.get<U>();
        c.n = r.get<U>();
        U codec = r.get<U>();
        need((c.width == 16 || c.width == 32) && c.nullable <= 1 && c.n > 0 && c.n <= 100000000,
             "stream column metadata");
        need(codec == (c.family == 5 ? 2 : c.family), "stream column codec");
        U valid_n = r.get<U>();
        need(valid_n == (U(c.n) + 63) / 64, "stream validity length");
        const size_t valid_bytes = size_t(valid_n) * sizeof(uint64_t);
        need(size_t(r.end - r.p) >= valid_bytes, "stream validity payload");
        c.valid_owned.resize(valid_n);
        std::memcpy(c.valid_owned.data(), r.p, valid_bytes);
        r.p += valid_bytes;
        c.valid = c.valid_owned.data();
        if (c.n % 64) need(c.valid[valid_n - 1] >> (c.n % 64) == 0, "stream validity padding");
        if (!c.nullable) {
            // Word-level NOT NULL validation: one check per 64 rows instead of
            // one branch per row; padding bits are forced to 1 and every real
            // bit must be 1, exactly the original condition.
            const U words = (c.n + 63) / 64;
            for (U w = 0; w < words; ++w) {
                uint64_t bits = c.valid_owned[w];
                if (w + 1 == words && (c.n % 64)) {
                    bits |= ~((1ULL << (c.n % 64)) - 1ULL);
                }
                need(bits == ~0ULL, "stream NOT NULL validity");
            }
        }
        if (!stream_parse_column(r, c)) return table;
    }
    if (!r.aligned) return table;
    need(r.p == r.end, "stream trailing bytes");
    parsed = true;
    return table;
}

static void stream_decode_block(const StreamColumn& c, U block, Scratch& scratch) {
    const StreamLeaf& leaf = c.leaves[block];
    U base = leaf.base;
    U dummy = 0;
    const U* packed = (leaf.packed && leaf.bw) ? leaf.packed : &dummy;
    unffor::unffor(const_cast<U*>(packed), scratch.d.data(), uint8_t(leaf.bw), &base);
    for (U j = 0; j < leaf.npatch; ++j)
        scratch.d[leaf.pos[j]] = leaf.vals[j];
}
