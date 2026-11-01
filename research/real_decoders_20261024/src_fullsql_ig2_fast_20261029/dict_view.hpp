#pragma once
// Zero-copy reader for the frozen BSSTR01 dictionary format (new source member).
//
// The dictionary file is mmap'd once; every entry is a view into that mapping and
// keeps no per-entry allocation or copy. Binding, magic, bounds, UTF-8 validity
// and (optionally) uniqueness are still checked exactly, so callers get the same
// admission guarantees as the frozen per-entry loader with one allocation instead
// of one-per-entry plus a std::set.
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string_view>
#include <unordered_set>

struct DictRegion {
    void* address = nullptr;
    size_t bytes = 0;
    ~DictRegion() {
        if (address) munmap(address, bytes);
    }
};

struct DictEntry {
    const char* data = nullptr;
    U bytes = 0;
};

struct DictView {
    std::string binding;
    std::shared_ptr<DictRegion> region;
    std::vector<DictEntry> entries;

    U size() const { return U(entries.size()); }
    bool empty() const { return entries.empty(); }
    std::string_view at(U i) const {
        need(i < entries.size(), "dictionary index");
        return std::string_view(entries[i].data, entries[i].bytes);
    }
};

static inline void dict_view_utf8(std::string_view s) {
    for (size_t i = 0; i < s.size();) {
        // Fast path: skip eight bytes at once while they are all ASCII.
        while (i + 8 <= s.size()) {
            uint64_t word = 0;
            std::memcpy(&word, s.data() + i, 8);
            if (word & 0x8080808080808080ULL) break;
            i += 8;
        }
        if (i >= s.size()) break;
        unsigned c = (unsigned char)s[i++];
        if (c < 0x80) continue;
        unsigned need_bytes = 0;
        uint32_t cp = 0;
        if (c >= 0xc2 && c <= 0xdf) { cp = c & 31; need_bytes = 1; }
        else if (c >= 0xe0 && c <= 0xef) { cp = c & 15; need_bytes = 2; }
        else if (c >= 0xf0 && c <= 0xf4) { cp = c & 7; need_bytes = 3; }
        else throw std::runtime_error("UTF8 leading byte");
        need(i + need_bytes <= s.size(), "UTF8 truncated");
        for (unsigned k = 0; k < need_bytes; ++k) {
            unsigned b = (unsigned char)s[i++];
            need((b & 0xc0) == 0x80, "UTF8 continuation");
            cp = (cp << 6) | (b & 63);
        }
        need(cp <= 0x10ffff && !(cp >= 0xd800 && cp <= 0xdfff), "UTF8 scalar");
    }
}

// Loads and admits one frozen dictionary. If `require_unique` is false only
// structural admission is done (the frozen readers always require uniqueness, so
// production call sites keep it true).
static DictView dict_view_load(const std::string& path, const std::string& expected_binding,
                               bool require_unique = true) {
    int fd = ::open(path.c_str(), O_RDONLY);
    need(fd >= 0, "dictionary input");
    struct stat st{};
    need(::fstat(fd, &st) == 0 && st.st_size >= 52 && st.st_size <= 100000000, "dictionary length");
    void* mapped = ::mmap(nullptr, size_t(st.st_size), PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    need(mapped != MAP_FAILED, "dictionary mmap");
    DictView view;
    view.binding = expected_binding;
    view.region = std::make_shared<DictRegion>();
    view.region->address = mapped;
    view.region->bytes = size_t(st.st_size);
    const char* data = reinterpret_cast<const char*>(mapped);
    const size_t size = size_t(st.st_size);
    need(size >= 8 && std::memcmp(data, "BSSTR01\n", 8) == 0, "dictionary magic");
    need(size >= 48 && std::memcmp(data + 8, expected_binding.data(), 40) == 0, "dictionary binding");
    U count = 0;
    std::memcpy(&count, data + 48, sizeof(U));
    need(count <= 1000000, "dictionary cardinality");
    size_t off = 52;
    uint64_t total = 0;
    view.entries.reserve(count);
    for (U i = 0; i < count; ++i) {
        need(off + 4 <= size, "dictionary entry length");
        U bytes = 0;
        std::memcpy(&bytes, data + off, sizeof(U));
        off += 4;
        need(bytes <= 1024 && total + bytes <= 64ULL * 1024 * 1024, "dictionary byte budget");
        need(off + bytes <= size, "dictionary entry payload");
        total += bytes;
        view.entries.push_back({data + off, bytes});
        off += bytes;
    }
    need(off == size, "dictionary suffix");
    for (const auto& entry : view.entries) dict_view_utf8(std::string_view(entry.data, entry.bytes));
    if (require_unique) {
        // Flat open-addressing uniqueness check on a 64-bit FNV-1a hash: one
        // allocation for the slot table, no per-entry nodes, and byte equality is
        // verified only for colliding hashes.
        size_t capacity = 16;
        while (capacity < view.entries.size() * 2 + 1) capacity <<= 1;
        std::vector<uint32_t> table(capacity, 0);
        const size_t mask = capacity - 1;
        for (size_t index = 0; index < view.entries.size(); ++index) {
            const std::string_view value(view.entries[index].data, view.entries[index].bytes);
            uint64_t hash = 1469598103934665603ULL;
            for (unsigned char c : value) { hash ^= c; hash *= 1099511628211ULL; }
            size_t slot = size_t(hash) & mask;
            while (table[slot] != 0) {
                const size_t other = table[slot] - 1;
                need(std::string_view(view.entries[other].data, view.entries[other].bytes) != value,
                     "dictionary unique");
                slot = (slot + 1) & mask;
            }
            table[slot] = uint32_t(index + 1);
        }
    }
    return view;
}
