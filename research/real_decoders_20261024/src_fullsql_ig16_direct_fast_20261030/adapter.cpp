// Exact-query bridge for the PublicBI IGlocations1 query 16:
//   SELECT trim(splitpart(City, ' ', 1), <literal trim set>) ... GROUP BY ... ORDER BY ... ASC;
// One-column projected case (City, dictionary family 5). The bridge is not a general
// SQL parser: it admits only the pinned original SQL and computes the exact distinct
// trimmed first tokens. Built on the frozen primitives and the fused stream reader.
#include "../src_patch_fusion/fullsql_reference.hpp"
#include "../src_fullsql_ig2_fast_20261029/streaming.hpp"
#include <algorithm>
#include <map>

static constexpr U CITY_ID = 3, CODE_BIAS = 0x80000000U;
static const std::string IG16_SQL =
#include "ig16_sql.inc"
    ;

static std::string sql_file(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  need(bool(f), "SQL file");
  return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

static bool sql_empty(const std::string& dir) {
  std::ifstream f(dir + "/empty.txt");
  if (!f) return false;
  std::string value{std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
  need(value == "1\n", "empty marker");
  return true;
}

static void utf8_exact(const std::string& s) {
  for (size_t i = 0; i < s.size();) {
    unsigned c = (unsigned char)s[i++];
    uint32_t cp = 0;
    unsigned k = 0;
    if (c < 0x80) continue;
    if (c >= 0xc2 && c <= 0xdf) { cp = c & 31; k = 1; }
    else if (c >= 0xe0 && c <= 0xef) { cp = c & 15; k = 2; }
    else if (c >= 0xf0 && c <= 0xf4) { cp = c & 7; k = 3; }
    else throw std::runtime_error("UTF8 leading byte");
    need(i + k <= s.size(), "UTF8 truncated");
    unsigned count = k;
    while (k--) {
      unsigned b = (unsigned char)s[i++];
      need((b & 0xc0) == 0x80, "UTF8 continuation");
      cp = (cp << 6) | (b & 63);
    }
    need((count == 1 && cp >= 0x80) || (count == 2 && cp >= 0x800) ||
             (count == 3 && cp >= 0x10000),
         "UTF8 overlong");
    need(cp <= 0x10ffff && !(cp >= 0xd800 && cp <= 0xdfff), "UTF8 scalar");
  }
}

static std::vector<std::string> sql_dictionary(const std::string& dir, const std::string& binding) {
  std::ifstream f(dir + "/city.dict", std::ios::binary);
  need(bool(f), "dictionary input");
  char magic[8], source[40];
  f.read(magic, 8);
  f.read(source, 40);
  need(bool(f) && std::string(magic, 8) == "BSSTR01\n" && std::string(source, 40) == binding,
       "dictionary source binding");
  U k = rd<U>(f);
  need(k <= 1000000, "dictionary cardinality");
  std::vector<std::string> d;
  d.reserve(k);
  uint64_t total = 0;
  for (U i = 0; i < k; ++i) {
    U n = rd<U>(f);
    need(n <= 1024 && total + n <= 64ULL * 1024 * 1024, "dictionary byte budget");
    total += n;
    std::string s(n, '\0');
    f.read(s.data(), n);
    need(bool(f), "dictionary string truncation");
    utf8_exact(s);
    d.push_back(std::move(s));
  }
  need(f.peek() == std::char_traits<char>::eof(), "dictionary suffix");
  return d;
}

static std::string sql_json(const std::string& s) {
  std::string out = "\"";
  const char* hex = "0123456789abcdef";
  for (unsigned char c : s) {
    if (c == '"' || c == '\\') { out += '\\'; out += char(c); }
    else if (c < 32) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
    else out += char(c);
  }
  return out + '"';
}

// splitpart(City,' ',1) followed by trim over the literal character set that the
// reference dialect applied for the escaped trim argument (space, backslash, t, x,
// 0, b, f, r); order and comparison stay byte-exact.
static std::string split_trim(const std::string& value) {
  size_t space = value.find(' ');
  std::string token = value.substr(0, space == std::string::npos ? value.size() : space);
  auto in_trim = [](char c) {
    return c == ' ' || c == '\\' || c == 't' || c == 'x' || c == '0' || c == 'b' ||
           c == 'f' || c == 'r';
  };
  size_t a = 0, b = token.size();
  while (a < b && in_trim(token[a])) ++a;
  while (b > a && in_trim(token[b - 1])) --b;
  return token.substr(a, b - a);
}

static void sql_schema(const Table3& t) {
  need(t.meta.size() == 1 && t.cs.size() == 1, "projected schema cardinality");
  U city = bank_column(t, CITY_ID);
  need(t.meta[city].width == 32 && t.meta[city].family == 5, "fixed projected source codec");
  need(t.cs[city].root->kind == CHUNKS, "fullsql chunk root");
}

static int sql_entry(int argc, char** argv) {
  need(argc >= 2, "fullsql mode");
  std::string mode = argv[1];
  need(mode == "build_fullsql" || mode == "verify_fullsql" || mode == "query_fullsql",
       "fullsql command");
  patch_fusion_enabled = true;
  physical_fill_mode = 3;
  need(compare_mask_available(), "SSE2 availability before kernel use");
  compare_mask_enabled = true;
  if (mode == "build_fullsql" || mode == "verify_fullsql") {
    need(argc == 4, "fullsql build/verify arity");
    bool build_mode = mode == "build_fullsql";
    std::string dir = build_mode ? argv[2] : argv[3];
    std::string path = build_mode ? argv[3] : argv[2];
    auto start = Clock::now();
    auto binding = binding3(dir + "/binding.txt");
    auto dict = sql_dictionary(dir, binding);
    if (sql_empty(dir)) {
      need(dict.empty(), "empty dictionary");
    } else {
      Table3 table = build_mode ? construct3(dir) : load3(path, binding);
      sql_schema(table);
      if (build_mode) {
        save3(path, table);
        table = load3(path, binding);
      }
      verify3(table, dir);
      U city = bank_column(table, CITY_ID);
      auto values = cache_column<U>(*table.cs[city].root, table.meta[city].n);
      for (U i = 0; i < values.size(); ++i)
        if (valid3(table.valid[city], i))
          need(values[i] >= CODE_BIAS && values[i] - CODE_BIAS < dict.size(),
               "dictionary code domain");
    }
    std::cout << "{\"fullsql_build_or_verify_ms\":" << ms(start)
              << ",\"dictionary_count\":" << dict.size()
              << ",\"empty_relation\":" << (sql_empty(dir) ? "true" : "false") << "}\n";
    return 0;
  }
  need(argc == 9, "query_fullsql encoded prepared SQL method result bitmap-or-dash metrics");
  const std::string encoded = argv[2], dir = argv[3], method = argv[5];
  need(sql_file(argv[4]) == IG16_SQL, "only exact pinned IG/16 SQL admitted");
  need(method == "stream", "fullsql method");
  auto setup = Clock::now();
  std::string binding = binding3(dir + "/binding.txt");
  auto dict = sql_dictionary(dir, binding);
  bool empty = sql_empty(dir);
  need(!empty, "this case is non-empty");
  bool parsed = true;
  StreamTable streaming = stream_load(encoded, binding, parsed);
  need(parsed, "streaming parser admitted the frozen encoded structure");
  need(!streaming.empty && streaming.columns.size() == 1, "stream projected cardinality");
  const StreamColumn* city = nullptr;
  for (auto& c : streaming.columns) {
    need(c.width == 32 && c.id == CITY_ID && c.family == 5, "stream projected id/width/family");
    city = &c;
  }
  need(city != nullptr && city->n > 0, "stream projected rows");
  U n = city->n;
  need(dict.size() > 0, "dictionary presence");
  double setup_ms = ms(setup);
  auto start = Clock::now();
  std::map<std::string, uint64_t> tokens;
  Scratch codes;
  for (U b = 0; b < city->leaves.size(); ++b) {
    const U off = b * V;
    const U take = std::min(V, n - off);
    stream_decode_block(*city, b, codes);
    for (U i = 0; i < take; ++i) {
      need(stream_valid_bit(city->valid, off + i), "NOT NULL City row");
      const U code = codes.d[i] - CODE_BIAS;
      need(code < dict.size(), "dictionary code domain");
      ++tokens[split_trim(dict[code])];
    }
  }
  std::string result = "{\"columns\":[{\"name\":\"City - Split 1\",\"type\":\"VARCHAR\"}],\"rows\":[";
  bool first = true;
  for (const auto& entry : tokens) {
    if (!first) result += ',';
    first = false;
    result += '[' + sql_json(entry.first) + ']';
  }
  result += "]}\n";
  double query_ms = ms(start);
  auto rss = resident_kib();
  auto write_start = Clock::now();
  std::ofstream output(argv[6], std::ios::binary);
  output.write(result.data(), result.size());
  output.flush();
  need(bool(output), "fullsql result write");
  double output_write_ms = ms(write_start);
  std::ofstream metrics(argv[8]);
  need(bool(metrics), "fullsql metrics");
  metrics << "{\"method\":\"" << method << "\",\"setup_ms\":" << setup_ms
          << ",\"query_ms\":" << query_ms << ",\"output_write_ms\":" << output_write_ms
          << ",\"rows\":" << tokens.size() << ",\"result_bytes\":" << result.size()
          << ",\"whole_process_rss_kib\":" << rss
          << ",\"full_result_serialization_charged\":true,\"empty_relation\":false"
          << ",\"result_type\":\"VARCHAR\"}\n";
  metrics.flush();
  need(bool(metrics), "fullsql metrics flush");
  return 0;
}

int main(int argc, char** argv) {
  try {
    return sql_entry(argc, argv);
  } catch (const std::exception& e) {
    std::cerr << "REJECT: " << e.what() << '\n';
    return 2;
  }
}
