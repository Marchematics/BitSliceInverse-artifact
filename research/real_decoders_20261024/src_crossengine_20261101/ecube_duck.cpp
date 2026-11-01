// ECUBE02/ECUBE03 as a DuckDB 2.0.0 SQL table function, plus the engine-resident
// measurement harness.
//
// The cube index is exposed as a table function
//     ecube(cube_path VARCHAR, dict_path VARCHAR, spec VARCHAR)
// that emits exactly the rows of the pinned query
//     SELECT g, SUM(v) s, COUNT(*) c, MIN(v) mn, MAX(v) mx
//     FROM C [WHERE v BETWEEN lo AND hi] [AND g IN (...)] [GROUP BY g] [HAVING ...]
// The predicate/HAVING arguments travel in one string using the native CLI's
// spec grammar ("hi=-720;where_in=1,2;having=sum>=1e10") so one spec string can
// be executed through every path and the outputs compared byte for byte.
//
// Latency is split into separately reported quantities:
//   index_load_ms  one-time mmap + dictionary parse, process-resident (cached)
//   bind/init ms   the exact reduction, i.e. what an index-scan operator does
//   sql_*_ms       the complete request: query + full chunk drain
//
// Two engine protocols are measured and never mixed:
//   plain      duckdb_query(sql) every execution  (parse + plan + execute + fetch)
//   prepared   duckdb_prepare once, then duckdb_execute_prepared + fetch
//
// Modes:
//   query  <cube> <dict> <out.json> [spec]   one request (cold-process use)
//   bench  <cube> <dict> <reps> [spec]       warm, unprepared protocol
//   benchp <cube> <dict> <reps> [spec]       warm, prepared protocol
//   panel  <cube> <dict> <panel.tsv> <reps>  warm panel (unprepared)
//   panelp <cube> <dict> <panel.tsv> <reps>  warm panel (prepared)
//   scan   <db> <panel.tsv> <reps>           native DuckDB scan of the same SQL
//   mv     <db> <panel.tsv> <reps>           DuckDB materialized-view control
//   load   <cube> <dict>                     index load only
#include "duckdb.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "ecube_index.hpp"

namespace {

ecube::LoadedIndex* g_idx = nullptr;
std::string g_idx_key;
double g_load_ms = 0.0;
double g_bind_ms = 0.0;
double g_init_ms = 0.0;
long g_bind_calls = 0;
std::string g_db_path;  // set by scan/mv modes for nicer output

const char* SQL_PREFIX =
    "SELECT passenger_count AS g, SUM(fare_cents) AS s, COUNT(*) AS c, "
    "MIN(fare_cents) AS mn, MAX(fare_cents) AS mx FROM nyc ";

void ensure_index(const std::string& cube, const std::string& dict) {
    const std::string key = cube + '\x1f' + dict;
    if (g_idx && g_idx_key == key) return;
    delete g_idx;
    g_idx = new ecube::LoadedIndex();
    ecube::load_index(cube, dict, *g_idx);
    g_idx_key = key;
    g_load_ms = g_idx->load_ms;
}

struct TFData {  // bind state
    std::string spec;
    ecube::QSpec q;
};

struct TFState {  // execution state
    const ecube::Cube* c = nullptr;
    ecube::QSpec q;
    std::vector<ecube::Row> rows;
    idx_t pos = 0;
};

char* param_varchar(duckdb_bind_info info, idx_t i) {
    duckdb_value v = duckdb_bind_get_parameter(info, i);
    if (!v) return nullptr;
    char* s = duckdb_get_varchar(v);
    duckdb_destroy_value(&v);
    return s;
}

void bind_cb(duckdb_bind_info info) {
    const double t0 = ecube::now_ms();
    char* cube = param_varchar(info, 0);
    char* dict = param_varchar(info, 1);
    char* spec = param_varchar(info, 2);
    auto* b = new TFData();
    try {
        if (!cube || !dict) throw std::runtime_error("ecube: cube/dict path must not be NULL");
        b->spec = spec ? spec : "";
        b->q = ecube::parse_spec_string(b->spec);
        ensure_index(cube, dict);
    } catch (std::exception& e) {
        duckdb_bind_set_error(info, e.what());
        if (cube) duckdb_free(cube);
        if (dict) duckdb_free(dict);
        if (spec) duckdb_free(spec);
        delete b;
        return;
    }
    duckdb_bind_add_result_column(info, "name", duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR));
    duckdb_bind_add_result_column(info, "sum", duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
    duckdb_bind_add_result_column(info, "count", duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
    duckdb_bind_add_result_column(info, "min", duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
    duckdb_bind_add_result_column(info, "max", duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
    duckdb_bind_set_cardinality(info, g_idx->cube.h.ngroups, false);
    duckdb_bind_set_bind_data(info, b, [](void* p) { delete static_cast<TFData*>(p); });
    if (cube) duckdb_free(cube);
    if (dict) duckdb_free(dict);
    if (spec) duckdb_free(spec);
    g_bind_ms += ecube::now_ms() - t0;
    ++g_bind_calls;
}

void init_cb(duckdb_init_info info) {
    const double t0 = ecube::now_ms();
    auto* b = static_cast<TFData*>(duckdb_init_get_bind_data(info));
    auto* s = new TFState();
    s->c = &g_idx->cube;
    s->q = b->q;
    ecube::reduce(*s->c, s->q, s->rows);
    // The spec argument carries the COMPLETE query, exactly as the native CLI
    // does, so HAVING is applied here: the function then emits precisely the
    // rows of the requested query (this is what makes the table-function output
    // byte-identical to `enc_cube query` on every spec).
    if (!s->q.having.empty()) {
        std::vector<ecube::Row> kept;
        kept.reserve(s->rows.size());
        for (const auto& r : s->rows) {
            bool ok = true;
            for (const auto& h : s->q.having)
                if (!ecube::parse_having(h, r.sum, r.cnt, r.mn, r.mx, r.val_null)) { ok = false; break; }
            if (ok) kept.push_back(r);
        }
        s->rows.swap(kept);
    }
    duckdb_init_set_init_data(info, s, [](void* p) { delete static_cast<TFState*>(p); });
    g_init_ms += ecube::now_ms() - t0;
}

void fn_cb(duckdb_function_info info, duckdb_data_chunk out) {
    auto* s = static_cast<TFState*>(duckdb_function_get_init_data(info));
    const idx_t n = std::min<idx_t>(2048, idx_t(s->rows.size() - s->pos));
    duckdb_data_chunk_set_size(out, n);
    auto* sums = static_cast<int64_t*>(duckdb_vector_get_data(duckdb_data_chunk_get_vector(out, 1)));
    auto* counts = static_cast<int64_t*>(duckdb_vector_get_data(duckdb_data_chunk_get_vector(out, 2)));
    auto* mins = static_cast<int64_t*>(duckdb_vector_get_data(duckdb_data_chunk_get_vector(out, 3)));
    auto* maxs = static_cast<int64_t*>(duckdb_vector_get_data(duckdb_data_chunk_get_vector(out, 4)));
    for (idx_t i = 0; i < n; ++i) {
        const ecube::Row& r = s->rows[s->pos + i];
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(out, 0), i,
                                           s->c->names[r.g].c_str());
        sums[i] = r.sum;
        counts[i] = int64_t(r.cnt);
        mins[i] = r.mn;
        maxs[i] = r.mx;
        if (r.val_null) {
            for (idx_t col : {idx_t(1), idx_t(3), idx_t(4)}) {
                auto vec = duckdb_data_chunk_get_vector(out, col);
                duckdb_vector_ensure_validity_writable(vec);
                duckdb_validity_set_row_invalid(duckdb_vector_get_validity(vec), i);
            }
        }
    }
    s->pos += n;
}

// A bounded, reproducible engine config so that a "cold DuckDB process" cost is
// not entangled with the host's 1 TB of RAM.
void apply_config(duckdb_config cfg) {
    duckdb_set_config(cfg, "threads", "1");
    duckdb_set_config(cfg, "memory_limit", "8GB");
    duckdb_set_config(cfg, "preserve_insertion_order", "true");
}

duckdb_connection open_memory(duckdb_database* db) {
    duckdb_config cfg;
    if (duckdb_create_config(&cfg) != DuckDBSuccess) throw std::runtime_error("duckdb config");
    apply_config(cfg);
    char* err = nullptr;
    if (duckdb_open_ext(nullptr, db, cfg, &err) != DuckDBSuccess) {
        std::string e = err ? err : "?";
        if (err) duckdb_free(err);
        duckdb_destroy_config(&cfg);
        throw std::runtime_error("duckdb_open_ext: " + e);
    }
    duckdb_destroy_config(&cfg);
    duckdb_connection con;
    if (duckdb_connect(*db, &con) != DuckDBSuccess) throw std::runtime_error("duckdb_connect");
    g_load_ms = 0.0;
    return con;
}

duckdb_connection open_file_readonly(duckdb_database* db, const std::string& path) {
    duckdb_config cfg;
    if (duckdb_create_config(&cfg) != DuckDBSuccess) throw std::runtime_error("duckdb config");
    apply_config(cfg);
    duckdb_set_config(cfg, "access_mode", "READ_ONLY");
    char* err = nullptr;
    if (duckdb_open_ext(path.c_str(), db, cfg, &err) != DuckDBSuccess) {
        std::string e = err ? err : "?";
        if (err) duckdb_free(err);
        duckdb_destroy_config(&cfg);
        throw std::runtime_error("duckdb_open_ext(file): " + e);
    }
    duckdb_destroy_config(&cfg);
    duckdb_connection con;
    if (duckdb_connect(*db, &con) != DuckDBSuccess) throw std::runtime_error("duckdb_connect");
    g_db_path = path;
    return con;
}

void register_ecube(duckdb_connection con) {
    duckdb_table_function tf = duckdb_create_table_function();
    duckdb_table_function_set_name(tf, "ecube");
    duckdb_logical_type vt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(tf, vt);
    duckdb_table_function_add_parameter(tf, vt);
    duckdb_table_function_add_parameter(tf, vt);
    duckdb_table_function_set_bind(tf, bind_cb);
    duckdb_table_function_set_init(tf, init_cb);
    duckdb_table_function_set_function(tf, fn_cb);
    if (duckdb_register_table_function(con, tf) != DuckDBSuccess)
        throw std::runtime_error("ecube registration failed");
    duckdb_destroy_logical_type(&vt);
    duckdb_destroy_table_function(&tf);
}

void drain(duckdb_result* res) {
    for (;;) {
        duckdb_data_chunk chunk = duckdb_fetch_chunk(*res);
        if (!chunk) break;
        duckdb_destroy_data_chunk(&chunk);
    }
}

idx_t run_sql(duckdb_connection con, const std::string& sql) {
    duckdb_result res;
    if (duckdb_query(con, sql.c_str(), &res) != DuckDBSuccess) {
        std::string e = duckdb_result_error(&res);
        duckdb_destroy_result(&res);
        throw std::runtime_error("query: " + e + "\nSQL: " + sql);
    }
    const idx_t rows = duckdb_row_count(&res);
    drain(&res);
    duckdb_destroy_result(&res);
    return rows;
}

// Prepared-statement protocol: parse/plan once, then execute+fetch per rep.
struct Prepared {
    duckdb_prepared_statement stmt = nullptr;
    duckdb_connection con = nullptr;
    std::string sql;
    idx_t rows = 0;

    explicit Prepared(duckdb_connection c) : con(c) {}
    ~Prepared() { if (stmt) duckdb_destroy_prepare(&stmt); }

    void prepare(const std::string& s) {
        sql = s;
        if (duckdb_prepare(con, s.c_str(), &stmt) != DuckDBSuccess) {
            std::string e = duckdb_prepare_error(stmt);
            duckdb_destroy_prepare(&stmt);
            stmt = nullptr;
            throw std::runtime_error("prepare: " + e + "\nSQL: " + s);
        }
    }

    idx_t execute() {
        duckdb_result res;
        if (duckdb_execute_prepared(stmt, &res) != DuckDBSuccess) {
            std::string e = duckdb_result_error(&res);
            duckdb_destroy_result(&res);
            throw std::runtime_error("execute_prepared: " + e);
        }
        idx_t r = duckdb_row_count(&res);
        drain(&res);
        duckdb_destroy_result(&res);
        return r;
    }
};

// Exactness path only (not timed): drain a query into "name|sum|count|min|max".
std::vector<std::string> run_sql_text(duckdb_connection con, const std::string& sql) {
    duckdb_result res;
    if (duckdb_query(con, sql.c_str(), &res) != DuckDBSuccess) {
        std::string e = duckdb_result_error(&res);
        duckdb_destroy_result(&res);
        throw std::runtime_error("query: " + e);
    }
    std::vector<std::string> out;
    const idx_t rows = duckdb_row_count(&res);
    for (idx_t r = 0; r < rows; ++r) {
        std::string line;
        for (idx_t c = 0; c < 5; ++c) {
            char* v = duckdb_value_varchar(&res, c, r);
            if (c) line += '|';
            line += v ? v : "NULL";
            duckdb_free(v);
        }
        out.push_back(std::move(line));
    }
    duckdb_destroy_result(&res);
    return out;
}

std::string sql_quote(const std::string& s) {
    std::string o;
    for (char ch : s) { if (ch == '\'') o += "''"; else o += ch; }
    return o;
}

std::string sql_for(const std::string& cube, const std::string& dict, const std::string& spec) {
    return "SELECT name, sum, count, min, max FROM ecube('" + sql_quote(cube) + "','" +
           sql_quote(dict) + "','" + sql_quote(spec) + "') ORDER BY name";
}

struct Stats {
    std::vector<double> v;
    void sort() { std::sort(v.begin(), v.end()); }
    double median() const { return v.empty() ? 0 : v[v.size() / 2]; }
    double min() const { return v.empty() ? 0 : v.front(); }
    double p95() const { return v.empty() ? 0 : v[size_t(v.size() * 95 / 100)]; }
};

void print_build_info(const ecube::Cube& c) {
    std::printf("\"format\":%u,\"groups\":%llu,\"values\":%llu,\"index_bytes\":%llu", c.format,
                (unsigned long long)c.h.ngroups, (unsigned long long)c.h.nvalues,
                (unsigned long long)c.bytes);
}

int mode_query(const char* cube, const char* dict, const char* out, const std::string& spec) {
    const double t0 = ecube::now_ms();
    duckdb_database db;
    duckdb_connection con = open_memory(&db);
    register_ecube(con);
    const double t_open = ecube::now_ms();
    const std::string sql = sql_for(cube, dict, spec);
    idx_t rows = run_sql(con, sql);
    const double t1 = ecube::now_ms();
    auto text = run_sql_text(con, sql);
    std::string base;
    {
        ecube::LoadedIndex li;
        ecube::load_index(cube, dict, li);
        auto q = ecube::parse_spec_string(spec);
        std::vector<ecube::Row> rws;
        ecube::reduce(li.cube, q, rws);
        base = ecube::to_json(li.cube, rws, q);
    }
    std::printf("{\"engine\":\"duckdb\",\"protocol\":\"cold_process\",\"format\":%u,"
                "\"t_main_to_done_ms\":%.6f,\"engine_open_ms\":%.6f,\"index_load_ms\":%.6f,"
                "\"bind_ms\":%.6f,\"init_ms\":%.6f,\"sql_ms\":%.6f,\"rows\":%llu,"
                "\"engine_rows\":[",
                g_idx->cube.format, t1 - t0, t_open - t0, g_load_ms, g_bind_ms, g_init_ms,
                t1 - t_open, (unsigned long long)rows);
    for (size_t i = 0; i < text.size(); ++i) {
        if (i) std::printf(",");
        std::vector<std::string> f;
        size_t p = 0;
        while (p <= text[i].size()) {
            size_t z = text[i].find('|', p);
            if (z == std::string::npos) z = text[i].size();
            f.push_back(text[i].substr(p, z - p));
            p = z + 1;
        }
        std::printf("[\"%s\",%s,%s,%s,%s]", f[0].c_str(), f[1].c_str(), f[2].c_str(),
                    f[3].c_str(), f[4].c_str());
    }
    std::printf("]}\n");
    FILE* f = std::fopen(out, "wb");
    if (f) { std::fwrite(base.data(), 1, base.size(), f); std::fclose(f); }
    duckdb_disconnect(&con);
    duckdb_close(&db);
    return 0;
}

int mode_bench(const char* cube, const char* dict, int reps, const std::string& spec,
               bool prepared) {
    duckdb_database db;
    duckdb_connection con = open_memory(&db);
    register_ecube(con);
    const std::string sql = sql_for(cube, dict, spec);
    run_sql(con, sql);  // warmup; also loads the index
    const double load_ms = g_load_ms;
    const idx_t rows = prepared ? 0 : run_sql(con, sql);
    Stats s;
    if (prepared) {
        Prepared p(con);
        p.prepare(sql);
        p.execute();  // warm the prepared plan
        for (int i = 0; i < reps; ++i) {
            const double t = ecube::now_ms();
            p.execute();
            s.v.push_back(ecube::now_ms() - t);
        }
    } else {
        for (int i = 0; i < reps; ++i) {
            const double t = ecube::now_ms();
            run_sql(con, sql);
            s.v.push_back(ecube::now_ms() - t);
        }
    }
    s.sort();
    std::printf("{\"engine\":\"duckdb\",\"protocol\":\"%s\",",
                prepared ? "warm_prepared" : "warm_engine_resident");
    print_build_info(g_idx->cube);
    std::printf(",\"index_load_ms\":%.6f,\"bind_ms\":%.6f,\"init_ms\":%.6f,"
                "\"sql_median_ms\":%.6f,\"sql_min_ms\":%.6f,\"sql_p95_ms\":%.6f,"
                "\"reps\":%d,\"rows\":%llu}\n",
                load_ms, g_bind_ms / double(g_bind_calls ? g_bind_calls : 1),
                g_init_ms / double(g_bind_calls ? g_bind_calls : 1), s.median(), s.min(),
                s.p95(), reps, (unsigned long long)rows);
    duckdb_disconnect(&con);
    duckdb_close(&db);
    return 0;
}

// DuckDB's own per-query floor: prepared `SELECT 42`, same loop shape as the
// cube measurement, so the engine tax can be separated from the index cost.
int mode_floor(int reps, bool prepared) {
    duckdb_database db;
    duckdb_connection con = open_memory(&db);
    const std::string sql = "SELECT 42 AS x";
    Stats s;
    if (prepared) {
        Prepared p(con);
        p.prepare(sql);
        p.execute();
        for (int i = 0; i < reps; ++i) {
            const double t = ecube::now_ms();
            p.execute();
            s.v.push_back(ecube::now_ms() - t);
        }
    } else {
        run_sql(con, sql);
        for (int i = 0; i < reps; ++i) {
            const double t = ecube::now_ms();
            run_sql(con, sql);
            s.v.push_back(ecube::now_ms() - t);
        }
    }
    s.sort();
    std::printf("{\"engine\":\"duckdb\",\"mode\":\"floor\",\"protocol\":\"%s\","
                "\"sql\":\"%s\",\"median_ms\":%.6f,\"min_ms\":%.6f,\"p95_ms\":%.6f,"
                "\"reps\":%d}\n", prepared ? "warm_prepared" : "warm_engine_resident",
                sql.c_str(), s.median(), s.min(), s.p95(), reps);
    duckdb_disconnect(&con);
    duckdb_close(&db);
    return 0;
}

int mode_load(const char* cube, const char* dict) {
    ecube::LoadedIndex li;
    ecube::load_index(cube, dict, li);
    std::printf("{\"index_load_ms\":%.6f,", li.load_ms);
    print_build_info(li.cube);
    std::printf("}\n");
    return 0;
}

// ---- panel definitions ------------------------------------------------------
// One tab-separated case per line:  name <TAB> cube_spec <TAB> scan_where <TAB> scan_having
struct PanelCase {
    std::string name, spec, where, having;
};

std::vector<PanelCase> read_panel(const char* path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("panel open");
    std::vector<PanelCase> out;
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::vector<std::string> fld;
        size_t p = 0;
        while (p <= line.size()) {
            size_t z = line.find('\t', p);
            if (z == std::string::npos) z = line.size();
            fld.push_back(line.substr(p, z - p));
            p = z + 1;
        }
        while (fld.size() < 4) fld.push_back("");
        out.push_back({fld[0], fld[1], fld[2], fld[3]});
    }
    return out;
}

std::string scan_sql(const PanelCase& c, bool order_by) {
    std::string sql = SQL_PREFIX;
    if (!c.where.empty()) sql += "WHERE " + c.where + " ";
    sql += "GROUP BY 1";
    if (!c.having.empty()) sql += " HAVING " + c.having;
    if (order_by) sql += " ORDER BY 1";
    return sql;
}

int mode_panel(const char* cube, const char* dict, const char* panel, int reps, bool prepared) {
    duckdb_database db;
    duckdb_connection con = open_memory(&db);
    register_ecube(con);
    auto cases = read_panel(panel);
    std::printf("{\"engine\":\"duckdb\",\"protocol\":\"%s\",\"cases\":[",
                prepared ? "warm_prepared" : "warm_engine_resident");
    bool first = true;
    for (auto& cs : cases) {
        const std::string sql = sql_for(cube, dict, cs.spec);
        run_sql(con, sql);  // warmup
        Stats s;
        if (prepared) {
            Prepared p(con);
            p.prepare(sql);
            p.execute();
            for (int i = 0; i < reps; ++i) {
                const double t = ecube::now_ms();
                p.execute();
                s.v.push_back(ecube::now_ms() - t);
            }
        } else {
            for (int i = 0; i < reps; ++i) {
                const double t = ecube::now_ms();
                run_sql(con, sql);
                s.v.push_back(ecube::now_ms() - t);
            }
        }
        s.sort();
        auto txt = run_sql_text(con, sql);
        std::string canon;
        for (auto& line : txt) canon += line + "\n";
        if (!first) std::printf(",");
        first = false;
        std::printf("{\"name\":\"%s\",\"spec\":\"%s\",\"median_ms\":%.6f,\"min_ms\":%.6f,"
                    "\"p95_ms\":%.6f,\"reps\":%d,\"rows\":%zu}",
                    cs.name.c_str(), cs.spec.c_str(), s.median(), s.min(), s.p95(), reps,
                    txt.size());
    }
    std::printf("],");
    print_build_info(g_idx->cube);
    std::printf(",\"index_load_ms\":%.6f}\n", g_load_ms);
    duckdb_disconnect(&con);
    duckdb_close(&db);
    return 0;
}

int mode_scan(const char* dbpath, const char* panel, int reps, bool with_prepare) {
    duckdb_database db;
    duckdb_connection con = open_file_readonly(&db, dbpath);
    auto cases = read_panel(panel);
    // reference answer from the engine itself (exactness ground truth)
    std::printf("{\"engine\":\"duckdb_native_scan\",\"db\":\"%s\",\"protocol\":\"%s\","
                "\"cases\":[", dbpath, with_prepare ? "warm_prepared" : "warm_engine_resident");
    bool first = true;
    for (auto& cs : cases) {
        const std::string sql = scan_sql(cs, true);
        run_sql(con, sql);  // warmup (page cache + plan)
        Stats s;
        if (with_prepare) {
            Prepared p(con);
            p.prepare(sql);
            p.execute();
            for (int i = 0; i < reps; ++i) {
                const double t = ecube::now_ms();
                p.execute();
                s.v.push_back(ecube::now_ms() - t);
            }
        } else {
            for (int i = 0; i < reps; ++i) {
                const double t = ecube::now_ms();
                run_sql(con, sql);
                s.v.push_back(ecube::now_ms() - t);
            }
        }
        s.sort();
        auto txt = run_sql_text(con, sql);
        std::string canon;
        for (auto& line : txt) canon += line + "\n";
        if (!first) std::printf(",");
        first = false;
        std::printf("{\"name\":\"%s\",\"sql\":\"%s\",\"median_ms\":%.6f,\"min_ms\":%.6f,"
                    "\"p95_ms\":%.6f,\"reps\":%d,\"rows\":%zu}",
                    cs.name.c_str(), sql_quote(sql).c_str(), s.median(), s.min(), s.p95(),
                    reps, txt.size());
    }
    std::printf("]}\n");
    duckdb_disconnect(&con);
    duckdb_close(&db);
    return 0;
}

// Materialized-view control: one exact aggregate table per panel query (this is
// the "DBA materialises the answer" baseline). The build cost is reported
// separately; the measured latency is the small-table read only.
int mode_mv(const char* dbpath, const char* panel, int reps) {
    duckdb_database db;
    duckdb_connection con = open_file_readonly(&db, dbpath);
    auto cases = read_panel(panel);
    std::printf("{\"engine\":\"duckdb_materialized_view\",\"db\":\"%s\",\"cases\":[", dbpath);
    bool first = true;
    double total_build = 0;
    for (size_t i = 0; i < cases.size(); ++i) {
        auto& cs = cases[i];
        const std::string mv = "mv_" + std::to_string(i);
        const double tb = ecube::now_ms();
        run_sql(con, "CREATE TEMP TABLE " + mv + " AS " + scan_sql(cs, false));
        const double build_ms = ecube::now_ms() - tb;
        total_build += build_ms;
        std::string q = "SELECT g, s, c, mn, mx FROM " + mv;
        if (!cs.having.empty()) q += " WHERE " + cs.having;
        q += " ORDER BY g";
        run_sql(con, q);  // warmup
        Stats s;
        for (int r = 0; r < reps; ++r) {
            const double t = ecube::now_ms();
            run_sql(con, q);
            s.v.push_back(ecube::now_ms() - t);
        }
        s.sort();
        std::string sz = "?";
        {
            duckdb_result res;
            const std::string cnt_sql = "SELECT count(*) FROM " + mv;
            if (duckdb_query(con, cnt_sql.c_str(), &res) == DuckDBSuccess) {
                char* v = duckdb_value_varchar(&res, 0, 0);
                sz = v ? v : "?";
                duckdb_free(v);
                duckdb_destroy_result(&res);
            }
        }
        if (!first) std::printf(",");
        first = false;
        std::printf("{\"name\":\"%s\",\"build_ms\":%.6f,\"median_ms\":%.6f,\"min_ms\":%.6f,"
                    "\"p95_ms\":%.6f,\"reps\":%d,\"rows\":%s,\"query\":\"%s\"}",
                    cs.name.c_str(), build_ms, s.median(), s.min(), s.p95(), reps, sz.c_str(),
                    sql_quote(q).c_str());
    }
    std::printf("],\"total_build_ms\":%.6f}\n", total_build);
    duckdb_disconnect(&con);
    duckdb_close(&db);
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            std::fprintf(stderr,
                         "usage:\n  ecube_duck query  <cube> <dict> <out.json> [spec]\n"
                         "  ecube_duck bench  <cube> <dict> <reps> [spec]\n"
                         "  ecube_duck benchp <cube> <dict> <reps> [spec]\n"
                         "  ecube_duck panel  <cube> <dict> <panel.tsv> <reps>\n"
                         "  ecube_duck panelp <cube> <dict> <panel.tsv> <reps>\n"
                         "  ecube_duck scan   <db> <panel.tsv> <reps>\n"
                         "  ecube_duck scanp  <db> <panel.tsv> <reps>\n"
                         "  ecube_duck mv     <db> <panel.tsv> <reps>\n"
                         "  ecube_duck floor  <reps>\n"
                         "  ecube_duck floorp <reps>\n"
                         "  ecube_duck load   <cube> <dict>\n");
            return 2;
        }
        const std::string mode = argv[1];
        if (mode == "query" && argc >= 5)
            return mode_query(argv[2], argv[3], argv[4], argc > 5 ? argv[5] : "");
        if (mode == "bench" && argc >= 5)
            return mode_bench(argv[2], argv[3], std::stoi(argv[4]), argc > 5 ? argv[5] : "", false);
        if (mode == "benchp" && argc >= 5)
            return mode_bench(argv[2], argv[3], std::stoi(argv[4]), argc > 5 ? argv[5] : "", true);
        if (mode == "panel" && argc >= 5)
            return mode_panel(argv[2], argv[3], argv[4], std::stoi(argv[5]), false);
        if (mode == "panelp" && argc >= 5)
            return mode_panel(argv[2], argv[3], argv[4], std::stoi(argv[5]), true);
        if (mode == "scan" && argc >= 5) return mode_scan(argv[2], argv[3], std::stoi(argv[4]), false);
        if (mode == "scanp" && argc >= 5) return mode_scan(argv[2], argv[3], std::stoi(argv[4]), true);
        if (mode == "mv" && argc >= 5) return mode_mv(argv[2], argv[3], std::stoi(argv[4]));
        if (mode == "floor" && argc >= 3) return mode_floor(std::stoi(argv[2]), false);
        if (mode == "floorp" && argc >= 3) return mode_floor(std::stoi(argv[2]), true);
        if (mode == "load" && argc >= 4) return mode_load(argv[2], argv[3]);
        std::fprintf(stderr, "bad mode/args\n");
        return 2;
    } catch (std::exception& e) {
        std::fprintf(stderr, "ERR %s\n", e.what());
        return 1;
    }
}
