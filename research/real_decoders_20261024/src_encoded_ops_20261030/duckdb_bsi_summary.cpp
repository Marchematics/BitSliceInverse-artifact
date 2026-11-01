// DuckDB C API table function exposing exact block-summary rows.
#include "duckdb.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>
struct H { char magic[8]; uint64_t nblocks, ngroups, rows; uint32_t gid, vid; char binding[40]; };
struct E { int64_t sum, count; int32_t min, max; uint8_t has; };
struct Row { std::string name; int64_t sum, count; int32_t min, max; };
struct Bind { std::vector<Row> rows; size_t pos = 0; };
static std::vector<std::string> dict_names(const std::string& p) {
    std::ifstream f(p, std::ios::binary); std::string d((std::istreambuf_iterator<char>(f)), {});
    if (d.size() < 52 || d.compare(0,8,"BSSTR01\n")) throw std::runtime_error("dict");
    const char* q=d.data()+48; uint32_t k; memcpy(&k,q,4); q+=4; std::vector<std::string> n;
    for(uint32_t i=0;i<k;i++){uint32_t len;memcpy(&len,q,4);q+=4;n.emplace_back(q,len);q+=len;} return n;
}
static void bind_cb(duckdb_bind_info info) {
    auto* pathv = duckdb_bind_get_parameter(info, 0);
    auto* dictv = duckdb_bind_get_parameter(info, 1);
    char* path = duckdb_get_varchar(pathv); char* dict = duckdb_get_varchar(dictv);
    std::ifstream f(path, std::ios::binary); std::vector<char> data((std::istreambuf_iterator<char>(f)), {}); fprintf(stderr,"BIND path=%s size=%zu magic=%.8s\n",path,data.size(),data.empty()?"EMPTY":data.data());
    H h; memcpy(&h,data.data(),sizeof(h)); if(memcmp(h.magic,"BSSUM01\n",8)) throw std::runtime_error("summary");
    auto* ent = reinterpret_cast<const E*>(data.data()+sizeof(H)); auto names=dict_names(dict);
    std::vector<__int128> sum(h.ngroups,0); std::vector<int64_t> cnt(h.ngroups,0);
    std::vector<int32_t> mn(h.ngroups,INT32_MAX), mx(h.ngroups,INT32_MIN);
    for(uint64_t b=0;b<h.nblocks;b++) for(uint64_t g=0;g<h.ngroups;g++) {
        const E& e=ent[b*h.ngroups+g]; if(!e.has) continue;
        sum[g]+=e.sum; cnt[g]+=e.count; mn[g]=std::min(mn[g],e.min); mx[g]=std::max(mx[g],e.max);
    }
    auto* b = new Bind();
    for(uint64_t g=0;g<h.ngroups && g<names.size();g++) if(cnt[g])
        b->rows.push_back({names[g],(int64_t)sum[g],cnt[g],mn[g],mx[g]});
    duckdb_bind_add_result_column(info,"name",duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR));
    duckdb_bind_add_result_column(info,"sum",duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
    duckdb_bind_add_result_column(info,"count",duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
    duckdb_bind_add_result_column(info,"min",duckdb_create_logical_type(DUCKDB_TYPE_INTEGER));
    duckdb_bind_add_result_column(info,"max",duckdb_create_logical_type(DUCKDB_TYPE_INTEGER));
    duckdb_bind_set_cardinality(info,b->rows.size(),true);
    duckdb_bind_set_bind_data(info,b,[](void* p){delete (Bind*)p;});
    duckdb_free(path); duckdb_free(dict);
}
static void init_cb(duckdb_init_info info){ duckdb_init_set_init_data(info,duckdb_init_get_bind_data(info),nullptr); }
static void fn_cb(duckdb_function_info info, duckdb_data_chunk output) {
    auto* b = (Bind*)duckdb_function_get_init_data(info);
    idx_t n = std::min<idx_t>(2048, b->rows.size()-b->pos); duckdb_data_chunk_set_size(output,n);
    auto* names=(duckdb_string_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output,0));
    auto* sums=(int64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output,1));
    auto* counts=(int64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output,2));
    auto* mins=(int32_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output,3));
    auto* maxs=(int32_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output,4));
    for(idx_t i=0;i<n;i++){ auto& r=b->rows[b->pos+i]; duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output,0),i,r.name.c_str()); sums[i]=r.sum; counts[i]=r.count; mins[i]=r.min; maxs[i]=r.max; }
    b->pos += n;
}
int main(int argc,char**argv){
    if(argc<4){fprintf(stderr,"usage: summary_dict DICT QUERY\n");return 2;}
    duckdb_database db; duckdb_connection con;
    if(duckdb_open(nullptr,&db)!=DuckDBSuccess) return 1;
    if(duckdb_connect(db,&con)!=DuckDBSuccess) return 1;
    auto tf=duckdb_create_table_function(); duckdb_table_function_set_name(tf,"bsi_summary");
    auto vt=duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR); duckdb_table_function_add_parameter(tf,vt); duckdb_table_function_add_parameter(tf,vt);
    duckdb_table_function_set_bind(tf,bind_cb); duckdb_table_function_set_init(tf,init_cb); duckdb_table_function_set_function(tf,fn_cb);
    if(duckdb_register_table_function(con,tf)!=DuckDBSuccess){fprintf(stderr,"register failed\n");return 1;}
    duckdb_result res; auto t0=std::chrono::steady_clock::now();
    if(duckdb_query(con,argv[3],&res)!=DuckDBSuccess){fprintf(stderr,"query: %s\n",duckdb_result_error(&res));return 1;}
    auto t1=std::chrono::steady_clock::now();
    idx_t rows=duckdb_row_count(&res), cols=duckdb_column_count(&res);
    for(idx_t r=0;r<rows;r++){ for(idx_t c=0;c<cols;c++){ char* s=duckdb_value_varchar(&res,c,r); printf("%s%s",s,c+1==cols?"\n":"|"); duckdb_free(s);} }
    fprintf(stderr,"duckdb_query_ms=%.3f rows=%llu\n",std::chrono::duration<double,std::milli>(t1-t0).count(),(unsigned long long)rows);
    duckdb_destroy_result(&res); duckdb_disconnect(&con); duckdb_close(&db); return 0;
}
