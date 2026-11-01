// DuckDB table function exposing each exact block-summary entry; DuckDB performs GROUP BY.
#include "duckdb.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>
struct H { char magic[8]; uint64_t nblocks,ngroups,rows; uint32_t gid,vid; char binding[40]; };
struct E { int64_t sum,count; int32_t min,max; uint8_t has; };
struct Row { const char* name; int64_t sum,count; int32_t min,max; };
struct Bind { std::vector<std::string> names; std::vector<char> data; const E* ent=nullptr; uint64_t nblocks=0,ngroups=0; size_t total=0,pos=0; };
static std::vector<std::string> dict_names(const std::string& p){ std::ifstream f(p,std::ios::binary); std::string d((std::istreambuf_iterator<char>(f)),{}); if(d.size()<52||d.compare(0,8,"BSSTR01\n")) throw std::runtime_error("dict"); const char* q=d.data()+48; uint32_t k; memcpy(&k,q,4); q+=4; std::vector<std::string> n; for(uint32_t i=0;i<k;i++){uint32_t l;memcpy(&l,q,4);q+=4;n.emplace_back(q,l);q+=l;} return n; }
static void bind_cb(duckdb_bind_info info){
 auto* pv=duckdb_bind_get_parameter(info,0); auto* dv=duckdb_bind_get_parameter(info,1); char* path=duckdb_get_varchar(pv); char* dict=duckdb_get_varchar(dv);
 std::ifstream f(path,std::ios::binary); std::string data((std::istreambuf_iterator<char>(f)),{}); H h; memcpy(&h,data.data(),sizeof(h)); if(memcmp(h.magic,"BSSUM01\n",8)) throw std::runtime_error("summary");
 auto* b=new Bind(); b->data.assign(data.begin(),data.end()); b->ent=reinterpret_cast<const E*>(b->data.data()+sizeof(H)); b->nblocks=h.nblocks; b->ngroups=h.ngroups; b->names=dict_names(dict);
 for(uint64_t i=0;i<h.nblocks*h.ngroups;i++) if(b->ent[i].has) b->total++;
 duckdb_bind_add_result_column(info,"name",duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR));
 duckdb_bind_add_result_column(info,"sum",duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
 duckdb_bind_add_result_column(info,"count",duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
 duckdb_bind_add_result_column(info,"min",duckdb_create_logical_type(DUCKDB_TYPE_INTEGER));
 duckdb_bind_add_result_column(info,"max",duckdb_create_logical_type(DUCKDB_TYPE_INTEGER));
 duckdb_bind_set_cardinality(info,b->total,true); duckdb_bind_set_bind_data(info,b,[](void*p){delete (Bind*)p;}); duckdb_free(path); duckdb_free(dict);
}
static void init_cb(duckdb_init_info info){ duckdb_init_set_init_data(info,duckdb_init_get_bind_data(info),nullptr); }
static void fn_cb(duckdb_function_info info,duckdb_data_chunk out){
 auto* b=(Bind*)duckdb_function_get_init_data(info); idx_t n=0; std::vector<const E*> emit; std::vector<idx_t> gs;
 while(b->pos<b->nblocks*b->ngroups && n<2048){ idx_t i=b->pos++; const E& e=b->ent[i]; if(!e.has) continue; idx_t g=i%b->ngroups; if(g>=b->names.size()) continue; emit.push_back(&e); gs.push_back(g); n++; }
 duckdb_data_chunk_set_size(out,n);
 auto* sums=(int64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(out,1)); auto* cnts=(int64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(out,2)); auto* mins=(int32_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(out,3)); auto* maxs=(int32_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(out,4));
 for(idx_t j=0;j<n;j++){ auto&e=*emit[j]; duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(out,0),j,b->names[gs[j]].c_str()); sums[j]=e.sum; cnts[j]=e.count; mins[j]=e.min; maxs[j]=e.max; }
}
int main(int argc,char**argv){ if(argc<3)return 2; duckdb_database db; duckdb_connection con; if(duckdb_open(nullptr,&db)||duckdb_connect(db,&con))return 1; auto tf=duckdb_create_table_function(); duckdb_table_function_set_name(tf,"bsi_blocks"); auto vt=duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR); duckdb_table_function_add_parameter(tf,vt); duckdb_table_function_add_parameter(tf,vt); duckdb_table_function_set_bind(tf,bind_cb); duckdb_table_function_set_init(tf,init_cb); duckdb_table_function_set_function(tf,fn_cb); if(duckdb_register_table_function(con,tf))return 1; duckdb_result res; auto t0=std::chrono::steady_clock::now(); if(duckdb_query(con,argv[2],&res)){fprintf(stderr,"query: %s\n",duckdb_result_error(&res));return 1;} auto t1=std::chrono::steady_clock::now(); idx_t rows=duckdb_row_count(&res),cols=duckdb_column_count(&res); for(idx_t r=0;r<rows;r++){for(idx_t c=0;c<cols;c++){char*s=duckdb_value_varchar(&res,c,r);printf("%s%s",s,c+1==cols?"\n":"|");duckdb_free(s);}} fprintf(stderr,"duckdb_query_ms=%.3f rows=%llu\n",std::chrono::duration<double,std::milli>(t1-t0).count(),(unsigned long long)rows); duckdb_destroy_result(&res); duckdb_disconnect(&con); duckdb_close(&db); }
