// DuckDB table function over a small group-summary CSV (two-level summary output).
#include "duckdb.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
struct Row { std::string name; int64_t sum,count; int32_t min,max; };
struct Bind { std::vector<Row> rows; size_t pos=0; };
static void bind_cb(duckdb_bind_info info){
 auto* pv=duckdb_bind_get_parameter(info,0); char* path=duckdb_get_varchar(pv);
 std::ifstream f(path); std::string line; auto* b=new Bind(); std::getline(f,line);
 while(std::getline(f,line)){ std::stringstream ss(line); std::string n,s,c,mn,mx; std::getline(ss,n,','); std::getline(ss,s,','); std::getline(ss,c,','); std::getline(ss,mn,','); std::getline(ss,mx,','); if(n.empty())continue; b->rows.push_back({n,std::stoll(s),std::stoll(c),std::stoi(mn),std::stoi(mx)}); }
 duckdb_bind_add_result_column(info,"name",duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR));
 duckdb_bind_add_result_column(info,"sum",duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
 duckdb_bind_add_result_column(info,"count",duckdb_create_logical_type(DUCKDB_TYPE_BIGINT));
 duckdb_bind_add_result_column(info,"min",duckdb_create_logical_type(DUCKDB_TYPE_INTEGER));
 duckdb_bind_add_result_column(info,"max",duckdb_create_logical_type(DUCKDB_TYPE_INTEGER));
 duckdb_bind_set_cardinality(info,b->rows.size(),true); duckdb_bind_set_bind_data(info,b,[](void*p){delete (Bind*)p;}); duckdb_free(path);
}
static void init_cb(duckdb_init_info info){ duckdb_init_set_init_data(info,duckdb_init_get_bind_data(info),nullptr); }
static void fn_cb(duckdb_function_info info,duckdb_data_chunk out){
 auto* b=(Bind*)duckdb_function_get_init_data(info); idx_t n=std::min<idx_t>(2048,b->rows.size()-b->pos); duckdb_data_chunk_set_size(out,n);
 auto* sums=(int64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(out,1)); auto* cnts=(int64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(out,2));
 auto* mins=(int32_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(out,3)); auto* maxs=(int32_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(out,4));
 for(idx_t i=0;i<n;i++){ auto&r=b->rows[b->pos+i]; duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(out,0),i,r.name.c_str()); sums[i]=r.sum; cnts[i]=r.count; mins[i]=r.min; maxs[i]=r.max; }
 b->pos+=n;
}
int main(int argc,char**argv){ if(argc<3)return 2; duckdb_database db; duckdb_connection con; if(duckdb_open(nullptr,&db)||duckdb_connect(db,&con))return 1;
 auto tf=duckdb_create_table_function(); duckdb_table_function_set_name(tf,"bsi_groups"); auto vt=duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR); duckdb_table_function_add_parameter(tf,vt);
 duckdb_table_function_set_bind(tf,bind_cb); duckdb_table_function_set_init(tf,init_cb); duckdb_table_function_set_function(tf,fn_cb); if(duckdb_register_table_function(con,tf))return 1;
 duckdb_result res; auto t0=std::chrono::steady_clock::now(); if(duckdb_query(con,argv[2],&res)){fprintf(stderr,"query: %s\n",duckdb_result_error(&res));return 1;} auto t1=std::chrono::steady_clock::now();
 idx_t rows=duckdb_row_count(&res),cols=duckdb_column_count(&res); for(idx_t r=0;r<rows;r++){for(idx_t c=0;c<cols;c++){char*s=duckdb_value_varchar(&res,c,r);printf("%s%s",s,c+1==cols?"\n":"|");duckdb_free(s);}}
 fprintf(stderr,"duckdb_query_ms=%.3f rows=%llu\n",std::chrono::duration<double,std::milli>(t1-t0).count(),(unsigned long long)rows); duckdb_destroy_result(&res); duckdb_disconnect(&con); duckdb_close(&db); }
