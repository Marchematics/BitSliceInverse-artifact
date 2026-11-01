// Exact-query bridge, NOT a SQL parser, FSST implementation or native .fls reader.
// The reference differs from the frozen candidate only by its entrypoint name.
#include "../src_patch_fusion/fullsql_reference.hpp"
#include "streaming.hpp"
#include <optional>
#include <set>

static constexpr U STATE_ID=17, POP_ID=13, CODE_BIAS=0x80000000U;
static const std::string SUM_ALIAS="TEMP(Calculation_8840730155303010)(3219150654)(0)";
static const std::string IG_SQL="SELECT \"IGlocations1_1\".\"State\" AS \"State\",   SUM(CAST(\"IGlocations1_1\".\"POPESTIMATE2014\" AS BIGINT)) AS \"TEMP(Calculation_8840730155303010)(3219150654)(0)\" FROM \"IGlocations1_1\" WHERE (\"IGlocations1_1\".\"State\" NOT IN ('District of Columbia')) GROUP BY \"IGlocations1_1\".\"State\";\n";
static std::string sql_file(const std::string&path){std::ifstream f(path,std::ios::binary);need(bool(f),"SQL file");return {std::istreambuf_iterator<char>(f),std::istreambuf_iterator<char>()};}
static bool sql_empty(const std::string&dir){std::ifstream f(dir+"/empty.txt");if(!f)return false;std::string value{std::istreambuf_iterator<char>(f),std::istreambuf_iterator<char>()};need(value=="1\n","empty marker");return true;}
// Reject non-canonical UTF-8, surrogates and >U+10FFFF; comparison is exact bytes,
// with no normalization, locale, case folding or alternative collation.
static void utf8_exact(const std::string&s){
 for(size_t i=0;i<s.size();){unsigned c=(unsigned char)s[i++];uint32_t cp=0;unsigned k=0;
  if(c<0x80)continue;
  if(c>=0xc2&&c<=0xdf){cp=c&31;k=1;}else if(c>=0xe0&&c<=0xef){cp=c&15;k=2;}
  else if(c>=0xf0&&c<=0xf4){cp=c&7;k=3;}else throw std::runtime_error("UTF8 leading byte");
  need(i+k<=s.size(),"UTF8 truncated");unsigned count=k;
  while(k--){unsigned b=(unsigned char)s[i++];need((b&0xc0)==0x80,"UTF8 continuation");cp=(cp<<6)|(b&63);}
  need((count==1&&cp>=0x80)||(count==2&&cp>=0x800)||(count==3&&cp>=0x10000),"UTF8 overlong");
  need(cp<=0x10ffff&&!(cp>=0xd800&&cp<=0xdfff),"UTF8 scalar");
 }
}
static std::vector<std::string> sql_dictionary(const std::string&dir,const std::string&binding){
 std::ifstream f(dir+"/state.dict",std::ios::binary);need(bool(f),"dictionary input");char magic[8],source[40];f.read(magic,8);f.read(source,40);
 need(bool(f)&&std::string(magic,8)=="BSSTR01\n"&&std::string(source,40)==binding,"dictionary source binding");
 U k=rd<U>(f);need(k<=1000000,"dictionary cardinality");std::vector<std::string>d;d.reserve(k);std::set<std::string>seen;
 uint64_t total=0;for(U i=0;i<k;++i){U n=rd<U>(f);need(n<=1024&&total+n<=64ULL*1024*1024,"dictionary byte budget");total+=n;
  std::string s(n,'\0');f.read(s.data(),n);need(bool(f),"dictionary string truncation");utf8_exact(s);need(seen.insert(s).second,"dictionary unique");d.push_back(std::move(s));}
 need(f.peek()==std::char_traits<char>::eof(),"dictionary suffix");return d;
}
static Query3 sql_not_in(const std::vector<std::string>&d,const std::optional<std::string>&literal){
 Query3 q{{0,STATE_ID,{NE,0,{}}},-1};
 if(!literal){q.constant=0;return q;}// NULL RHS produces no TRUE rows, including NULL LHS.
 auto found=std::find(d.begin(),d.end(),*literal);
 if(found==d.end())q.constant=1;else q.query.p.value=CODE_BIAS+U(found-d.begin());
 return q;
}
static std::string sql_json(const std::string&s){
 std::string out="\"";const char*hex="0123456789abcdef";
 for(unsigned char c:s){if(c=='"'||c=='\\'){out+='\\';out+=char(c);}else if(c<32){out+="\\u00";out+=hex[c>>4];out+=hex[c&15];}else out+=char(c);}return out+'"';
}
using SqlWide=__int128_t;
static std::string sql_decimal(SqlWide x){
 if(x==0)return "0";bool neg=x<0;__uint128_t u=neg?__uint128_t(-(x+1))+1:__uint128_t(x);std::string s;
 while(u){s+=char('0'+u%10);u/=10;}if(neg)s+='-';std::reverse(s.begin(),s.end());return s;
}
struct SqlGroup{SqlWide sum=0;bool present=false,has_value=false;};
static U sql_code(U biased,size_t k){need(biased>=CODE_BIAS&&uint64_t(biased-CODE_BIAS)<k,"dictionary code domain");return biased-CODE_BIAS;}
static void sql_add(std::vector<SqlGroup>&groups,U code,bool payload_valid,U payload){
 need(code<groups.size(),"aggregate key domain");auto&g=groups[code];g.present=true;
 if(payload_valid){int64_t value=int64_t(payload)-int64_t(CODE_BIAS);g.sum+=SqlWide(value);g.has_value=true;}
 // At <=1,000,000 admitted INT32 rows, |sum| <= 2^31*10^6 < 2^51.
 // Thus this signed 128-bit addition cannot overflow; no float or int64 narrowing.
}
static std::string sql_result(const std::vector<std::string>&dict,const std::vector<SqlGroup>&groups){
 need(dict.size()==groups.size(),"aggregate dictionary size");std::vector<U>order;
 for(U i=0;i<groups.size();++i)if(groups[i].present)order.push_back(i);
 std::sort(order.begin(),order.end(),[&](U a,U b){return dict[a]<dict[b];});
 std::string s="{\"columns\":[{\"name\":\"State\",\"type\":\"VARCHAR\"},{\"name\":"+sql_json(SUM_ALIAS)+",\"type\":\"HUGEINT\"}],\"rows\":[";
 bool first=true;for(U i:order){if(!first)s+=',';first=false;s+='[';s+=sql_json(dict[i]);s+=',';s+=groups[i].has_value?sql_json(sql_decimal(groups[i].sum)):"null";s+=']';}return s+"]}\n";
}
static void sql_schema(const Table3&t){need(t.meta.size()==2&&t.cs.size()==2,"projected schema cardinality");
 U state=bank_column(t,STATE_ID),pop=bank_column(t,POP_ID);
 need(t.meta[state].width==32&&t.meta[state].family==5&&t.meta[pop].width==32&&t.meta[pop].family==2,"fixed projected source codec");
 need(t.meta[state].n==t.meta[pop].n,"projected row count");
 for(U col:{state,pop})need(t.cs[col].root->kind==CHUNKS,"fullsql chunk root");
}
static void sql_empty_encoded(const std::string&path,const std::string&binding,bool write){
 const std::string bytes="BSQEMP1\n"+binding;
 if(write){std::ofstream f(path,std::ios::binary);f.write(bytes.data(),bytes.size());f.flush();need(bool(f),"empty encoded write");}
 else need(sql_file(path)==bytes,"empty encoded source binding");
}
static int sql_selftest(){
 std::vector<std::string>d={"District of Columbia","","\xc3\xa9","e\xcc\x81","a\n\"\\","\xf0\x9f\x98\x80"};
 for(const auto&s:d)utf8_exact(s);need(sql_not_in(d,std::nullopt).constant==0,"NULL RHS lowering");
 need(sql_not_in(d,std::string("missing")).constant==1,"absent singleton lowering");
 auto q=sql_not_in(d,std::string("District of Columbia"));need(q.constant==-1&&q.query.p.value==CODE_BIAS,"present singleton lowering");
 need(!q.query.p.test(CODE_BIAS)&&q.query.p.test(CODE_BIAS+1),"singleton complement");
 for(const std::string&s:{std::string("\xc0\x80",2),std::string("\xed\xa0\x80",3),std::string("\xf4\x90\x80\x80",4),std::string("\xe2",1)}){bool rejected=false;try{utf8_exact(s);}catch(...){rejected=true;}need(rejected,"invalid UTF8 rejection");}
 std::vector<SqlGroup>g(d.size());sql_add(g,1,false,0);need(g[1].present&&!g[1].has_value,"all NULL group");
 sql_add(g,2,true,0);sql_add(g,2,true,UINT32_MAX);need(g[2].sum==-1,"INT32 endpoints wide sum");
 SqlWide huge=(SqlWide(1)<<80);need(sql_decimal(huge)=="1208925819614629174706176"&&sql_decimal(-huge)=="-1208925819614629174706176","wide decimal serializer");
 SqlWide widest=SqlWide((~__uint128_t(0))>>1);need(sql_decimal(widest)=="170141183460469231731687303715884105727"&&sql_decimal(-widest-1)=="-170141183460469231731687303715884105728","signed128 endpoint serializer");
 need(sql_result({},{}).find("\"rows\":[]")!=std::string::npos,"empty result");
 std::cout<<"{\"fullsql_helper_fixtures\":\"PASS\",\"observed_values_used\":false}\n";return 0;
}

// In-process microbenchmark of the decode/aggregate variants on one encoded
// table. Not a result panel: no fresh process, no DuckDB, no statistics. Used
// only to choose the kernel shape inside the direct adapter before the timed
// panel.
static int sql_bench(const std::string&encoded,const std::string&dir,U iters){
 std::string binding=binding3(dir+"/binding.txt");auto dict=sql_dictionary(dir,binding);need(!sql_empty(dir),"bench observed relation");
 Table3 table=load3(encoded,binding);sql_schema(table);const U st=bank_column(table,STATE_ID),pop=bank_column(table,POP_ID),n=table.meta[st].n;
 const bool st_nullable=table.meta[st].nullable,pop_nullable=table.meta[pop].nullable;const U k=U(dict.size());
 auto query=sql_not_in(dict,std::string("District of Columbia"));
 volatile int64_t sink=0;struct Row{const char*name;double ms;int64_t check;};std::vector<Row> rows;
 auto timeit=[&](const char*name,auto&&fn){auto t=Clock::now();int64_t c=0;for(U it=0;it<iters;++it)c+=fn();rows.push_back({name,ms(t),c});};
 auto base=[&](auto&&body){std::vector<SqlGroup>groups(k);Scratch keys,values;uint64_t blocks=0;
  for(U off=0;off<n;off+=V){const U take=std::min(V,n-off);decode_bulk(*table.cs[st].root->children[off/V],keys);decode_bulk(*table.cs[pop].root->children[off/V],values);blocks+=2;body(groups,keys,values,off,take);}
  if(query.constant==0)for(auto&g:groups)g.present=false;
  else if(query.constant<0)groups[sql_code(query.query.p.value,k)].present=false;
  int64_t check=0;for(U i=0;i<k;++i)if(groups[i].present)check+=int64_t(groups[i].sum)+(groups[i].has_value?1:0);
  sink+=check;return check;};
 timeit("decode_only",[&]{int64_t c=0;Scratch keys,values;for(U off=0;off<n;off+=V){decode_bulk(*table.cs[st].root->children[off/V],keys);decode_bulk(*table.cs[pop].root->children[off/V],values);c+=keys.d[0]+values.d[0];}sink+=c;return c;});
 timeit("gtf_frozen_loop",[&]{return base([&](auto&groups,auto&keys,auto&values,U off,U take){
  for(U i=0;i<take;++i){const U row=off+i;if(st_nullable&&!valid3(table.valid[st],row))continue;sql_add(groups,sql_code(keys.d[i],k),!pop_nullable||valid3(table.valid[pop],row),values.d[i]);}});});
 timeit("direct_plain",[&]{return base([&](auto&groups,auto&keys,auto&values,U off,U take){
  for(U i=0;i<take;++i){const U row=off+i;if(st_nullable&&!valid3(table.valid[st],row))continue;auto&g=groups[keys.d[i]-CODE_BIAS];g.present=true;
   if(!pop_nullable||valid3(table.valid[pop],row)){g.sum+=SqlWide(int64_t(values.d[i])-int64_t(CODE_BIAS));g.has_value=true;}}});});
 timeit("direct_run",[&]{return base([&](auto&groups,auto&keys,auto&values,U off,U take){
  U i=0;while(i<take){const U code=keys.d[i]-CODE_BIAS;U j=i+1;while(j<take&&keys.d[j]==keys.d[i])++j;int64_t acc=0;U live=0;
   if(!pop_nullable){int64_t a0=0,a1=0,a2=0,a3=0;U t=i;for(;t+3<j;t+=4){a0+=int64_t(values.d[t]);a1+=int64_t(values.d[t+1]);a2+=int64_t(values.d[t+2]);a3+=int64_t(values.d[t+3]);}for(;t<j;++t)a0+=int64_t(values.d[t]);live=j-i;acc=a0+a1+a2+a3-int64_t(CODE_BIAS)*(j-i);}
   else for(U t=i;t<j;++t)if(valid3(table.valid[pop],off+t)){acc+=int64_t(values.d[t])-int64_t(CODE_BIAS);++live;}
   auto&g=groups[code];g.present=true;g.sum+=SqlWide(acc);if(live)g.has_value=true;i=j;}});});
 timeit("direct_uniform",[&]{return base([&](auto&groups,auto&keys,auto&values,U off,U take){
  U first=keys.d[0];bool uniform=!st_nullable;if(uniform)for(U i=1;i<take;++i)if(keys.d[i]!=first){uniform=false;break;}
  if(uniform){const U code=first-CODE_BIAS;int64_t acc=0;U live=0;if(!pop_nullable){for(U i=0;i<take;++i)acc+=int64_t(values.d[i])-int64_t(CODE_BIAS);live=take;}
   else for(U i=0;i<take;++i)if(valid3(table.valid[pop],off+i)){acc+=int64_t(values.d[i])-int64_t(CODE_BIAS);++live;}
   auto&g=groups[code];g.present=true;g.sum+=SqlWide(acc);if(live)g.has_value=true;return;}
  for(U i=0;i<take;++i){const U row=off+i;if(st_nullable&&!valid3(table.valid[st],row))continue;auto&g=groups[keys.d[i]-CODE_BIAS];g.present=true;
   if(!pop_nullable||valid3(table.valid[pop],row)){g.sum+=SqlWide(int64_t(values.d[i])-int64_t(CODE_BIAS));g.has_value=true;}}});});
 std::string out="{\"rows\":"+std::to_string(n)+",\"iters\":"+std::to_string(iters)+",\"block_uniformity\":\"runtime-detect\",\"variants\":[";
 for(size_t i=0;i<rows.size();++i){if(i)out+=',';out+="{\"name\":\""+std::string(rows[i].name)+"\",\"total_ms\":"+std::to_string(rows[i].ms)+",\"ms_per_iter\":"+std::to_string(rows[i].ms/iters)+",\"ns_per_row\":"+std::to_string(rows[i].ms*1000000.0/iters/n)+",\"check\":"+std::to_string(rows[i].check)+"}";}
 out+="]}\n";std::cout<<out;return 0;
}
static int sql_entry(int argc,char**argv){
 need(argc>=2,"fullsql mode");std::string mode=argv[1];
 if(mode=="selftest_fullsql"){need(argc==2,"selftest arity");return sql_selftest();}
 if(mode=="bench_kernel"){need(argc==5,"bench_kernel encoded prepared iters");return sql_bench(argv[2],argv[3],U(std::stoul(argv[4])));}
 need(mode=="build_fullsql"||mode=="verify_fullsql"||mode=="query_fullsql","fullsql command");
 patch_fusion_enabled=true;physical_fill_mode=3;
 need(compare_mask_available(),"SSE2 availability before kernel use");compare_mask_enabled=true;
 if(mode=="build_fullsql"||mode=="verify_fullsql"){
  need(argc==4,"fullsql build/verify arity");bool build_mode=mode=="build_fullsql";std::string dir=build_mode?argv[2]:argv[3],path=build_mode?argv[3]:argv[2];
  auto start=Clock::now();auto binding=binding3(dir+"/binding.txt");auto dict=sql_dictionary(dir,binding);
  if(sql_empty(dir)){need(dict.empty(),"empty dictionary");sql_empty_encoded(path,binding,build_mode);}
  else{Table3 table=build_mode?construct3(dir):load3(path,binding);sql_schema(table);
   if(build_mode){save3(path,table);table=load3(path,binding);}verify3(table,dir);
   U state=bank_column(table,STATE_ID);auto values=cache_column<U>(*table.cs[state].root,table.meta[state].n);
   for(U i=0;i<values.size();++i)if(valid3(table.valid[state],i))sql_code(values[i],dict.size());}
  std::cout<<"{\"fullsql_build_or_verify_ms\":"<<ms(start)<<",\"dictionary_count\":"<<dict.size()<<",\"empty_relation\":"<<(sql_empty(dir)?"true":"false")<<"}\n";return 0;
 }
 need(argc==9,"query_fullsql encoded prepared SQL method result bitmap-or-dash metrics");
 const std::string encoded=argv[2],dir=argv[3],method=argv[5];
 need(sql_file(argv[4])==IG_SQL,"only exact pinned IG/2 SQL admitted");
 need(method=="generic"||method=="expert"||method=="scan_fused"||method=="group_then_filter"||method=="cache_fused"||method=="packed_code_fused"||method=="direct"||method=="direct_plain"||method=="direct_run"||method=="stream"||method=="stream_uniform","fullsql method");
 auto setup=Clock::now();std::string binding=binding3(dir+"/binding.txt");auto dict=sql_dictionary(dir,binding);bool empty=sql_empty(dir),diagnostic=std::string(argv[7])!="-";
 Table3 table;Catalog views;U n=0,st=0,pop=0;std::vector<U>plain_state,plain_pop;StreamTable streaming;const StreamColumn*sst=nullptr,*spop=nullptr;
 if(empty){need(dict.empty(),"empty dictionary");sql_empty_encoded(encoded,binding,false);}
 else if(method=="packed_code_fused"){
  table.binding=binding;table.meta=metadata3(dir+"/columns.tsv");need(table.meta.size()==2,"packed projected cardinality");
  st=bank_column(table,STATE_ID);pop=bank_column(table,POP_ID);n=table.meta[st].n;
  need(table.meta[st].width==32&&table.meta[st].family==5&&table.meta[pop].width==32&&table.meta[pop].family==2,"packed projected schema");
  for(const auto&m:table.meta){auto valid=validity3(dir+"/col"+std::to_string(m.id)+".valid.bitmap",n);
   if(!m.nullable)for(U i=0;i<n;++i)need(valid3(valid,i),"packed NOT NULL validity");table.valid.push_back(std::move(valid));}
  plain_state=raw(dir,STATE_ID);plain_pop=raw(dir,POP_ID);need(plain_state.size()==n&&plain_pop.size()==n,"packed lengths");
  for(U i=0;i<n;++i){need(valid3(table.valid[st],i)||plain_state[i]==0,"packed NULL code placeholder");need(valid3(table.valid[pop],i)||plain_pop[i]==0,"packed NULL population placeholder");}
 }else if(method=="stream"||method=="stream_uniform"){
  bool parsed=true;streaming=stream_load(encoded,binding,parsed);
  need(parsed,"streaming parser admitted the frozen encoded structure");
  need(!streaming.empty&&streaming.columns.size()==2,"stream projected cardinality");
  for(auto&c:streaming.columns){need(c.width==32&&(c.id==STATE_ID||c.id==POP_ID),"stream projected ids/width");
   if(c.id==STATE_ID){need(c.family==5,"stream state family");sst=&c;}else{need(c.family==2,"stream payload family");spop=&c;}}
  need(sst&&spop&&sst->n==spop->n&&sst->n>0,"stream projected common rows");n=sst->n;
 }else{table=load3(encoded,binding);sql_schema(table);st=bank_column(table,STATE_ID);pop=bank_column(table,POP_ID);n=table.meta[st].n;if(method=="generic"||method=="expert")views=make_views(table.cs);}
 double setup_ms=ms(setup);auto start=Clock::now();auto query=sql_not_in(dict,std::string("District of Columbia"));std::vector<Query3>queries{query};
 std::vector<SqlGroup>groups(dict.size());B selected,diag;BankPlan plan;BankCounters counters;uint64_t decoded_blocks=0;double plan_ms=0,decode_once_ms=0;
 if(diagnostic)diag.assign((size_t(n)+63)/64,0);
 auto accepted=[&](U row,U biased){if(!valid3(table.valid[st],row))return false;U code=sql_code(biased,dict.size());return query.constant>=0?bool(query.constant):query.query.p.test(CODE_BIAS+code);};
 auto consume=[&](U row,U biased,U payload,bool diagnostic_selected=true){U code=sql_code(biased,dict.size());sql_add(groups,code,valid3(table.valid[pop],row),payload);if(diagnostic&&diagnostic_selected)bit(diag,row,true);};
 if(n&&(method=="generic"||method=="expert")){
  auto pt=Clock::now();bank_validate_queries(table,queries);plan=bank_compile(table,queries,views,method);plan_ms=ms(pt);
  selected.assign((size_t(n)+63)/64,0);bank_execute(plan,views,selected,selected.size(),true,counters);bank_constants_validity(table,queries,selected,selected.size());
  Scratch keys,values;for(U off=0;off<n;off+=V){U take=std::min(V,n-off);bool any=false;for(U i=0;i<take;++i)any=any||valid3(selected,off+i);if(!any)continue;
   decode_bulk(*table.cs[st].root->children[off/V],keys);decode_bulk(*table.cs[pop].root->children[off/V],values);decoded_blocks+=2;
   for(U i=0;i<take;++i)if(valid3(selected,off+i))consume(off+i,keys.d[i],values.d[i]);}
 }else if(n&&(method=="scan_fused"||method=="group_then_filter")){
  Scratch keys,values;for(U off=0;off<n;off+=V){U take=std::min(V,n-off);decode_bulk(*table.cs[st].root->children[off/V],keys);decode_bulk(*table.cs[pop].root->children[off/V],values);decoded_blocks+=2;
   if(method=="group_then_filter"){
    for(U i=0;i<take;++i){U row=off+i;if(valid3(table.valid[st],row))consume(row,keys.d[i],values.d[i],diagnostic&&accepted(row,keys.d[i]));}
   }else for(U i=0;i<take;++i)if(accepted(off+i,keys.d[i]))consume(off+i,keys.d[i],values.d[i]);}
 }else if(n&&(method=="stream"||method=="stream_uniform")){
  // Fused reader: the encoded tree is parsed into flat leaf descriptors once and
  // never materialized as a Node/Table3 graph; each block decodes directly into
  // the run/block-uniform aggregation kernel.
  const bool st_nullable=sst->nullable!=0,pop_nullable=spop->nullable!=0;const U k=U(dict.size());
  Scratch keys,values;
  for(U b=0;b<sst->leaves.size();++b){const U off=b*V;const U take=std::min(V,n-off);
   stream_decode_block(*sst,b,keys);stream_decode_block(*spop,b,values);decoded_blocks+=2;
   if(method=="stream"&&!st_nullable){
    U first=keys.d[0];bool uniform=true;for(U i=1;i<take;++i)if(keys.d[i]!=first){uniform=false;break;}
    if(uniform){const U code=first-CODE_BIAS;need(code<k,"aggregate key domain");int64_t acc=0;U live=0;
     if(!pop_nullable){int64_t a0=0,a1=0,a2=0,a3=0;U t=0;for(;t+3<take;t+=4){a0+=int64_t(values.d[t]);a1+=int64_t(values.d[t+1]);a2+=int64_t(values.d[t+2]);a3+=int64_t(values.d[t+3]);}for(;t<take;++t)a0+=int64_t(values.d[t]);live=take;acc=a0+a1+a2+a3-int64_t(CODE_BIAS)*(take);}
     else for(U i=0;i<take;++i)if(stream_valid_bit(spop->valid,off+i)){acc+=int64_t(values.d[i])-int64_t(CODE_BIAS);++live;}
     auto&g=groups[code];g.present=true;g.sum+=SqlWide(acc);if(live)g.has_value=true;continue;}
   }
   U i=0;
   if(!st_nullable){while(i<take){const U code=keys.d[i]-CODE_BIAS;need(code<k,"aggregate key domain");U j=i+1;while(j<take&&keys.d[j]==keys.d[i])++j;
    int64_t acc=0;U live=0;
    if(!pop_nullable){int64_t a0=0,a1=0,a2=0,a3=0;U t=i;for(;t+3<j;t+=4){a0+=int64_t(values.d[t]);a1+=int64_t(values.d[t+1]);a2+=int64_t(values.d[t+2]);a3+=int64_t(values.d[t+3]);}for(;t<j;++t)a0+=int64_t(values.d[t]);live=j-i;acc=a0+a1+a2+a3-int64_t(CODE_BIAS)*(j-i);}
    else for(U t=i;t<j;++t)if(stream_valid_bit(spop->valid,off+t)){acc+=int64_t(values.d[t])-int64_t(CODE_BIAS);++live;}
    auto&g=groups[code];g.present=true;g.sum+=SqlWide(acc);if(live)g.has_value=true;i=j;}
   }else for(U t=0;t<take;++t){const U row=off+t;if(!stream_valid_bit(sst->valid,row))continue;const U code=keys.d[t]-CODE_BIAS;need(code<k,"aggregate key domain");auto&g=groups[code];g.present=true;
    if(!pop_nullable||stream_valid_bit(spop->valid,row)){g.sum+=SqlWide(int64_t(values.d[t])-int64_t(CODE_BIAS));g.has_value=true;}}
  }
 }else if(n&&method=="direct_run"){
  // Run-splitting specialization: a contiguous equal-key run needs one scalar
  // sum, not a per-row group lookup. Runs are detected from decoded keys and are
  // independent of whether the stored relation is sorted.
  const bool st_nullable=table.meta[st].nullable, pop_nullable=table.meta[pop].nullable;
  const U k=U(dict.size());Scratch keys,values;
  for(U off=0;off<n;off+=V){const U take=std::min(V,n-off);decode_bulk(*table.cs[st].root->children[off/V],keys);decode_bulk(*table.cs[pop].root->children[off/V],values);decoded_blocks+=2;
   if(st_nullable){for(U i=0;i<take;++i){const U row=off+i;if(!valid3(table.valid[st],row))continue;const U code=keys.d[i]-CODE_BIAS;need(code<k,"aggregate key domain");auto&g=groups[code];g.present=true;
    if(!pop_nullable||valid3(table.valid[pop],row)){g.sum+=SqlWide(int64_t(values.d[i])-int64_t(CODE_BIAS));g.has_value=true;}}continue;}
   U i=0;while(i<take){const U code=keys.d[i]-CODE_BIAS;need(code<k,"aggregate key domain");U j=i+1;while(j<take&&keys.d[j]==keys.d[i])++j;
    int64_t acc=0;U live=0;
    if(!pop_nullable){int64_t a0=0,a1=0,a2=0,a3=0;U t=i;for(;t+3<j;t+=4){a0+=int64_t(values.d[t]);a1+=int64_t(values.d[t+1]);a2+=int64_t(values.d[t+2]);a3+=int64_t(values.d[t+3]);}for(;t<j;++t)a0+=int64_t(values.d[t]);live=j-i;acc=a0+a1+a2+a3-int64_t(CODE_BIAS)*(j-i);}
    else for(U t=i;t<j;++t)if(valid3(table.valid[pop],off+t)){acc+=int64_t(values.d[t])-int64_t(CODE_BIAS);++live;}
    auto&g=groups[code];g.present=true;g.sum+=SqlWide(acc);if(live)g.has_value=true;i=j;}
  }
 }else if(n&&(method=="direct"||method=="direct_plain")){
  // Structural specialization: block-uniform group keys take a scalar-sum fast
  // path that needs no per-row group lookup; all other blocks take the tight
  // per-row path. Nullability is fixed from the encoded schema, never guessed
  // from values, and both paths use the same exact signed128 accumulator.
  const bool st_nullable=table.meta[st].nullable, pop_nullable=table.meta[pop].nullable;
  const U k=U(dict.size());Scratch keys,values;
  for(U off=0;off<n;off+=V){const U take=std::min(V,n-off);decode_bulk(*table.cs[st].root->children[off/V],keys);decode_bulk(*table.cs[pop].root->children[off/V],values);decoded_blocks+=2;
   U first=keys.d[0];bool uniform=true;if(method=="direct"&&!st_nullable){for(U i=1;i<take;++i)if(keys.d[i]!=first){uniform=false;break;}}
   if(method=="direct"&&uniform&&!st_nullable){
    const U code=first-CODE_BIAS;need(code<k,"aggregate key domain");int64_t acc=0;U live=0;
    if(!pop_nullable){for(U i=0;i<take;++i)acc+=int64_t(values.d[i])-int64_t(CODE_BIAS);live=take;}
    else for(U i=0;i<take;++i)if(valid3(table.valid[pop],off+i)){acc+=int64_t(values.d[i])-int64_t(CODE_BIAS);++live;}
    auto&g=groups[code];g.present=true;g.sum+=SqlWide(acc);if(live)g.has_value=true;
   }else{
    for(U i=0;i<take;++i){const U row=off+i;if(st_nullable&&!valid3(table.valid[st],row))continue;
     const U code=keys.d[i]-CODE_BIAS;need(code<k,"aggregate key domain");auto&g=groups[code];g.present=true;
     if(!pop_nullable||valid3(table.valid[pop],row)){g.sum+=SqlWide(int64_t(values.d[i])-int64_t(CODE_BIAS));g.has_value=true;}}
   }
  }
 }else if(n){
  if(method=="cache_fused"){auto ct=Clock::now();plain_state=cache_column<U>(*table.cs[st].root,n);plain_pop=cache_column<U>(*table.cs[pop].root,n);decode_once_ms=ms(ct);decoded_blocks=2*((n+V-1)/V);}
  for(U i=0;i<n;++i)if(accepted(i,plain_state[i]))consume(i,plain_state[i],plain_pop[i]);
 }
 // For this exact query the predicate is constant on each GROUP BY key.
 // No per-row predicate is required by this authored analytic control in its
 // primary (non-diagnostic) path. Even excluded groups satisfy the INT32*n bound.
 if(method=="group_then_filter"||method=="direct"||method=="direct_plain"||method=="direct_run"||method=="stream"||method=="stream_uniform"){
  if(query.constant==0)for(auto&g:groups)g.present=false;
  else if(query.constant<0)groups[sql_code(query.query.p.value,dict.size())].present=false;
 }
 const auto result=sql_result(dict,groups);double query_ms=ms(start);auto rss=resident_kib();auto write_start=Clock::now();
 std::ofstream output(argv[6],std::ios::binary);output.write(result.data(),result.size());output.flush();need(bool(output),"fullsql result write");
 if(diagnostic){std::ofstream bitmap(argv[7],std::ios::binary);bitmap.write(reinterpret_cast<const char*>(diag.data()),diag.size()*8);bitmap.flush();need(bool(bitmap),"diagnostic bitmap write");}
 double output_write_ms=ms(write_start);std::ofstream metrics(argv[8]);need(bool(metrics),"fullsql metrics");
 metrics<<"{\"method\":"<<sql_json(method)<<",\"setup_ms\":"<<setup_ms<<",\"query_ms\":"<<query_ms<<",\"plan_ms\":"<<plan_ms<<",\"cold_decode_once_ms\":"<<decode_once_ms<<",\"output_write_ms\":"<<output_write_ms
  <<",\"rows\":"<<n<<",\"result_bytes\":"<<result.size()<<",\"selection_bitmap_bytes\":"<<selected.size()*8<<",\"diagnostic_bitmap_bytes\":"<<diag.size()*8<<",\"diagnostic_enabled\":"<<(diagnostic?"true":"false")
  <<",\"plan_owned_capacity_bytes\":"<<bank_plan_bytes(plan)<<",\"aggregate_capacity_bytes\":"<<groups.capacity()*sizeof(SqlGroup)<<",\"code_cache_capacity_bytes\":"<<(plain_state.capacity()+plain_pop.capacity())*sizeof(U)
  <<",\"decoder_scratch_nominal_bytes\":"<<(n?(method=="packed_code_fused"?0:(method=="cache_fused"?sizeof(Scratch):2*sizeof(Scratch))):0)
  <<",\"payload_and_key_decode_blocks\":"<<decoded_blocks<<",\"predicate_decode_blocks\":"<<counters.decoded<<",\"whole_process_rss_kib\":"<<rss<<",\"compressed_tree_retained\":"<<((method=="packed_code_fused"||empty)?"false":"true")
  <<",\"group_key_filter_after_aggregate\":"<<(method=="group_then_filter"?"true":"false")
  <<",\"full_result_serialization_charged\":true,\"sum_type\":\"HUGEINT\",\"runtime_sse2\":true,\"empty_relation\":"<<(empty?"true":"false")<<"}\n";metrics.flush();need(bool(metrics),"fullsql metrics flush");return 0;
}
int main(int argc,char**argv){try{return sql_entry(argc,argv);}catch(const std::exception&e){std::cerr<<"REJECT: "<<e.what()<<'\n';return 2;}}
