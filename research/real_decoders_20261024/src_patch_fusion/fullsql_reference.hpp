// Bank-aware leaf sharing. Every decoder/comparator below is the immutable
// admitted primitive or the same bounded output writer shared by all new arms.
#include "../src_bank_shared/decode_once_reference.hpp"

// Fixed structural region rule, never chosen from table/query identity or timing.
static bool patch_fusion_enabled=false;

static constexpr uint64_t BANK_PLAN_LIMIT=64ULL*1024*1024;
static constexpr uint64_t BANK_OUTPUT_LIMIT=512ULL*1024*1024;
enum BankTaskKind {BANK_LEAF,BANK_PATCH,BANK_RUNS,BANK_VIEW,BANK_SCAN_BLOCK,BANK_SCAN_RUNS,BANK_FUSED_PATCH};
struct BankDomain {std::vector<Pred> preds;std::vector<U> qids;};
struct BankTask {
 const Node*x=nullptr;const Node*dict=nullptr;const View*view=nullptr;
 U domain=0,off=0,take=0,run=0,skip=0;BankTaskKind kind=BANK_LEAF;
 const Node*raw_pack=nullptr;uint64_t patch_physical=0,patch_logical=0;
};
struct BankQueryRef {U column=0,pred=0;size_t begin=0,end=0;bool active=false;};
struct BankPlan {
 std::vector<BankDomain> domains;std::vector<BankTask> tasks;
 std::vector<BankQueryRef> queries;
};
struct BankCounters {
 uint64_t decoded=0,compared=0,scratch_capacity=0;
 uint64_t patch_bitmap_tests=0,patch_bitmap_writes=0,patch_scratch_assignments=0;
 uint64_t fused_compared=0,fused_uniform=0;
};
// Only a nonempty-exception unary PATCH chain ending in one physical PACK.
// MAP, RUNS, prefix and empty-only compatibility wrappers remain unfused.
static const Node* patch_region_pack(const Node&root){
 const Node*x=&root;bool any=false;
 if(root.kind!=PATCH||root.n!=V)return nullptr;
 while(x->kind==PATCH){
  if(x->n!=V||x->children.size()!=1||x->pos.size()!=x->vals.size())return nullptr;
  any=any||!x->pos.empty();x=x->children[0].get();
 }
 return any&&x->kind==PACK&&x->n==V?x:nullptr;
}
static uint64_t patch_entries(const Node&x,U take){
 uint64_t n=0;if(x.kind==PATCH)for(U pos:x.pos)if(pos<take)++n;
 for(const auto&child:x.children)n+=patch_entries(*child,take);
 return n;
}
// Used only on a recognized chain; ordered false as well as true overwrites.
static void patch_region_bitmap(const Node&x,const Pred&p,B&bank,U off,U take){
 if(x.kind==PACK)return;
 need(x.kind==PATCH&&x.children.size()==1,"fused patch chain shape");
 patch_region_bitmap(*x.children[0],p,bank,off,take);
 for(U j=0;j<x.pos.size();++j)if(x.pos[j]<take)bit(bank,off+x.pos[j],p.test(x.vals[j]));
}
static uint64_t bank_plan_bytes(const BankPlan&p){
 uint64_t bytes=sizeof(p)+p.domains.capacity()*sizeof(BankDomain)+
  p.tasks.capacity()*sizeof(BankTask)+p.queries.capacity()*sizeof(BankQueryRef);
 for(const auto&d:p.domains){bytes+=d.preds.capacity()*sizeof(Pred)+d.qids.capacity()*sizeof(U);
  for(const auto&q:d.preds)bytes+=q.lut.capacity()*sizeof(uint8_t);}
 return bytes;
}
static void bank_budget(const BankPlan&p,uint64_t additional=0){
 const uint64_t bytes=bank_plan_bytes(p);
 need(bytes<=BANK_PLAN_LIMIT&&additional<=BANK_PLAN_LIMIT-bytes,"bank plan budget");
}
static size_t bank_node_count(const Node&x){size_t n=1;for(const auto&c:x.children)n+=bank_node_count(*c);return n;}
static U bank_offset(U qid,size_t stride,U off){
 const uint64_t value=uint64_t(qid)*stride*64+off;
 need(value<=UINT32_MAX,"bank global bit offset");return U(value);
}
static size_t bank_size(U n,size_t queries){
 const size_t stride=(size_t(n)+63)/64;
 need(queries>0&&stride>0&&uint64_t(queries)<=BANK_OUTPUT_LIMIT/(stride*8),"bank output budget");
 need(uint64_t(queries)*stride*64<=UINT32_MAX,"bank writer uint32 domain");
 return stride*queries;
}
static void bank_task(BankPlan&p,const Node&x,U domain,U off,U take,BankTaskKind kind,
                      const View*view=nullptr,const Node*dict=nullptr,U run=0,U skip=0){
 need(domain<p.domains.size()&&take>0&&take<=x.n,"bank task bounds");
 need(p.tasks.size()<p.tasks.capacity(),"bank reserved task budget");
 BankTask task{&x,dict,view,domain,off,take,run,skip,kind};
 if(kind==BANK_FUSED_PATCH){task.raw_pack=patch_region_pack(x);need(task.raw_pack!=nullptr,"fused region admission");}
 if(kind==BANK_FUSED_PATCH||kind==BANK_SCAN_BLOCK){
  task.patch_physical=patch_entries(x,V);task.patch_logical=patch_entries(x,take);
 }else if(kind==BANK_PATCH){for(U pos:x.pos)if(pos<take)++task.patch_logical;}
 p.tasks.push_back(task);
}
static U bank_pull(BankPlan&p,const Node&map,U parent){
 need(map.kind==MAP&&map.children.size()==1&&parent<p.domains.size(),"bank MAP domain");
 const auto count=p.domains[parent].preds.size();
 need(map.vals.size()<=BANK_PLAN_LIMIT&&count<=BANK_PLAN_LIMIT/(sizeof(Pred)+sizeof(U)+map.vals.size()),
      "bank LUT budget multiplication");
 bank_budget(p,count*(sizeof(Pred)+sizeof(U)+map.vals.size()));
 BankDomain d;d.preds.reserve(count);d.qids=p.domains[parent].qids;
 for(const auto&q:p.domains[parent].preds)d.preds.push_back(pull_map(map,q));
 need(p.domains.size()<p.domains.capacity(),"bank reserved domain budget");
 U id=U(p.domains.size());p.domains.push_back(std::move(d));bank_budget(p);return id;
}
// This pass visits structure once, not once per query. A domain is reused by
// every CHUNKS child; MAP creates exactly one pulled bank at its own node.
static void bank_lower_rec(const Node&x,U domain,U off,U logical_n,BankPlan&p,const Catalog&views){
 need(off<logical_n,"bank lowering offset");U take=std::min(x.n,logical_n-off);
 if(const auto*v=find_view(views,x)){
  need(v->off==off&&v->take==take,"bank run-view interval");
  bank_task(p,x,domain,off,take,BANK_VIEW,v);return;
 }
 if(x.kind==MAP){auto next=bank_pull(p,x,domain);bank_lower_rec(*x.children[0],next,off,logical_n,p,views);return;}
 if(x.kind==CHUNKS){for(U j=0;j<x.children.size();++j){
   uint64_t begin=uint64_t(off)+uint64_t(j)*V;need(begin<logical_n,"bank CHUNKS coverage");
   bank_lower_rec(*x.children[j],domain,U(begin),logical_n,p,views);}return;}
 if(x.kind==PATCH){need(x.children.size()==1&&x.pos.size()==x.vals.size(),"bank PATCH shape");
  if(patch_fusion_enabled&&patch_region_pack(x)){bank_task(p,x,domain,off,take,BANK_FUSED_PATCH);return;}
  bank_lower_rec(*x.children[0],domain,off,logical_n,p,views);
  bank_task(p,x,domain,off,take,BANK_PATCH);return;
 }
 if(x.kind==RUNS){bank_task(p,x,domain,off,take,BANK_RUNS);return;}
 need(x.kind==PACK||x.kind==CONSTANT||prefix_shape(x),"bank supported leaf");
 need(take<=V,"bank decoded leaf bound");bank_task(p,x,domain,off,take,BANK_LEAF);
}
// Handwritten admitted-codec composition, with the identical domains, tasks,
// scratch decoder and output executor as generic lowering.
static void bank_lower_expert(const Col&c,U domain,BankPlan&p,const Catalog&views){
 const Node&root=*c.root;const U n=root.n;
 if(c.codec==0){bank_task(p,root,domain,0,n,BANK_RUNS);return;}
 if(c.codec==3){U pulled=bank_pull(p,root,domain);const Node&chunks=*root.children[0];
  for(U j=0;j<chunks.children.size();++j)bank_task(p,*chunks.children[j],pulled,j*V,std::min(V,n-j*V),BANK_LEAF);return;}
 for(U j=0;j<root.children.size();++j){const Node&x=*root.children[j];U off=j*V,take=std::min(V,n-off);
  if(c.codec==1){if(const auto*v=find_view(views,x))bank_task(p,x,domain,off,take,BANK_VIEW,v);
   else{U pulled=bank_pull(p,x,domain);bank_task(p,*x.children[0],pulled,off,take,BANK_LEAF);}}
  else{need(x.kind==PATCH&&x.children.size()==1,"bank expert PATCH shape");
   if(patch_fusion_enabled&&patch_region_pack(x))bank_task(p,x,domain,off,take,BANK_FUSED_PATCH);
   else{bank_task(p,*x.children[0],domain,off,take,BANK_LEAF);bank_task(p,x,domain,off,take,BANK_PATCH);}}
 }
}
static void bank_lower_scan(const Col&c,U domain,BankPlan&p){
 const Node&root=*c.root;const U n=root.n;
 if(root.kind==RUNS){
  U run=0,skip=0;for(U off=0;off<n;off+=V){U take=std::min(V,n-off);
   bank_task(p,root,domain,off,take,BANK_SCAN_RUNS,nullptr,nullptr,run,skip);
   U left=take;while(left){need(run<root.lens.size()&&skip<root.lens[run],"bank scan run cursor");
    U step=std::min(left,root.lens[run]-skip);left-=step;skip+=step;
    if(skip==root.lens[run]){++run;skip=0;}}
  }return;
 }
 const Node*dict=root.kind==MAP?&root:nullptr;const Node&chunks=dict?*root.children[0]:root;
 need(chunks.kind==CHUNKS,"bank scan chunk shape");
 for(U j=0;j<chunks.children.size();++j)
  bank_task(p,*chunks.children[j],domain,j*V,std::min(V,n-j*V),BANK_SCAN_BLOCK,nullptr,dict);
}
static BankPlan bank_compile(const Table3&table,const std::vector<Query3>&qs,const Catalog&views,const std::string&kind){
 BankPlan p;size_t nodes=0,blocks=0;
 for(const auto&c:table.cs){nodes+=bank_node_count(*c.root);blocks+=(size_t(c.root->n)+V-1)/V;}
 need(nodes+blocks+table.cs.size()<=BANK_PLAN_LIMIT/sizeof(BankTask),"bank structural plan budget");
 p.tasks.reserve(nodes+blocks);p.domains.reserve(nodes+table.cs.size());p.queries.resize(qs.size());bank_budget(p);
 for(U column=0;column<table.cs.size();++column){BankDomain d;size_t count=0;
  for(const auto&q:qs)if(q.query.col==table.cs[column].id&&q.constant<0)++count;
  bank_budget(p,count*(sizeof(Pred)+sizeof(U)));d.preds.reserve(count);d.qids.reserve(count);
  for(const auto&q:qs)if(q.query.col==table.cs[column].id){
   need(q.query.id<qs.size(),"bank query ID");auto&ref=p.queries[q.query.id];ref.column=column;
   if(q.constant<0){ref.active=true;ref.pred=U(d.preds.size());d.preds.push_back(q.query.p);d.qids.push_back(q.query.id);}}
  if(d.preds.empty())continue;
  U domain=U(p.domains.size());p.domains.push_back(std::move(d));bank_budget(p);size_t begin=p.tasks.size();
  if(kind=="generic")bank_lower_rec(*table.cs[column].root,domain,0,table.meta[column].n,p,views);
  else if(kind=="expert")bank_lower_expert(table.cs[column],domain,p,views);
  else{need(kind=="scan","bank plan kind");bank_lower_scan(table.cs[column],domain,p);}
  for(U qid:p.domains[domain].qids){p.queries[qid].begin=begin;p.queries[qid].end=p.tasks.size();}
 }
 bank_budget(p);return p;
}
struct BankLeaf {const uint16_t*a=nullptr;const U*d=nullptr;};
static BankLeaf bank_decode_leaf(const Node&x,Scratch&s){
 if(prefix_shape(x)){const Node&r=*x.children[0];const Node&pk=*r.children[0];uint16_t base=uint16_t(pk.base),dummy=0;
  unffor::unffor(pk.p16.empty()?&dummy:pk.p16.data(),s.a.data(),uint8_t(pk.bw),&base);
  generated::rsum::fallback::scalar::rsum(s.a.data(),s.b.data(),r.bases.data());
  generated::untranspose::fallback::scalar::untranspose_i(s.b.data(),s.c.data());return {s.c.data(),nullptr};}
 need(x.kind==PACK,"bank decode primitive leaf");
 if(x.width==16){uint16_t base=uint16_t(x.base),dummy=0;unffor::unffor(x.p16.empty()?&dummy:x.p16.data(),s.a.data(),uint8_t(x.bw),&base);return {s.a.data(),nullptr};}
 need(x.width==32,"bank decode width");U dummy=0;
 unffor::unffor(x.p32.empty()?&dummy:x.p32.data(),s.d.data(),uint8_t(x.bw),&x.base);return {nullptr,s.d.data()};
}
static BankLeaf bank_decode_scan(const BankTask&t,Scratch&s){
 if(t.kind==BANK_SCAN_RUNS){U run=t.run,skip=t.skip,at=0;
  while(at<t.take){need(run<t.x->lens.size()&&skip<t.x->lens[run],"bank scan run expansion");
   U take=std::min(t.take-at,t.x->lens[run]-skip);std::fill_n(s.d.begin()+at,take,t.x->vals[run]);at+=take;++run;skip=0;}
 }else{decode_bulk(*t.x,s);if(t.dict)for(auto&value:s.d){need(value<t.dict->vals.size(),"bank scan dictionary code");value=t.dict->vals[value];}}
 need(s.fallback.capacity()<=V,"bank scan bounded fallback");return {nullptr,s.d.data()};
}
template<Op op>static void bank_runs_op(const BankTask&t,const Catalog&views,const Pred&p,B&bank,U off){
 const Node&x=*t.x;U begin=0;const U runs=t.view?t.view->end-t.view->begin:U(x.vals.size());
 need(!t.view||runs<=x.vals.size(),"bank view values");
 for(U j=0;j<runs&&begin<t.take;++j){uint64_t raw_end=t.view?views.ends[t.view->begin+j]:uint64_t(begin)+x.lens.at(j);
  need(raw_end>begin&&raw_end<=x.n,"bank run endpoint");U end=std::min<U>(U(raw_end),t.take);
  emit_interval<true>(bank,off+begin,off+end,test_bulk<op>(x.vals[j],p));begin=end;}
 need(begin==t.take,"bank run complete logical coverage");
}
static void bank_runs(const BankTask&t,const Catalog&views,const Pred&p,B&bank,U off){
 switch(p.op){
#define BANK_RUN_CASE(OP) case OP:bank_runs_op<OP>(t,views,p,bank,off);break;
 BANK_RUN_CASE(EQ) BANK_RUN_CASE(NE) BANK_RUN_CASE(LT) BANK_RUN_CASE(LE)
 BANK_RUN_CASE(GT) BANK_RUN_CASE(GE) BANK_RUN_CASE(LUT)
#undef BANK_RUN_CASE
 }
}
static void bank_one(const BankTask&t,U i,const BankPlan&p,const Catalog&views,B&bank,size_t stride,
                     Scratch&s,BankCounters&c,BankLeaf&leaf,bool&ready,bool share){
 const auto&d=p.domains[t.domain];need(i<d.preds.size(),"bank predicate index");
 const Pred&pred=d.preds[i];const Node&x=*t.x;U off=bank_offset(d.qids[i],stride,t.off);
 if(t.kind==BANK_PATCH){
  for(U j=0;j<x.pos.size();++j)if(x.pos[j]<t.take)bit(bank,off+x.pos[j],pred.test(x.vals[j]));
  c.patch_bitmap_tests+=t.patch_logical;c.patch_bitmap_writes+=t.patch_logical;return;
 }
 if(t.kind==BANK_FUSED_PATCH){
  need(t.raw_pack!=nullptr,"fused raw PACK pointer");int same=uniform(*t.raw_pack,pred);
  if(same!=-1){fill_word(bank,off,off+t.take,same);patch_region_bitmap(x,pred,bank,off,t.take);
   c.patch_bitmap_tests+=t.patch_logical;c.patch_bitmap_writes+=t.patch_logical;++c.fused_uniform;return;}
 }
 if(t.kind==BANK_RUNS||t.kind==BANK_VIEW){bank_runs(t,views,pred,bank,off);return;}
 if(t.kind==BANK_LEAF){int same=x.kind==CONSTANT?int(pred.test(x.base)):(x.kind==PACK?uniform(x,pred):-1);
  if(same!=-1){fill_word(bank,off,off+t.take,same);return;}}
 if(!share||!ready){leaf=t.kind==BANK_LEAF?bank_decode_leaf(x,s):bank_decode_scan(t,s);ready=true;++c.decoded;
  c.patch_scratch_assignments+=t.patch_physical;
 }
 if(t.kind==BANK_FUSED_PATCH)++c.fused_compared;
 if(leaf.a)bulk_words(leaf.a,t.take,pred,bank,off);else{need(leaf.d!=nullptr,"bank decoded scratch");bulk_words(leaf.d,t.take,pred,bank,off);}
 ++c.compared;c.scratch_capacity=std::max<uint64_t>(c.scratch_capacity,sizeof(Scratch)+s.fallback.capacity()*sizeof(U));
}
static void bank_execute(const BankPlan&p,const Catalog&views,B&bank,size_t stride,bool share,BankCounters&c,bool task_major=false){
 Scratch scratch;c.scratch_capacity=sizeof(Scratch);
 if(share||task_major){for(const auto&t:p.tasks){BankLeaf leaf;bool ready=false;
   for(U i=0;i<p.domains[t.domain].preds.size();++i)bank_one(t,i,p,views,bank,stride,scratch,c,leaf,ready,share);}}
 else{for(const auto&q:p.queries)if(q.active){
   for(size_t j=q.begin;j<q.end;++j){BankLeaf leaf;bool ready=false;
    bank_one(p.tasks[j],q.pred,p,views,bank,stride,scratch,c,leaf,ready,false);}}}
}
static void bank_validate_queries(const Table3&table,const std::vector<Query3>&qs){
 need(!table.meta.empty(),"bank source metadata");bank_size(table.meta[0].n,qs.size());
 for(size_t id=0;id<qs.size();++id){const auto&q=qs[id];
  need(q.query.id==id,"bank original sequential query IDs");
  auto m=std::find_if(table.meta.begin(),table.meta.end(),[&](const Meta3&m){return m.id==q.query.col;});
  need(m!=table.meta.end(),"bank query column");
  need(q.constant==-1||q.constant==0||q.constant==1,"bank constant grammar");
  need(q.constant!=-1||m->width==32||q.query.p.value<=65535,"bank source-width literal");
 }
}
static U bank_column(const Table3&t,U id){auto it=std::find_if(t.meta.begin(),t.meta.end(),[&](const Meta3&m){return m.id==id;});need(it!=t.meta.end(),"bank column");return U(it-t.meta.begin());}
static void bank_constants_validity(const Table3&t,const std::vector<Query3>&qs,B&bank,size_t stride){
 for(const auto&q:qs){U column=bank_column(t,q.query.col);size_t off=size_t(q.query.id)*stride;
  if(q.constant>=0){if(q.constant)std::copy(t.valid[column].begin(),t.valid[column].end(),bank.begin()+off);}
  else for(size_t k=0;k<stride;++k)bank[off+k]&=t.valid[column][k];
 }
}
static void bank_packed_compare(const Table3&t,const std::vector<Query3>&qs,
 const std::map<U,std::vector<uint16_t>>&p16,const std::map<U,std::vector<U>>&p32,B&bank,size_t stride){
 for(const auto&q:qs)if(q.constant<0){U j=bank_column(t,q.query.col),off=bank_offset(q.query.id,stride,0);
  if(t.meta[j].width==16)bulk_words(p16.at(q.query.col).data(),t.meta[j].n,q.query.p,bank,off);
  else bulk_words(p32.at(q.query.col).data(),t.meta[j].n,q.query.p,bank,off);}
 bank_constants_validity(t,qs,bank,stride);
}

// The correctness fixture implementation is kept separate from the command and
// is frozen together with it; it is never invoked by a performance worker.
#include "../src_bank_shared/fixtures.hpp"
#include "fixtures.hpp"

static int bank_entry(int argc,char**argv){
 if(argc<6||(std::string(argv[5])!="query_bank"&&std::string(argv[5])!="selftest_bank_shared"&&std::string(argv[5])!="selftest_patch_fusion"))return decode_once_entry(argc,argv);
 const std::string command=argv[5];
 need((command=="query_bank"&&argc==12)||((command=="selftest_bank_shared"||command=="selftest_patch_fusion")&&argc==6),"bank command arity");
 need(std::string(argv[1])=="--bitmap-kernel"&&std::string(argv[3])=="--physical-fill","bank selector order");
 std::string kernel=argv[2],layout=argv[4];need(kernel=="scalar"||kernel=="sse2","bank comparison kernel");
 need(layout=="0"||layout=="3","bank primary layout");need(kernel!="sse2"||compare_mask_available(),"bank requested SSE2 unavailable");
 compare_mask_enabled=kernel=="sse2";physical_fill_mode=unsigned(layout[0]-'0');
 std::cout<<"{\"bitmap_kernel\":\""<<kernel<<"\",\"physical_fill_mode\":"<<physical_fill_mode<<",\"runtime_sse2\":"<<int(compare_mask_available())<<",\"patch_fusion_enabled\":"<<(patch_fusion_enabled?"true":"false")<<"}\n";
 if(command=="selftest_bank_shared")return selftest_bank_shared();
 if(command=="selftest_patch_fusion")return selftest_patch_fusion();
 const std::string method=argv[9],dir=argv[7];bool packed=method=="packed_direct",cache=method=="cache_direct";
 bool share=method=="generic_shared"||method=="expert_shared"||method=="scan_shared";
 bool block_repeat=method=="generic_block_repeat"||method=="expert_block_repeat"||method=="scan_block_repeat";
 std::string kind=method.rfind("generic_",0)==0?"generic":(method.rfind("expert_",0)==0?"expert":"scan");
 need(packed||cache||share||block_repeat||method=="generic_repeat"||method=="expert_repeat"||method=="scan_repeat","bank method");
 auto setup=Clock::now();auto qs=queries3(argv[8]);Table3 table;Catalog views;DecodeOnceCache decoded;
 double view_build_ms=0;std::map<U,std::vector<uint16_t>>p16;std::map<U,std::vector<U>>p32;
 if(packed){table.binding=binding3(dir+"/binding.txt");table.meta=metadata3(dir+"/columns.tsv");
  for(const auto&m:table.meta){std::string prefix=dir+"/col"+std::to_string(m.id);
   if(m.width==16){p16[m.id]=raw_block<uint16_t>(prefix+".biased.u16");need(p16[m.id].size()==m.n,"bank packed16 rows");}
   else{p32[m.id]=raw_block<U>(prefix+".biased.u32");need(p32[m.id].size()==m.n,"bank packed32 rows");}
   auto valid=validity3(prefix+".valid.bitmap",m.n);if(!m.nullable)for(U i=0;i<m.n;++i)need(valid3(valid,i),"bank packed NOT NULL");table.valid.push_back(std::move(valid));}}
 else{table=load3(argv[6],binding3(dir+"/binding.txt"));if(!cache&&kind!="scan"){auto tv=Clock::now();views=make_views(table.cs);view_build_ms=ms(tv);}}
 bank_validate_queries(table,qs);U n=table.meta[0].n;size_t stride=(size_t(n)+63)/64;double setup_ms=ms(setup);
 const auto loaded_rss=resident_kib();auto start=Clock::now();BankPlan plan;
 double plan_build_ms=0,decode_once_ms=0,comparison_ms=0;uint64_t cache_rss=0;
 if(!packed&&!cache){auto tp=Clock::now();plan=bank_compile(table,qs,views,kind);plan_build_ms=ms(tp);}
 if(cache){auto tc=Clock::now();decoded=materialize_cache(table);decode_once_ms=ms(tc);cache_rss=resident_kib();}
 auto execute=Clock::now();B bank(bank_size(n,qs.size()),0);BankCounters counters;
 if(packed)bank_packed_compare(table,qs,p16,p32,bank,stride);
 else if(cache)bank_packed_compare(table,qs,decoded.p16,decoded.p32,bank,stride);
 else{bank_execute(plan,views,bank,stride,share,counters,block_repeat);bank_constants_validity(table,qs,bank,stride);}
 comparison_ms=ms(execute);auto rss=resident_kib();double elapsed=ms(start);
 std::ofstream out(argv[10],std::ios::binary),metrics(argv[11]);need(bool(out)&&bool(metrics),"bank outputs");
 out.write(reinterpret_cast<const char*>(bank.data()),bank.size()*8);out.flush();need(bool(out),"bank full output write");
 metrics<<"{\"record\":\"setup\",\"setup_ms\":"<<setup_ms<<",\"loaded_rss_kib\":"<<loaded_rss
  <<",\"view_build_ms\":"<<view_build_ms<<",\"view_owned_capacity_bytes\":"<<sizeof(Catalog)+views.views.capacity()*sizeof(View)+views.ends.capacity()*sizeof(uint16_t)
  <<",\"original_values_read\":"<<(packed?"true":"false")<<"}\n";
 metrics<<"{\"record\":\"bank\",\"method\":\""<<method<<"\",\"bank_ms\":"<<elapsed<<",\"primary_bank_ms\":"<<elapsed
  <<",\"query_count\":"<<qs.size()<<",\"rows\":"<<n<<",\"output_bytes\":"<<bank.size()*8
  <<",\"plan_build_ms\":"<<plan_build_ms<<",\"execute_ms\":"<<comparison_ms
  <<",\"plan_owned_capacity_bytes\":"<<((packed||cache)?0:bank_plan_bytes(plan))
  <<",\"plan_budget_bytes\":"<<BANK_PLAN_LIMIT<<",\"decoded_scratch_capacity_bytes\":"<<(cache?"null":std::to_string(counters.scratch_capacity))
  <<",\"decoded_leaf_calls\":"<<(cache?"null":std::to_string(counters.decoded))
  <<",\"predicate_leaf_comparisons\":"<<(cache?"null":std::to_string(counters.compared))
  <<",\"decode_counters_instrumented\":"<<(cache?"false":"true")
  <<",\"patch_fusion_enabled\":"<<(patch_fusion_enabled?"true":"false")
  <<",\"patch_accounting_instrumented\":"<<(cache?"false":"true")
  <<",\"patch_bitmap_tests\":"<<(cache?"null":std::to_string(counters.patch_bitmap_tests))
  <<",\"patch_bitmap_writes\":"<<(cache?"null":std::to_string(counters.patch_bitmap_writes))
  <<",\"patch_scratch_assignments\":"<<(cache?"null":std::to_string(counters.patch_scratch_assignments))
  <<",\"fused_nonuniform_comparisons\":"<<(cache?"null":std::to_string(counters.fused_compared))
  <<",\"fused_uniform_fallbacks\":"<<(cache?"null":std::to_string(counters.fused_uniform))
  <<",\"fused_regions\":"<<std::count_if(plan.tasks.begin(),plan.tasks.end(),[](const BankTask&t){return t.kind==BANK_FUSED_PATCH;})
  <<",\"post_bank_rss_kib\":"<<rss<<",\"shared_leaf_schedule\":"<<(share?"true":"false")
  <<",\"schedule\":\""<<(share?"task_major_shared":(block_repeat?"task_major_repeat":((packed||cache)?"direct_control":"query_major_repeat")))<<"\""
  <<",\"compressed_tree_retained\":"<<(packed?"false":"true")
  <<",\"full_bank_allocation_validity_charged\":true,\"direct_original_order_bitmap_writes\":true";
 if(cache)metrics<<",\"decode_once_ms\":"<<decode_once_ms<<",\"comparison_ms\":"<<comparison_ms
  <<",\"cache_payload_bytes\":"<<decoded.payload_bytes<<",\"cache_payload_capacity_bytes\":"<<decoded.payload_capacity_bytes
  <<",\"post_cache_rss_kib\":"<<cache_rss<<",\"cold_decode_cache_allocation_charged\":true,\"cache_transient\":true";
 metrics<<"}\n";metrics.flush();need(bool(metrics),"bank metrics write");return 0;
}
static int frozen_patch_main(int argc,char**argv){try{
 need(argc>=3&&std::string(argv[1])=="--patch-fusion","patch fusion selector required");
 const std::string mode=argv[2];need(mode=="off"||mode=="on","patch fusion selector");patch_fusion_enabled=mode=="on";
 return bank_entry(argc-2,argv+2);
 }catch(const std::exception&e){std::cerr<<"REJECT: "<<e.what()<<'\n';return 2;}}
