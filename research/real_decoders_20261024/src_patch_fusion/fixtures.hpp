// Prospective mathematical tests only. No production values or timing outcomes.
// Existing bank helpers are included first by adapter.cpp. This independent
// oracle never calls a decoder, Pred::test, uniform, a planner, or a word writer.
struct PatchFixtureTotals {uint64_t banks=0,plans=0,mode_checks=0,exact_checks=0,scenarios=0;};
static bool patch_fixture_scalar(U value,const Pred&p){
 switch(p.op){case EQ:return value==p.value;case NE:return value!=p.value;
 case LT:return value<p.value;case LE:return value<=p.value;case GT:return value>p.value;case GE:return value>=p.value;
 case LUT:return value<p.lut.size()&&p.lut[value]!=0;}throw std::runtime_error("patch fixture operator");
}
static B patch_fixture_validity(U n,bool all_null=false){
 B valid((size_t(n)+63)/64,0);for(U i=0;i<n;++i)if(!all_null&&i%11!=5)valid[i/64]|=uint64_t(1)<<(i%64);return valid;
}
static B patch_fixture_oracle(const Table3&t,const std::vector<Query3>&qs,const std::vector<U>&original){
 need(t.meta.size()==1&&t.valid.size()==1&&original.size()==t.meta[0].n,"patch fixture oracle dimensions");
 U n=t.meta[0].n;size_t stride=(size_t(n)+63)/64;B out(stride*qs.size(),0);
 for(size_t qid=0;qid<qs.size();++qid){const auto&q=qs[qid];need(q.query.id==qid&&q.query.col==0,"patch fixture oracle qids");
  for(U i=0;i<n;++i){const bool live=(t.valid[0][i/64]>>(i%64))&1;
   const bool yes=q.constant<0?patch_fixture_scalar(original[i],q.query.p):q.constant==1;
   if(live&&yes)out[qid*stride+i/64]|=uint64_t(1)<<(i%64);}}
 return out;
}
static N patch_fixture_patch(N child,const std::vector<U>&positions,const std::vector<U>&values){
 need(positions.size()==values.size(),"patch fixture arrays");auto x=node(PATCH);
 x->pos=positions;x->vals=values;x->children.push_back(std::move(child));return x;
}
// Construct the declared original values row-by-row, separately from decoder
// construction. Listed later exceptions win; two calls express nested patches.
static std::vector<U>patch_fixture_original(const std::vector<U>&base,const std::vector<U>&positions,
                                           const std::vector<U>&values,U n){
 need(base.size()>=n&&positions.size()==values.size(),"patch fixture original shape");std::vector<U>out(n);
 for(U i=0;i<n;++i){out[i]=base[i];for(size_t k=0;k<positions.size();++k)if(i==positions[k])out[i]=values[k];}return out;
}
static Table3 patch_fixture_table(N child,U n,U width,U codec,B valid){
 Table3 t;t.binding=std::string(40,'0');t.meta.push_back({0,width,1,codec==4?4U:5U,n});t.valid.push_back(std::move(valid));
 auto root=node(CHUNKS,n);root->children.push_back(std::move(child));t.cs.push_back({0,codec,std::move(root)});return t;
}
static std::vector<Query3>patch_fixture_queries(bool mixed){
 std::vector<Pred>preds={{EQ,70000,{}},{NE,70000,{}},{GE,0,{}},{LT,0,{}}};
 if(mixed)preds={{EQ,70000,{}},{EQ,7,{}},{NE,70000,{}},{LT,31,{}},
                 {LUT,0,{0,1,0,1}},{GE,0,{}},{NE,7,{}},{LT,0,{}}};
 std::vector<Query3>qs;for(auto&p:preds)qs.push_back({{U(qs.size()),0,std::move(p)},-1});return qs;
}
using PatchFixturePair=std::array<std::array<BankCounters,3>,2>;
static PatchFixturePair patch_fixture_pair(const Table3&t,const std::vector<Query3>&qs,
                                           const std::vector<U>&original,const std::string&kind,
                                           const std::vector<const Node*>&regions,PatchFixtureTotals&total){
 const auto expected=patch_fixture_oracle(t,qs,original);const auto views=make_views(t.cs);
 const size_t stride=(size_t(t.meta[0].n)+63)/64;PatchFixturePair result{};bank_validate_queries(t,qs);
 for(U mode:{0U,1U}){patch_fusion_enabled=mode!=0;auto plan=bank_compile(t,qs,views,kind);
  std::vector<const Node*>found;for(const auto&task:plan.tasks)if(task.kind==BANK_FUSED_PATCH){
   need(task.raw_pack&&task.raw_pack->kind==PACK&&task.raw_pack->n==V,"patch fixture raw PACK identity");found.push_back(task.x);}
  const std::vector<const Node*>wanted=mode&&kind!="scan"?regions:std::vector<const Node*>{};
  need(found==wanted,"patch fixture exact recognized regions and boundaries");++total.plans;
  const auto capacity=bank_plan_bytes(plan);
  for(U schedule:{0U,1U,2U}){B out(expected.size()+2,0);out[expected.size()]=0x1234567890abcdefULL;out.back()=0xfedcba0987654321ULL;
   auto&c=result[mode][schedule];bank_execute(plan,views,out,stride,schedule==2,c,schedule==1);bank_constants_validity(t,qs,out,stride);
   need(std::equal(expected.begin(),expected.end(),out.begin()),"patch fixture independent full original-value bitmap");
   need(out[expected.size()]==0x1234567890abcdefULL&&out.back()==0xfedcba0987654321ULL,"patch fixture adjacent sentinels");
   need(bank_plan_bytes(plan)==capacity,"patch fixture immutable plan capacity");++total.banks;
  }
 }
 for(U schedule:{0U,1U,2U}){const auto&a=result[0][schedule];const auto&b=result[1][schedule];
  need(a.decoded==b.decoded&&a.compared==b.compared,"patch fixture exact old decode/comparison multiplicity");
  need(a.fused_compared==0&&a.fused_uniform==0,"patch fixture disabled fusion counters");
  if(kind=="scan")need(a.patch_bitmap_tests==b.patch_bitmap_tests&&a.patch_bitmap_writes==b.patch_bitmap_writes&&
   a.patch_scratch_assignments==b.patch_scratch_assignments&&b.fused_compared==0&&b.fused_uniform==0,
   "patch fixture unchanged scan control");++total.mode_checks;
 }
 return result;
}
// Exact counts below are supplied analytically from fixture construction. They
// never use uniform(), patch_entries(), or observed planner/decoder counts.
static void patch_fixture_exact(const PatchFixturePair&r,const std::string&kind,U uniforms,U nonuniforms,
                                 U physical,U logical,bool eligible,PatchFixtureTotals&total){
 for(U mode:{0U,1U})for(U schedule:{0U,1U,2U}){const auto&c=r[mode][schedule];const bool scan=kind=="scan";
  const uint64_t comparisons=scan?uniforms+nonuniforms:nonuniforms;
  const uint64_t decodes=schedule==2?uint64_t(comparisons!=0):comparisons;
  const bool fused=mode&&eligible&&!scan;
  const uint64_t bitmap=scan?0:uint64_t(fused?uniforms:uniforms+nonuniforms)*logical;
  const uint64_t assignments=(scan||fused)?decodes*physical:0;
  need(c.decoded==decodes&&c.compared==comparisons&&c.patch_bitmap_tests==bitmap&&c.patch_bitmap_writes==bitmap&&
       c.patch_scratch_assignments==assignments&&c.fused_compared==(fused?nonuniforms:0)&&c.fused_uniform==(fused?uniforms:0),
       "patch fixture analytic lazy decode/bitmap/scratch counts");++total.exact_checks;
 }
}
static int selftest_patch_fusion(){
 struct Restore {bool saved=patch_fusion_enabled;~Restore(){patch_fusion_enabled=saved;}}restore;
 PatchFixtureTotals total;
 const std::vector<U>pos={0,1,31,63,64,70,1023},values={UINT32_MAX,0,70000,63,5,UINT32_MAX,0};
 // Admitted single PATCH->PACK. Raw U16 values coexist with U32 exceptions;
 // column metadata is U32 so no truncation or source-type relaxation is hidden.
 for(U width:{16U,32U})for(U n:{1U,63U,64U,65U,1023U,1024U})for(bool all_null:{false,true})for(bool mixed:{false,true}){
  std::vector<U>base(V);for(U i=0;i<V;++i)base[i]=i%64;
  auto child=patch_fixture_patch(pack(base,width,0,6),pos,values);const Node*region=child.get();
  auto original=patch_fixture_original(base,pos,values,n);auto t=patch_fixture_table(std::move(child),n,32,2,patch_fixture_validity(n,all_null));
  const auto qs=patch_fixture_queries(mixed);U logical=0;for(U at:pos)if(at<n)++logical;
  for(const std::string kind:{"generic","expert","scan"}){
   auto r=patch_fixture_pair(t,qs,original,kind,{region},total);patch_fixture_exact(r,kind,4,mixed?4:0,7,logical,true,total);}
  ++total.scenarios;
 }
 // Nested overlaps: outer false/true overrides reverse inner decisions. Old
 // expert admission is not widened: these are generic composition fixtures.
 for(U width:{16U,32U})for(U n:{65U,1024U}){
  std::vector<U>base(V);for(U i=0;i<V;++i)base[i]=i%64;
  const std::vector<U>ip={0,1,63,64,70,1023},iv={70000,0,7,UINT32_MAX,9,0};
  const std::vector<U>op={0,1,2,64,1023},ov={0,70000,UINT32_MAX,1,70000};
  auto inner=patch_fixture_patch(pack(base,width,0,6),ip,iv);auto child=patch_fixture_patch(std::move(inner),op,ov);const Node*region=child.get();
  auto first=patch_fixture_original(base,ip,iv,V);auto original=patch_fixture_original(first,op,ov,n);
  auto t=patch_fixture_table(std::move(child),n,32,99,patch_fixture_validity(n));U logical=0;for(U at:ip)if(at<n)++logical;for(U at:op)if(at<n)++logical;
  auto r=patch_fixture_pair(t,patch_fixture_queries(true),original,"generic",{region},total);
  patch_fixture_exact(r,"generic",4,4,11,logical,true,total);++total.scenarios;
 }
 // Empty-only wrappers are not counted as fusion opportunities, even nested.
 for(U depth:{1U,2U}){U n=65;std::vector<U>base(V);for(U i=0;i<V;++i)base[i]=i%64;N child=pack(base,32,0,6);
  for(U d=0;d<depth;++d)child=patch_fixture_patch(std::move(child),{},{});
  std::vector<U>original(base.begin(),base.begin()+n);auto t=patch_fixture_table(std::move(child),n,32,99,patch_fixture_validity(n));
  auto r=patch_fixture_pair(t,patch_fixture_queries(true),original,"generic",{},total);patch_fixture_exact(r,"generic",4,4,0,0,false,total);++total.scenarios;
 }
 // Exact node-identity checks prevent recognizing across a MAP. Mode 0 has
 // MAP outside a candidate; mode 1 also has an outer unfused PATCH; mode 2 has
 // PATCH->MAP->PACK and must not recognize any region at all.
 for(U n:{65U,1024U})for(U mode:{0U,1U,2U}){
  std::vector<U>codes(V);for(U i=0;i<V;++i)codes[i]=i%4;
  const std::vector<U>cp={0,1,63,64,70,1023},cv={2,3,0,1,2,3},dict={70000,0,UINT32_MAX,7};
  N child=pack(codes,16,0,2);std::vector<const Node*>regions;
  if(mode!=2){child=patch_fixture_patch(std::move(child),cp,cv);regions.push_back(child.get());}
  auto decoded_codes=mode!=2?patch_fixture_original(codes,cp,cv,V):codes;
  std::vector<U>mapped(V);for(U i=0;i<V;++i)mapped[i]=dict[decoded_codes[i]];
  auto map=node(MAP);map->vals=dict;map->children.push_back(std::move(child));child=std::move(map);
  std::vector<U>original(mapped.begin(),mapped.begin()+n);
  if(mode!=0){const std::vector<U>op={0,1,64,1023},ov={0,UINT32_MAX,70000,0};
   child=patch_fixture_patch(std::move(child),op,ov);original=patch_fixture_original(mapped,op,ov,n);}
  auto t=patch_fixture_table(std::move(child),n,32,99,patch_fixture_validity(n));
  patch_fixture_pair(t,patch_fixture_queries(true),original,"generic",regions,total);++total.scenarios;
 }
 // Structural fallbacks: PATCH->RUNS, PATCH->RLE-view and PATCH->non-RLE
 // prefix. Their original values are supplied directly, including tail patches.
 for(U n:{65U,1024U})for(U shape:{0U,1U,2U}){
  std::vector<U>base(V);N child;
  if(shape<2){for(U i=0;i<V;++i)base[i]=i<31?0:(i<64?7:70000);
   if(shape==0){child=node(RUNS);child->vals={0,7,70000};child->lens={31,33,V-64};}else child=rle(base);
  }else{std::array<uint16_t,V>indices{};const std::vector<U>dict={0,70000,7};
   for(U i=0;i<V;++i){indices[i]=uint16_t(i%3);base[i]=dict[indices[i]];}child=bank_fixture_prefix(indices,dict);}
  const std::vector<U>fp={0,31,64,1023},fv={70000,0,UINT32_MAX,7};child=patch_fixture_patch(std::move(child),fp,fv);
  auto original=patch_fixture_original(base,fp,fv,n);auto t=patch_fixture_table(std::move(child),n,32,99,patch_fixture_validity(n));
  auto views=make_views(t.cs);need(views.views.size()==(shape==1?1U:0U),"patch fixture view/prefix fallback distinction");
  patch_fixture_pair(t,patch_fixture_queries(true),original,"generic",{},total);++total.scenarios;
 }
 // Admitted PATCH->CONSTANT fallback retains expert and scan controls.
 for(U n:{65U,1024U}){std::vector<U>base(V,7);auto constant=node(CONSTANT);constant->base=7;
  const std::vector<U>fp={0,31,64,1023},fv={70000,0,UINT32_MAX,7};auto child=patch_fixture_patch(std::move(constant),fp,fv);
  auto original=patch_fixture_original(base,fp,fv,n);auto t=patch_fixture_table(std::move(child),n,32,4,patch_fixture_validity(n));
  U logical=0;for(U at:fp)if(at<n)++logical;auto qs=patch_fixture_queries(true);
  for(const std::string kind:{"generic","expert","scan"}){auto r=patch_fixture_pair(t,qs,original,kind,{},total);patch_fixture_exact(r,kind,8,0,4,logical,false,total);}++total.scenarios;
 }
 // Constant-only/NULL-literal lowering allocates no predicate domain or task.
 for(bool all_null:{false,true}){U n=65;std::vector<U>base(V);for(U i=0;i<V;++i)base[i]=i%64;
  auto child=patch_fixture_patch(pack(base,32,0,6),pos,values);auto original=patch_fixture_original(base,pos,values,n);
  auto t=patch_fixture_table(std::move(child),n,32,2,patch_fixture_validity(n,all_null));
  std::vector<Query3>qs={{{0,0,{EQ,0,{}}},0},{{1,0,{NE,0,{}}},1},{{2,0,{LT,0,{}}},0}};
  for(const std::string kind:{"generic","expert","scan"}){auto r=patch_fixture_pair(t,qs,original,kind,{},total);patch_fixture_exact(r,kind,0,0,0,0,false,total);}++total.scenarios;
 }
 // All six signed operators and outside-source-type folds/NULL literals, with
 // both source widths. The old typed lowering is tested against explicit signed
 // comparisons before the independent original-value bank oracle is used.
 for(U width:{16U,32U})for(bool all_null:{false,true}){U n=65;const U maximum=width==16?65535U:UINT32_MAX;
  std::vector<U>base(V);for(U i=0;i<V;++i)base[i]=i%64;const std::vector<U>sp={0,1,31,63,64,1023},sv={maximum,0,maximum,1,maximum,0};
  auto child=patch_fixture_patch(pack(base,32,0,6),sp,sv);const Node*region=child.get();auto original=patch_fixture_original(base,sp,sv,n);
  auto t=patch_fixture_table(std::move(child),n,width,2,patch_fixture_validity(n,all_null));auto qs=bank_fixture_queries(0,width);
  const int64_t lo=-(int64_t(1)<<(width-1)),hi=(int64_t(1)<<(width-1))-1;
  for(U op=0;op<=GE;++op)for(int64_t literal:{lo-1,lo,int64_t(-1),int64_t(0),int64_t(1),hi,hi+1}){
   auto q=lower_signed3(op,literal,width);for(U i=0;i<n;++i){const int64_t v=int64_t(original[i])+lo;bool expected=false;
    switch(Op(op)){case EQ:expected=v==literal;break;case NE:expected=v!=literal;break;case LT:expected=v<literal;break;
     case LE:expected=v<=literal;break;case GT:expected=v>literal;break;case GE:expected=v>=literal;break;default:throw std::runtime_error("patch signed fixture operator");}
    need((q.constant<0?patch_fixture_scalar(original[i],q.query.p):q.constant==1)==expected,"patch independent signed lowering");}}
  for(const std::string kind:{"generic","expert","scan"})patch_fixture_pair(t,qs,original,kind,{region},total);++total.scenarios;
 }
 // Source-derived expected coverage, not an observation or performance claim.
 need(total.scenarios==74&&total.banks==1116&&total.plans==372&&total.mode_checks==558&&total.exact_checks==972,
      "patch fixture prespecified complete coverage");
 std::cout<<"{\"patch_fusion_cases\":"<<total.banks<<",\"patch_fusion_scenarios\":"<<total.scenarios<<",\"patch_fusion_banks\":"<<total.banks
  <<",\"patch_fusion_plan_checks\":"<<total.plans<<",\"patch_fusion_mode_checks\":"<<total.mode_checks
  <<",\"patch_fusion_exact_counter_checks\":"<<total.exact_checks
  <<",\"new_malformed_descriptor_claim\":false,\"performance_claim\":false}\n";return 0;
}
