// Mathematical fixtures, never production data and never a timing workload.
// Expected bitmaps are generated directly from supplied original biased values
// and validity, not by the decoder/plan being tested.
static std::vector<Query3> bank_fixture_queries(U col,U width){
 std::vector<Query3>qs;int64_t lo=-(int64_t(1)<<(width-1)),hi=(int64_t(1)<<(width-1))-1;
 for(U op=0;op<=GE;++op){for(int64_t literal:{lo-1,lo,int64_t(-1),int64_t(0),int64_t(1),hi,hi+1}){
   auto q=lower_signed3(op,literal,width);q.query.id=U(qs.size());q.query.col=col;qs.push_back(q);}
  auto q=lower_signed3(op,0,width,true);q.query.id=U(qs.size());q.query.col=col;qs.push_back(q);}
 auto duplicate=qs.at(3);duplicate.query.id=U(qs.size());qs.push_back(duplicate);return qs;
}
static Table3 bank_fixture_table(std::vector<U>values,U width,U family,B valid,U id=0){
 Table3 t;t.binding=std::string(40,'0');U n=U(values.size());
 t.meta.push_back({id,width,1,family,n});t.valid.push_back(std::move(valid));
 t.cs.push_back({id,family==5?2:family,build3(values,family)});return t;
}
static B bank_fixture_oracle(const Table3&t,const std::vector<Query3>&qs,const std::vector<std::vector<U>>&source){
 U n=t.meta[0].n;size_t stride=(size_t(n)+63)/64;B expected(bank_size(n,qs.size()),0);
 for(const auto&q:qs){U j=bank_column(t,q.query.col);need(source[j].size()==n,"fixture source rows");
  for(U row=0;row<n;++row){bool yes=q.constant<0?q.query.p.test(source[j][row]):q.constant!=0;
   if(valid3(t.valid[j],row)&&yes)expected[size_t(q.query.id)*stride+row/64]|=uint64_t(1)<<(row%64);}}
 return expected;
}
static void bank_fixture_check(const Table3&t,const std::vector<Query3>&qs,const std::vector<std::vector<U>>&source,
                               bool expert_scan_cache,uint64_t&cases){
 bank_validate_queries(t,qs);const auto expected=bank_fixture_oracle(t,qs,source);const auto views=make_views(t.cs);
 const size_t stride=(size_t(t.meta[0].n)+63)/64;
 for(const std::string kind:{"generic","expert","scan"}){
  if(kind!="generic"&&!expert_scan_cache)continue;
  auto plan=bank_compile(t,qs,views,kind);const uint64_t frozen_bytes=bank_plan_bytes(plan);
  for(U schedule:{0U,1U,2U}){B output(expected.size()+2,0);output[expected.size()]=0x1234567890abcdefULL;output.back()=0xfedcba0987654321ULL;
   BankCounters counters;bank_execute(plan,views,output,stride,schedule==2,counters,schedule==1);bank_constants_validity(t,qs,output,stride);
   need(std::equal(expected.begin(),expected.end(),output.begin()),"bank full-vector fixture");
   need(output[expected.size()]==0x1234567890abcdefULL&&output.back()==0xfedcba0987654321ULL,"bank tail adjacent sentinels");
   need(bank_plan_bytes(plan)==frozen_bytes,"bank immutable plan execution");++cases;}
 }
 if(expert_scan_cache){auto cache=materialize_cache(t);B output(expected.size(),0);
  bank_packed_compare(t,qs,cache.p16,cache.p32,output,stride);need(output==expected,"bank paid cache direct fixture");++cases;
  std::map<U,std::vector<uint16_t>>p16;std::map<U,std::vector<U>>p32;
  for(U j=0;j<t.meta.size();++j){if(t.meta[j].width==16){auto&v=p16[t.meta[j].id];for(U x:source[j])v.push_back(cached_value<uint16_t>(x));}else p32[t.meta[j].id]=source[j];}
  std::fill(output.begin(),output.end(),0);bank_packed_compare(t,qs,p16,p32,output,stride);
  need(output==expected,"bank packed direct fixture");++cases;
 }
}
static N bank_fixture_prefix(const std::array<uint16_t,V>&idx,const std::vector<U>&values){
 std::array<uint16_t,V>tr{},delta{};generated::transpose::fallback::scalar::transpose_i(idx.data(),tr.data());
 generated::unrsum::fallback::scalar::unrsum(tr.data(),delta.data());std::vector<U>d(delta.begin(),delta.end());
 auto mm=std::minmax_element(d.begin(),d.end());auto rs=node(RSUM);rs->bases.assign(tr.begin(),tr.begin()+64);
 rs->children.push_back(pack(d,16,*mm.first,bits(*mm.second-*mm.first)));auto ut=node(UNTRANSPOSE);ut->children.push_back(std::move(rs));
 auto mp=node(MAP);mp->vals=values;mp->children.push_back(std::move(ut));return mp;
}
static int selftest_bank_shared(){
 uint64_t cases=0,mechanism_cases=0,rejections=0;
 // Signed-domain extremes, NULLs, every operator, duplicate predicates, constants,
 // logical tails and all six admitted composition families.
 for(U width:{16U,32U})for(U n:{1U,63U,64U,65U,1023U,1031U})for(U family:{0U,1U,2U,3U,4U,5U}){
  int64_t lo=-(int64_t(1)<<(width-1)),hi=(int64_t(1)<<(width-1))-1;
  std::array<int64_t,7>values={lo,lo+1,-1,0,1,hi-1,hi};std::vector<U>a(n);B valid((n+63)/64,0);
  for(U i=0;i<n;++i){bool live=i%13!=0;bit(valid,i,live);a[i]=live?U(values[(i/11)%values.size()]-lo):0;}
  auto t=bank_fixture_table(a,width,family,valid);auto qs=bank_fixture_queries(0,width);
  // Independent signed relation checks the query type-bias lowering itself.
  for(U op=0;op<=GE;++op)for(int64_t literal:{lo-1,lo,int64_t(-1),int64_t(0),int64_t(1),hi,hi+1}){
   auto q=lower_signed3(op,literal,width);for(U i=0;i<n;++i)if(valid3(valid,i))
    need((q.constant<0?q.query.p.test(a[i]):q.constant!=0)==compare_signed3(int64_t(a[i])+lo,op,literal),"bank signed oracle relation");}
  bank_fixture_check(t,qs,{a},true,cases);
 }
 // Two interleaved columns, repeated destinations in query order, all-NULL one
 // column, and constants must not borrow another column's validity/domain.
 for(bool all_null:{false,true}){U n=65;std::vector<U>a(n),b(n);B va((n+63)/64,0),vb(va.size(),0);
  for(U i=0;i<n;++i){a[i]=i%7;b[i]=(i*3)%19;bit(va,i,i%5!=0);bit(vb,i,!all_null&&i%3!=0);}
  auto t=bank_fixture_table(a,16,2,va,7);t.meta.push_back({29,32,1,3,n});t.valid.push_back(vb);t.cs.push_back({29,3,build3(b,3)});
  std::vector<Query3>qs;for(U op=0;op<=GE;++op)for(U col:{29U,7U,29U})qs.push_back({{U(qs.size()),col,{Op(op),3,{}}},-1});
  for(U col:{7U,29U})for(int constant:{0,1})qs.push_back({{U(qs.size()),col,{EQ,0,{}}},constant});
  bank_fixture_check(t,qs,{a,b},true,cases);
 }
 // Constant-only banks: no domain execution, no leaf decode, exact validity.
 {U n=65;std::vector<U>a(n,7);B valid(2,~uint64_t(0));valid.back()=1;auto t=bank_fixture_table(a,16,5,valid);
  std::vector<Query3>qs={{{0,0,{EQ,0,{}}},0},{{1,0,{NE,0,{}}},1},{{2,0,{LT,0,{}}},0}};
  bank_fixture_check(t,qs,{a},true,cases);auto views=make_views(t.cs);auto p=bank_compile(t,qs,views,"generic");
  B out(bank_size(n,qs.size()),0);BankCounters c;bank_execute(p,views,out,2,true,c);
  need(p.tasks.empty()&&c.decoded==0&&c.compared==0,"bank constant-only zero decode");++mechanism_cases;
 }
 // A valid prefix MAP that is not an RLE view must use the pulled domain and
 // the same opaque prefix decoder. Values intentionally exceed code-domain size.
 for(U pattern:{0U,1U,2U}){U n=65;std::array<uint16_t,V>idx{};
  for(U i=0;i<V;++i)idx[i]=uint16_t(pattern==0?1:(pattern==1?i%2:(i<32?0:2)));
  std::vector<U>dict={7,11,13},a(n);for(U i=0;i<n;++i)a[i]=dict[idx[i]];B valid(2,~uint64_t(0));valid.back()=1;
  auto t=bank_fixture_table(a,16,1,valid);auto chunks=node(CHUNKS,n);chunks->children.push_back(bank_fixture_prefix(idx,dict));t.cs[0].root=std::move(chunks);
  need(make_views(t.cs).views.empty(),"bank non-RLE prefix refuses view");auto qs=bank_fixture_queries(0,16);
  qs.push_back({{U(qs.size()),0,{EQ,11,{}}},-1});bank_fixture_check(t,qs,{a},true,cases);
 }
 // Nested MAP and PATCH order. Inner/outer patches overlap; outer wins. These
 // are generic composition fixtures, not admitted expert/scan descriptor claims.
 for(U n:{65U,V}){std::vector<U>codes(V),source(n);for(U i=0;i<V;++i)codes[i]=i%3;
  auto leaf=pack(codes,32,0,2);auto inner=node(MAP);inner->vals={9,5,9};inner->children.push_back(std::move(leaf));
  auto ip=node(PATCH);ip->pos={1,5,70};ip->vals={5,9,5};ip->children.push_back(std::move(inner));
  auto outer=node(MAP);outer->vals={2,3,5,7,11,13,17,19,23,29};auto outer_values=outer->vals;outer->children.push_back(std::move(ip));
  auto patch=node(PATCH);patch->pos={1,2,70};patch->vals={11,0,17};patch->children.push_back(std::move(outer));
  for(U i=0;i<n;++i){U value=std::array<U,3>{9,5,9}[codes[i]];if(i==1||i==70)value=5;if(i==5)value=9;value=outer_values[value];
   if(i==1)value=11;if(i==2)value=0;if(i==70)value=17;source[i]=value;}
  auto chunks=node(CHUNKS,n);chunks->children.push_back(std::move(patch));B valid((n+63)/64,~uint64_t(0));if(n%64)valid.back()=(uint64_t(1)<<(n%64))-1;
  auto t=bank_fixture_table(source,32,5,valid);t.cs[0].root=std::move(chunks);auto qs=bank_fixture_queries(0,32);
  for(U op=0;op<=GE;++op)qs.push_back({{U(qs.size()),0,{Op(op),13,{}}},-1});
  qs.push_back({{U(qs.size()),0,{LUT,0,{1,0,1,1,0,1}}},-1});bank_fixture_check(t,qs,{source},false,cases);
 }
 // Clipped CHUNKS -> RUNS tails cannot overwrite the next qid's padded slice.
 for(U n:{1U,63U,64U,65U}){std::vector<U>a(n);for(U i=0;i<n;++i)a[i]=i<31?1:2;
  auto run=node(RUNS);run->vals={1,2,3};run->lens={31,60,V-91};auto chunks=node(CHUNKS,n);chunks->children.push_back(std::move(run));
  B valid((n+63)/64,~uint64_t(0));if(n%64)valid.back()=(uint64_t(1)<<(n%64))-1;
  auto t=bank_fixture_table(a,16,0,valid);t.cs[0].root=std::move(chunks);
  std::vector<Query3>qs;for(U op=0;op<=GE;++op)qs.push_back({{U(qs.size()),0,{Op(op),2,{}}},-1});
  bank_fixture_check(t,qs,{a},false,cases);
 }
 // A run view inside an outer MAP must compare its code-valued run values in
 // the already pulled outer domain. PATCH both inside/outside that MAP keeps
 // the corresponding domain and wins after the view, including shared rows.
 for(U n:{65U,V}){std::vector<U>codes(V);for(U i=0;i<V;++i)codes[i]=i<31?1:(i<64?2:0);
  auto inner_patch=node(PATCH);inner_patch->pos={1,31,64,70};inner_patch->vals={0,1,2,0};inner_patch->children.push_back(rle(codes));
  auto map=node(MAP);map->vals={7,11,19};map->children.push_back(std::move(inner_patch));
  auto outer_patch=node(PATCH);outer_patch->pos={1,32,64,70};outer_patch->vals={19,7,11,7};outer_patch->children.push_back(std::move(map));
  auto chunks=node(CHUNKS,n);chunks->children.push_back(std::move(outer_patch));std::vector<U>source(n);
  for(U i=0;i<n;++i){U code=codes[i];if(i==1||i==70)code=0;if(i==31)code=1;if(i==64)code=2;
   U value=std::array<U,3>{7,11,19}[code];if(i==1)value=19;if(i==32||i==70)value=7;if(i==64)value=11;source[i]=value;}
  B valid((n+63)/64,~uint64_t(0));if(n%64)valid.back()=(uint64_t(1)<<(n%64))-1;
  auto t=bank_fixture_table(source,16,5,valid);t.cs[0].root=std::move(chunks);need(make_views(t.cs).views.size()==1,"bank nested view qualification");
  std::vector<Query3>qs;for(U op=0;op<=GE;++op)for(U literal:{7U,11U,19U})qs.push_back({{U(qs.size()),0,{Op(op),literal,{}}},-1});
  Pred lut{LUT,0,std::vector<uint8_t>(12,0)};lut.lut[7]=1;qs.push_back({{U(qs.size()),0,lut},-1});
  bank_fixture_check(t,qs,{source},false,cases);
 }
 // Raw PACK values outside a predicate LUT remain false, including the implicit
 // complement of a pulled NE. This is predicate semantics, not malformed MAP admission.
 {U n=65;std::vector<U>a(n);for(U i=0;i<n;++i)a[i]=i%4==3?65535:i%4;B valid(2,~uint64_t(0));valid.back()=1;
  auto t=bank_fixture_table(a,16,5,valid);std::vector<Query3>qs={{{0,0,{LUT,0,{0,1}}},-1},{{1,0,{LUT,0,{1,0}}},-1}};
  bank_fixture_check(t,qs,{a},true,cases);
 }
 // Analytic decode multiplicity, independent of time: one materialization per
 // physical leaf vs one per nonuniform predicate, with identical comparisons.
 for(U n:{65U,1031U})for(U width:{16U,32U}){std::vector<U>a(n);for(U i=0;i<n;++i)a[i]=(i+40*(i/V))%64;
  auto chunks=node(CHUNKS,n);for(U off=0;off<n;off+=V){std::vector<U>block(V,0);std::copy_n(a.begin()+off,std::min(V,n-off),block.begin());chunks->children.push_back(pack(block,width,0,6));}
  B valid((n+63)/64,~uint64_t(0));if(n%64)valid.back()=(uint64_t(1)<<(n%64))-1;
  auto t=bank_fixture_table(a,width,5,valid);t.cs[0].root=std::move(chunks);
  std::vector<Query3>qs={{{0,0,{EQ,7,{}}},-1},{{1,0,{NE,7,{}}},-1},{{2,0,{LT,31,{}}},-1},{{3,0,{GE,31,{}}},-1}};
  bank_fixture_check(t,qs,{a},false,cases);auto views=make_views(t.cs);auto p=bank_compile(t,qs,views,"generic");
  B one(bank_size(n,qs.size()),0),repeat=one,block_repeat=one;BankCounters c1,c2,c3;size_t stride=(size_t(n)+63)/64;
  bank_execute(p,views,one,stride,true,c1);bank_execute(p,views,repeat,stride,false,c2);
  bank_execute(p,views,block_repeat,stride,false,c3,true);
  need(one==repeat&&one==block_repeat&&c1.decoded==(n+V-1)/V&&c2.decoded==4*c1.decoded&&c1.compared==c2.compared&&c1.compared==c2.decoded&&c3.decoded==c2.decoded&&c3.compared==c2.compared,"bank analytic leaf reuse graph");++mechanism_cases;
 }
 // Mixed uniform/nonuniform domains retain identical shortcuts and only decode
 // lazily for the predicates requiring values. The all-uniform bank decodes 0.
 for(U width:{16U,32U}){U n=65;std::vector<U>a(n);for(U i=0;i<n;++i)a[i]=i%64;
  std::vector<U>physical(V,0);std::copy(a.begin(),a.end(),physical.begin());auto chunks=node(CHUNKS,n);chunks->children.push_back(pack(physical,width,0,6));
  B valid(2,~uint64_t(0));valid.back()=1;auto t=bank_fixture_table(a,width,5,valid);t.cs[0].root=std::move(chunks);auto views=make_views(t.cs);
  std::vector<Query3>qs={{{0,0,{EQ,999,{}}},-1},{{1,0,{NE,999,{}}},-1}};
  for(bool mixed:{false,true}){if(mixed){qs.push_back({{2,0,{EQ,7,{}}},-1});qs.push_back({{3,0,{NE,7,{}}},-1});qs.push_back({{4,0,{LUT,0,{0,1}}},-1});}
   bank_fixture_check(t,qs,{a},false,cases);auto p=bank_compile(t,qs,views,"generic");
   for(U schedule:{0U,1U,2U}){B out(bank_size(n,qs.size()),0);BankCounters c;bank_execute(p,views,out,2,schedule==2,c,schedule==1);
    need(c.decoded==(mixed?(schedule==2?1U:3U):0U)&&c.compared==(mixed?3U:0U),"bank lazy uniform sharing graph");++mechanism_cases;}
  }
 }
 auto reject=[&](auto fn){bool caught=false;try{fn();}catch(const std::exception&){caught=true;}need(caught,"bank expected bounded rejection");++rejections;};
 reject([&](){bank_size(1000000,10000);});reject([&](){bank_offset(UINT32_MAX,1,64);});
 reject([&](){BankPlan p;bank_budget(p,BANK_PLAN_LIMIT+1);});
 need(bank_size(65,2)==4,"bank checked stride arithmetic");
 std::cout<<"{\"bank_sharing_cases\":"<<cases<<",\"analytic_reuse_cases\":"<<mechanism_cases
  <<",\"bounded_rejections\":"<<rejections<<",\"new_malformed_descriptor_claim\":false}\n";return 0;
}
