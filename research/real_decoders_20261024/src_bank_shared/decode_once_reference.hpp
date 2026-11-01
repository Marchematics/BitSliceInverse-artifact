// Decode-once lifecycle control. The included, separately hashed reference has
// exactly one entrypoint rename; every admitted decoder/comparator is unchanged.
#include "../src_decode_once/factorial_reference.hpp"

struct DecodeOnceCache {
 std::map<U,std::vector<uint16_t>> p16;
 std::map<U,std::vector<U>> p32;
 uint64_t payload_bytes=0, payload_capacity_bytes=0;
};

template<class T>static T cached_value(U value){
 need(uint64_t(value)<=uint64_t(std::numeric_limits<T>::max()),"cached source-width overflow");
 return T(value);
}

template<class T>static std::vector<T> cache_column(const Node&root,U n){
 need(root.n==n,"cached logical rows");
 std::vector<T> out(n);
 if(root.kind==RUNS){
  need(root.vals.size()==root.lens.size(),"cached run arrays");
  U off=0;
  for(size_t j=0;j<root.vals.size();++j){
   need(root.lens[j]>0&&root.lens[j]<=n-off,"cached run coverage");
   std::fill_n(out.begin()+off,root.lens[j],cached_value<T>(root.vals[j]));
   off+=root.lens[j];
  }
  need(off==n,"cached complete run coverage");return out;
 }
 const Node*chunks=&root;
 const std::vector<U>*dictionary=nullptr;
 if(root.kind==MAP){
  need(root.children.size()==1,"cached dictionary arity");
  dictionary=&root.vals;chunks=root.children[0].get();
 }
 need(chunks->kind==CHUNKS&&chunks->n==n&&chunks->children.size()==(size_t(n)+V-1)/V,
      "cached chunk shape");
 Scratch scratch;
 for(size_t j=0;j<chunks->children.size();++j){
  need(chunks->children[j]->n==V,"cached leaf rows");
  // One admitted vector decoder invocation per physical 1024-value block.
  // It applies PATCH after its child; dictionary pullback here is value decoding,
  // not predicate evaluation. Physical tails are decoded but never cached.
  decode_bulk(*chunks->children[j],scratch);
  if(dictionary)for(auto&value:scratch.d){
   need(value<dictionary->size(),"cached dictionary bound");value=(*dictionary)[value];
  }
  U off=U(j)*V,take=std::min(V,n-off);
  for(U k=0;k<take;++k)out[off+k]=cached_value<T>(scratch.d[k]);
 }
 return out;
}

static DecodeOnceCache materialize_cache(const Table3&table){
 need(table.meta.size()==table.cs.size()&&table.valid.size()==table.cs.size(),"cached column count");
 DecodeOnceCache cache;
 for(size_t j=0;j<table.meta.size();++j){
  const auto&m=table.meta[j];
  need(table.cs[j].id==m.id,"cached column identity");
  if(m.width==16){
   auto inserted=cache.p16.emplace(m.id,cache_column<uint16_t>(*table.cs[j].root,m.n));
   need(inserted.second,"cached duplicate column");
   cache.payload_capacity_bytes+=uint64_t(inserted.first->second.capacity())*sizeof(uint16_t);
  }else{
   need(m.width==32,"cached source width");
   auto inserted=cache.p32.emplace(m.id,cache_column<U>(*table.cs[j].root,m.n));
   need(inserted.second,"cached duplicate column");
   cache.payload_capacity_bytes+=uint64_t(inserted.first->second.capacity())*sizeof(U);
  }
  cache.payload_bytes+=uint64_t(m.n)*(m.width/8);
 }
 return cache;
}

static void verify_cached(const Table3&table,const DecodeOnceCache&cache,const std::string&dir){
 auto expected=metadata3(dir+"/columns.tsv");
 need(expected.size()==table.meta.size(),"cached verify column count");
 uint64_t values=0,live=0;
 for(size_t j=0;j<table.meta.size();++j){
  const auto&m=table.meta[j];const auto&e=expected[j];
  need(m.id==e.id&&m.width==e.width&&m.nullable==e.nullable&&m.family==e.family&&m.n==e.n,
       "cached source metadata");
  auto source=raw(dir,m.id);need(source.size()==m.n,"cached source rows");
  auto valid=validity3(dir+"/col"+std::to_string(m.id)+".valid.bitmap",m.n);
  need(valid==table.valid[j],"cached full validity reconstruction");
  if(m.width==16)need(cache.p16.at(m.id).size()==m.n,"cached16 rows");
  else need(cache.p32.at(m.id).size()==m.n,"cached32 rows");
  for(U k=0;k<m.n;++k){
   need(m.width==32||source[k]<=65535,"cached verify source width");
   need(valid3(valid,k)||source[k]==0,"cached verify canonical NULL source");
   if(valid3(valid,k)){
    U value=m.width==16?U(cache.p16.at(m.id)[k]):cache.p32.at(m.id)[k];
    need(value==source[k],"cached valid value reconstruction");++live;
   }
  }
  values+=m.n;
 }
 std::cout<<"{\"cached_logical_values\":"<<values<<",\"verified_non_null_values\":"<<live
          <<",\"full_validity_verified\":true,\"NULL_cache_payload_semantically_ignored\":true}\n";
}

static int decode_once_entry(int argc,char**argv){
 // Preserve every pre-existing command, including its original selector rules.
 if(argc<6||(std::string(argv[5])!="query_cached"&&std::string(argv[5])!="verify_cached"))
  return factorial_entry(argc,argv);
 const std::string mode=argv[5];
 need((mode=="query_cached"&&argc==11)||(mode=="verify_cached"&&argc==8),
      "query_cached encoded prepared queries bitmap metrics; verify_cached encoded prepared");
 need(std::string(argv[1])=="--bitmap-kernel"&&std::string(argv[3])=="--physical-fill",
      "decode-once flag order");
 const std::string kernel=argv[2],fill_mode=argv[4];
 need(kernel=="scalar"||kernel=="sse2","decode-once bitmap kernel");
 need(fill_mode=="0"||fill_mode=="3","decode-once primary fill selector");
 const bool available=compare_mask_available();
 need(kernel!="sse2"||available,"requested SSE2 unavailable; no silent substitution");
 compare_mask_enabled=kernel=="sse2";physical_fill_mode=unsigned(fill_mode[0]-'0');
 std::cout<<"{\"bitmap_kernel\":\""<<kernel<<"\",\"physical_fill_mode\":"<<physical_fill_mode
          <<",\"compiled_sse2\":"<<FACTORIAL_COMPILED_SSE2<<",\"runtime_sse2\":"<<int(available)
          <<",\"control\":\"decode_once\"}\n";
 auto setup_start=Clock::now();const std::string dir=argv[7];
 auto table=load3(argv[6],binding3(dir+"/binding.txt"));
 if(mode=="verify_cached"){
  auto cache=materialize_cache(table);verify_cached(table,cache,dir);return 0;
 }
 auto qs=queries3(argv[8]);
 for(const auto&q:qs){
  auto m=std::find_if(table.meta.begin(),table.meta.end(),[&](const Meta3&m){return m.id==q.query.col;});
  need(m!=table.meta.end(),"cached query column");
  need(q.constant!=-1||m->width==32||q.query.p.value<=65535,"cached source-width literal");
 }
 const U n=table.meta[0].n;
 const auto loaded_rss=resident_kib();const double setup_ms=ms(setup_start);
 // Primary timer starts BEFORE all cache materialization and cache allocation.
 auto bank_start=Clock::now();auto cache=materialize_cache(table);
 const double decode_once_ms=ms(bank_start);const auto post_cache_rss=resident_kib();
 auto comparison_start=Clock::now();const size_t stride=(size_t(n)+63)/64;
 B bank(stride*qs.size(),0);
 for(const auto&q:qs){
  U j=U(std::find_if(table.meta.begin(),table.meta.end(),[&](const Meta3&m){return m.id==q.query.col;})-table.meta.begin());
  B out(stride,0);
  // Exactly the admitted packed_width comparison, validity and bank-copy path.
  if(q.constant>=0){if(q.constant)out=table.valid[j];}
  else{
   if(table.meta[j].width==16)bulk_words(cache.p16.at(q.query.col).data(),n,q.query.p,out,0);
   else bulk_words(cache.p32.at(q.query.col).data(),n,q.query.p,out,0);
   for(U k=0;k<out.size();++k)out[k]&=table.valid[j][k];
  }
  std::copy(out.begin(),out.end(),bank.begin()+size_t(q.query.id)*stride);
 }
 const auto post_bank_rss=resident_kib();const double comparison_ms=ms(comparison_start);
 const double bank_ms=ms(bank_start);
 std::ofstream output(argv[9],std::ios::binary),metrics(argv[10]);need(bool(output)&&bool(metrics),"cached output");
 output.write(reinterpret_cast<const char*>(bank.data()),bank.size()*8);output.flush();need(bool(output),"cached bitmap write");
 metrics<<"{\"record\":\"setup\",\"setup_ms\":"<<setup_ms<<",\"loaded_rss_kib\":"<<loaded_rss
        <<",\"input_predecode\":false,\"original_values_read\":false}\n";
 metrics<<"{\"record\":\"bank\",\"bank_ms\":"<<bank_ms<<",\"primary_bank_ms\":"<<bank_ms
        <<",\"decode_once_ms\":"<<decode_once_ms<<",\"comparison_ms\":"<<comparison_ms
        <<",\"query_count\":"<<qs.size()<<",\"rows\":"<<n<<",\"output_bytes\":"<<bank.size()*8
        <<",\"cache_payload_bytes\":"<<cache.payload_bytes<<",\"cache_payload_capacity_bytes\":"<<cache.payload_capacity_bytes
        <<",\"post_cache_rss_kib\":"<<post_cache_rss<<",\"post_bank_rss_kib\":"<<post_bank_rss
        <<",\"full_bank_allocation_copy_validity_charged\":true,\"cold_decode_cache_allocation_charged\":true"
        <<",\"compressed_tree_retained\":true,\"cache_transient\":true,\"rss_observations_charged\":true}\n";
 metrics.flush();need(bool(metrics),"cached metrics write");return 0;
}

int frozen_decode_once_outer_entry(int argc,char**argv){try{return decode_once_entry(argc,argv);}catch(const std::exception&e){std::cerr<<"REJECT: "<<e.what()<<'\n';return 2;}}
