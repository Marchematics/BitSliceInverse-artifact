// Multi-column per-group bitplanes for dynamic multi-predicate aggregation.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
struct GH { int64_t base, max; uint64_t count; uint32_t bits, pad; };
static std::vector<int64_t> read_i64(const char* p){ std::ifstream f(p,std::ios::binary); f.seekg(0,std::ios::end); size_t n=f.tellg()/8; f.seekg(0); std::vector<int64_t> v(n); f.read((char*)v.data(),n*8); return v; }
int main(int argc,char**argv){
 if(argc<3)return 2; const std::string mode=argv[1];
 if(mode=="build"){
  if(argc<7){fprintf(stderr,"build gid p d q out\n");return 2;}
  auto t0=std::chrono::steady_clock::now();
  std::vector<std::vector<int64_t>> cols={read_i64(argv[2]),read_i64(argv[3]),read_i64(argv[4]),read_i64(argv[5])};
  size_t n=cols[0].size(); uint64_t G=0; for(auto x:cols[0]) if(x>=0 && uint64_t(x)+1>G) G=uint64_t(x)+1; const uint32_t C=3;
  std::vector<std::vector<int64_t>> vals[3]; // [c][g]
  for(uint32_t c=0;c<C;c++) vals[c].resize(G);
  for(size_t i=0;i<n;i++){ int64_t g=cols[0][i]; if(g<0)continue; for(uint32_t c=0;c<C;c++) vals[c][g].push_back(cols[c+1][i]); }
  std::ofstream o(argv[6],std::ios::binary); o.write("MBITS01 ",8); o.write((char*)&G,8); o.write((char*)&C,4); o.write((char*)&n,8);
  std::vector<GH> gh(G*C);
  for(uint64_t g=0;g<G;g++) for(uint32_t c=0;c<C;c++){ auto&a=vals[c][g]; if(a.empty()){gh[g*C+c]={0,0,0,0,0};continue;} int64_t b=*std::min_element(a.begin(),a.end()); int64_t mx=*std::max_element(a.begin(),a.end()); uint64_t bits=0; uint64_t m=uint64_t(mx-b); while(m){bits++;m>>=1;} gh[g*C+c]={b,mx,a.size(),uint32_t(bits),0}; }
  o.write((char*)gh.data(),G*C*sizeof(GH));
  for(uint64_t g=0;g<G;g++) for(uint32_t c=0;c<C;c++){ auto&a=vals[c][g]; auto&h=gh[g*C+c]; size_t words=(a.size()+63)/64; std::vector<uint64_t> plane(words); for(uint32_t j=0;j<h.bits;j++){ std::fill(plane.begin(),plane.end(),0); for(size_t i=0;i<a.size();i++){uint64_t d=uint64_t(a[i]-h.base); if((d>>j)&1)plane[i>>6]|=1ULL<<(i&63);} o.write((char*)plane.data(),words*8);} }
  o.close(); auto t1=std::chrono::steady_clock::now(); fprintf(stderr,"{\"build_ms\":%.3f,\"groups\":%llu,\"bytes\":%llu}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),(unsigned long long)G,(unsigned long long)0); return 0;
 }
 if(argc<4)return 2; auto t0=std::chrono::steady_clock::now();
 int fd=open(argv[2],O_RDONLY); struct stat st{}; fstat(fd,&st); const char* base=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0); close(fd);
 char magic[8]; memcpy(magic,base,8); if(memcmp(magic,"MBITS01 ",8))return 1; uint64_t G,n; uint32_t C; memcpy(&G,base+8,8); memcpy(&C,base+16,4); memcpy(&n,base+20,8); const GH* gh=(const GH*)(base+28); size_t off=28+G*C*sizeof(GH);
 // args: p_lo,p_hi,d_lo,d_hi,q_lo,q_hi
 int64_t lo[3],hi[3]; for(int c=0;c<3;c++){lo[c]=std::stoll(argv[4+2*c]);hi[c]=std::stoll(argv[5+2*c]);}
  // optional block-metadata empty short-circuit
 { std::string blkpath; for(int i=0;i<argc;i++){std::string a=argv[i]; if(a.rfind("blk=",0)==0) blkpath=a.substr(4);} const char* blk=blkpath.empty()?nullptr:blkpath.c_str();
  if(blk){ int bfd=open(blk,O_RDONLY); struct stat bs{}; fstat(bfd,&bs); const char* bb=(const char*)mmap(nullptr,bs.st_size,PROT_READ,MAP_PRIVATE,bfd,0); close(bfd); uint32_t G2,B,nmeta; memcpy(&G2,bb+8,4); memcpy(&B,bb+12,4); memcpy(&nmeta,bb+16,4); const int64_t* bm=(const int64_t*)(bb+20); bool any=false; uint64_t mi=0;
   for(uint64_t g=0;g<G;g++){ uint64_t c0=gh[g*C].count, nb=(c0+B-1)/B; for(uint64_t b=0;b<nb;b++){ const int64_t* r=bm+mi*6; bool cand=true; for(int c=0;c<3;c++) if(hi[c]<r[c] || lo[c]>r[3+c]) cand=false; if(cand){any=true;break;} mi++; } if(any)break; }
   if(!any){ auto te=std::chrono::steady_clock::now(); FILE* f=fopen(argv[3],"w"); fprintf(f,"{\"rows\":[]}\n"); fclose(f); auto tf=std::chrono::steady_clock::now(); fprintf(stderr,"{\"query_ms\":%.3f,\"output_ms\":%.3f,\"stored_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(te-t0).count(),std::chrono::duration<double,std::milli>(tf-te).count(),std::chrono::duration<double,std::milli>(tf-t0).count()); return 0; }
  }
 }
 std::vector<__int128> sum(G,0); std::vector<uint64_t> cnt(G,0);
 for(uint64_t g=0;g<G;g++){
  uint64_t c0=gh[g*C].count, words=(c0+63)/64; if(!c0){for(uint32_t c=0;c<C;c++){uint32_t b=gh[g*C+c].bits; off+=uint64_t(b)*words*8;} continue;}
  std::vector<uint64_t> mask(words), valid(words), cm(words), eq(words), le(words), gt(words), p(words);
  for(uint64_t w=0;w<words;w++) valid[w]=~0ULL; if(c0%64)valid[words-1]=(1ULL<<(c0%64))-1ULL;
  mask=valid;
  for(uint32_t c=0;c<C;c++){
   const GH& h=gh[g*C+c]; const uint64_t* planes=(const uint64_t*)(base+off); uint32_t bits=h.bits;
   int64_t b=h.base,gmax=h.max; bool all=(lo[c]<=b && hi[c]>=gmax);
   if(!all){
    uint64_t dlo=(lo[c]<=b)?0:uint64_t((__int128)lo[c]-b); uint64_t dhi=(hi[c]>=gmax)?uint64_t(gmax-b):uint64_t((__int128)hi[c]-b);
    if((int64_t)dlo>(int64_t)dhi || hi[c]<b || lo[c]>gmax){ for(auto&x:mask)x=0; }
    else{
     if(dhi<uint64_t(gmax-b)){
      for(uint64_t w=0;w<words;w++){eq[w]=valid[w];le[w]=0;}
      for(int j=int(bits)-1;j>=0;j--){const uint64_t* pj=planes+uint64_t(j)*words; for(uint64_t w=0;w<words;w++)p[w]=pj[w]; if((dhi>>j)&1){for(uint64_t w=0;w<words;w++){le[w]|=eq[w]&~p[w];eq[w]&=p[w];}}else{for(uint64_t w=0;w<words;w++)eq[w]&=~p[w];}}
      for(uint64_t w=0;w<words;w++)mask[w]&=le[w]|eq[w];
     }
     if(dlo>0){
      for(uint64_t w=0;w<words;w++){eq[w]=valid[w];gt[w]=0;}
      for(int j=int(bits)-1;j>=0;j--){const uint64_t* pj=planes+uint64_t(j)*words; for(uint64_t w=0;w<words;w++)p[w]=pj[w]; if((dlo>>j)&1){for(uint64_t w=0;w<words;w++)eq[w]&=p[w];}else{for(uint64_t w=0;w<words;w++){gt[w]|=eq[w]&p[w];eq[w]&=~p[w];}}}
      for(uint64_t w=0;w<words;w++)mask[w]&=gt[w]|eq[w];
     }
    }
   }
   off+=uint64_t(bits)*words*8;
  }
  uint64_t nc=0; for(uint64_t w=0;w<words;w++)nc+=__builtin_popcountll(mask[w]); cnt[g]=nc; if(!nc)continue;
  const GH& hp=gh[g*C]; const uint64_t* pp=(const uint64_t*)(base+ (28+G*C*sizeof(GH))); // need locate price planes; recompute offset
  // locate price planes: skip to g,c=0
  size_t poff=28+G*C*sizeof(GH); for(uint64_t gg=0;gg<g;gg++){uint64_t wc=(gh[gg*C].count+63)/64; for(uint32_t cc=0;cc<C;cc++) poff+=uint64_t(gh[gg*C+cc].bits)*wc*8;}
  const uint64_t* pplanes=(const uint64_t*)(base+poff); uint64_t words_p=words;
  __int128 s=(__int128)hp.base*(__int128)nc;
  for(uint32_t j=0;j<hp.bits;j++){const uint64_t* pj=pplanes+uint64_t(j)*words_p;uint64_t pc=0;for(uint64_t w=0;w<words;w++)pc+=__builtin_popcountll(pj[w]&mask[w]);s+=(__int128)pc<<j;}
  sum[g]=s;
 }
 auto t1=std::chrono::steady_clock::now(); FILE* f=fopen(argv[3],"w"); fprintf(f,"{\"rows\":["); bool first=true;
 for(uint64_t g=0;g<G;g++){if(!cnt[g])continue;if(!first)fprintf(f,",");first=false;fprintf(f,"[\"%llu\",\"%lld\",\"%llu\"]",(unsigned long long)g,(long long)sum[g],(unsigned long long)cnt[g]);}
 fprintf(f,"]}\n");fclose(f);auto t2=std::chrono::steady_clock::now();fprintf(stderr,"{\"query_ms\":%.3f,\"output_ms\":%.3f,\"stored_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),std::chrono::duration<double,std::milli>(t2-t1).count(),std::chrono::duration<double,std::milli>(t2-t0).count());
}
