// BSBITS03 range filter: bitwise comparison masks + popcount SUM, exact COUNT/MIN/MAX.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
struct GH { uint64_t count, base, max; uint32_t bits, pad; };
int main(int argc,char**argv){
 if(argc<5){fprintf(stderr,"usage: FILE OUT lo hi [where_in=...]\n");return 2;}
 auto t0=std::chrono::steady_clock::now();
 int fd=open(argv[1],O_RDONLY); struct stat st{}; fstat(fd,&st); const char* base=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0); close(fd);
 char magic[8]; memcpy(magic,base,8); if(memcmp(magic,"BSBITS03",8)) return 1;
 uint64_t G,total; memcpy(&G,base+8,8); memcpy(&total,base+16,8); const GH* gh=(const GH*)(base+24); size_t off=24+G*sizeof(GH);
 int64_t lo=std::stoll(argv[3]), hi=std::stoll(argv[4]);
 std::vector<uint64_t> allow; bool has_allow=false;
 for(int i=5;i<argc;i++){std::string a=argv[i]; if(a.rfind("where_in=",0)==0){has_allow=true;std::string t=a.substr(9);size_t p=0;while(p<=t.size()){size_t q=t.find(',',p);if(q==std::string::npos)q=t.size();allow.push_back(std::stoull(t.substr(p,q-p)));p=q+1;}}}
 std::vector<__int128> sum(G,0); std::vector<uint64_t> cnt(G,0); std::vector<int64_t> mn(G,0), mx(G,0);
 for(uint64_t g=0;g<G;g++){
  bool ok=!has_allow; for(auto x:allow)if(x==g)ok=true; uint64_t c=gh[g].count,words=(c+63)/64; const uint64_t* planes=(const uint64_t*)(base+off);
  if(!ok){off+=uint64_t(gh[g].bits)*words*8; continue;}
  if(!c){off+=uint64_t(gh[g].bits)*words*8; continue;}
  int64_t b=(int64_t)gh[g].base; int64_t gmax=(int64_t)gh[g].max; int64_t maxd=gmax-b; uint64_t bits=gh[g].bits;
  uint64_t valid_last = (c%64)? ((1ULL<<(c%64))-1ULL) : ~0ULL;
  auto word_valid=[&](uint64_t w){ return (w==words-1)?valid_last:~0ULL; };
  uint64_t dlo = (lo<=b)?0:uint64_t((__int128)lo-b);
  uint64_t dhi = (hi>=gmax)?uint64_t(maxd):uint64_t((__int128)hi-b);
  bool all = (lo<=b && hi>=gmax);
  std::vector<uint64_t> mask(words);
  if(all){ for(uint64_t w=0;w<words;w++) mask[w]=word_valid(w); }
  else if((int64_t)dlo>(int64_t)dhi || hi<b || lo>gmax){ for(auto&x:mask)x=0; }
  else{
   for(uint64_t w=0;w<words;w++) mask[w]=word_valid(w);
   if(dhi<maxd){
    std::vector<uint64_t> eq(words), lt(words), p(words);
    for(uint64_t w=0;w<words;w++){eq[w]=word_valid(w);lt[w]=0;}
    for(int j=int(bits)-1;j>=0;j--){ for(uint64_t w=0;w<words;w++)p[w]=planes[uint64_t(j)*words+w];
     if((dhi>>j)&1){ for(uint64_t w=0;w<words;w++){lt[w]|=eq[w]&~p[w];eq[w]&=p[w];} }
     else { for(uint64_t w=0;w<words;w++)eq[w]&=~p[w]; }
    }
    for(uint64_t w=0;w<words;w++) mask[w]&=lt[w]|eq[w];
   }
   if(dlo>0){
    std::vector<uint64_t> eq(words), gt(words), p(words);
    for(uint64_t w=0;w<words;w++){eq[w]=word_valid(w);gt[w]=0;}
    for(int j=int(bits)-1;j>=0;j--){ for(uint64_t w=0;w<words;w++)p[w]=planes[uint64_t(j)*words+w];
     if((dlo>>j)&1){ for(uint64_t w=0;w<words;w++)eq[w]&=p[w]; }
     else { for(uint64_t w=0;w<words;w++){gt[w]|=eq[w]&p[w];eq[w]&=~p[w];} }
    }
    for(uint64_t w=0;w<words;w++) mask[w]&=gt[w]|eq[w];
   }
  }
  uint64_t n=0; for(uint64_t w=0;w<words;w++)n+=__builtin_popcountll(mask[w]); cnt[g]=n;
  if(!n){ mn[g]=0;mx[g]=0;off+=uint64_t(bits)*words*8; continue; }
  __int128 s=(__int128)b*(__int128)n;
  for(uint32_t j=0;j<bits;j++){const uint64_t* p=planes+uint64_t(j)*words;uint64_t pc=0;for(uint64_t w=0;w<words;w++)pc+=__builtin_popcountll(p[w]&mask[w]);if(pc)s+=(__int128)pc<<j;}
  sum[g]=s;
  // max: candidates = mask; prefer bit1 at each level
  std::vector<uint64_t> cand(mask); uint64_t maxd2=0;
  for(int j=int(bits)-1;j>=0;j--){const uint64_t* p=planes+uint64_t(j)*words;bool any=false;for(uint64_t w=0;w<words;w++)if(cand[w]&p[w]){any=true;break;}if(any){maxd2|=1ULL<<j;for(uint64_t w=0;w<words;w++)cand[w]&=p[w];}else{for(uint64_t w=0;w<words;w++)cand[w]&=~p[w];}}
  mx[g]=b+(int64_t)maxd2;
  cand=mask; uint64_t mind=0;
  for(int j=int(bits)-1;j>=0;j--){const uint64_t* p=planes+uint64_t(j)*words;bool any=false;for(uint64_t w=0;w<words;w++)if(cand[w]&~p[w]){any=true;break;}if(any){for(uint64_t w=0;w<words;w++)cand[w]&=~p[w];}else{mind|=1ULL<<j;for(uint64_t w=0;w<words;w++)cand[w]&=p[w];}}
  mn[g]=b+(int64_t)mind; off+=uint64_t(bits)*words*8;
 }
 auto t1=std::chrono::steady_clock::now(); FILE* f=fopen(argv[2],"w");fprintf(f,"{\"rows\":[");bool first=true;
 for(uint64_t g=0;g<G;g++){if(!cnt[g])continue;if(!first)fprintf(f,",");first=false;fprintf(f,"[\"%llu\",\"%lld\",\"%llu\",\"%lld\",\"%lld\"]",(unsigned long long)g,(long long)sum[g],(unsigned long long)cnt[g],(long long)mn[g],(long long)mx[g]);}
 fprintf(f,"]}\n");fclose(f);auto t2=std::chrono::steady_clock::now();fprintf(stderr,"{\"query_ms\":%.3f,\"output_ms\":%.3f,\"stored_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),std::chrono::duration<double,std::milli>(t2-t1).count(),std::chrono::duration<double,std::milli>(t2-t0).count());
}
