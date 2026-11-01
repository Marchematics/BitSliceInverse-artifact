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
struct GH { int64_t base, max; uint64_t count; uint32_t bits, pad; };
struct BM { int64_t mn[3], mx[3]; };
int main(int argc,char**argv){
 if(argc<10){fprintf(stderr,"query mb blk out p_lo p_hi d_lo d_hi q_lo q_hi\n");return 2;}
 auto t0=std::chrono::steady_clock::now();
 int fd=open(argv[1],O_RDONLY); struct stat st{}; fstat(fd,&st); const char* base=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0); close(fd);
 uint64_t G,n; uint32_t C; memcpy(&G,base+8,8); memcpy(&C,base+16,4); memcpy(&n,base+20,8); const GH* gh=(const GH*)(base+28);
 int fd2=open(argv[2],O_RDONLY); struct stat st2{}; fstat(fd2,&st2); const char* b2=(const char*)mmap(nullptr,st2.st_size,PROT_READ,MAP_PRIVATE,fd2,0); close(fd2);
 uint32_t G2,B,nmeta; memcpy(&G2,b2+8,4); memcpy(&B,b2+12,4); memcpy(&nmeta,b2+16,4); const BM* bm=(const BM*)(b2+20);
 int64_t lo[3],hi[3]; for(int c=0;c<3;c++){lo[c]=std::stoll(argv[4+2*c]);hi[c]=std::stoll(argv[5+2*c]);}
 size_t planes_off=28+G*C*sizeof(GH); uint64_t word_total=0;
 std::vector<__int128> sum(G,0); std::vector<uint64_t> cnt(G,0); uint64_t meta_idx=0;
 for(uint64_t g=0;g<G;g++){
  uint64_t c0=gh[g*C].count, wg=(c0+63)/64, nb=(c0+B-1)/B; size_t goff=planes_off+word_total*8; size_t coloff=goff;
  if(g==0 && getenv("MBDBG")) fprintf(stderr,"g0 c0=%llu wg=%llu nb=%llu meta=%lld,%lld\n",(unsigned long long)c0,(unsigned long long)wg,(unsigned long long)nb,(long long)bm[meta_idx].mn[0],(long long)bm[meta_idx].mx[0]);
  for(uint64_t b=0;b<nb;b++){
   const BM& bb=bm[meta_idx+b]; bool cand=true;
   for(int c=0;c<3;c++) if(hi[c]<bb.mn[c] || lo[c]>bb.mx[c]) cand=false;
   if(g==0 && b<2 && getenv("MBDBG")) fprintf(stderr,"b%llu cand=%d mn=%lld mx=%lld\n",(unsigned long long)b,(int)cand,(long long)bb.mn[0],(long long)bb.mx[0]);
   if(!cand) continue;
   uint64_t row0=b*B, nr=std::min<uint64_t>(B,c0-row0); uint64_t lw=(nr+63)/64; uint64_t bw0=row0/64;
   std::vector<uint64_t> mask(lw), valid(lw), eq(lw), le(lw), gt(lw), p(lw);
   for(uint64_t w=0;w<lw;w++) valid[w]=~0ULL; if(nr%64)valid[lw-1]=(1ULL<<(nr%64))-1ULL;
   mask=valid;
   for(uint32_t c=0;c<C;c++){
    const GH& h=gh[g*C+c]; const uint64_t* planes=(const uint64_t*)(base+coloff); uint32_t bits=h.bits;
    if(getenv("MBDBG") && g==0 && b==0) fprintf(stderr,"g0 c%u bits%u base%lld max%lld coloff%zu goff%zu wg%llu lw%llu bw0%llu filesz%lld\n",c,bits,(long long)h.base,(long long)h.max,coloff,goff,(unsigned long long)wg,(unsigned long long)lw,(unsigned long long)bw0,(long long)st.st_size);
    std::vector<uint64_t> local(uint64_t(bits)*lw); for(uint32_t jj=0;jj<bits;jj++){const uint64_t* src=planes+uint64_t(jj)*wg+bw0; for(uint64_t ww=0;ww<lw;ww++) local[uint64_t(jj)*lw+ww]=src[ww];}
    const uint64_t* pw=local.data();
    int64_t b0=h.base,gmax=h.max; bool all=(lo[c]<=b0 && hi[c]>=gmax);
    if(!all){
     uint64_t dlo=(lo[c]<=b0)?0:uint64_t((__int128)lo[c]-b0); uint64_t dhi=(hi[c]>=gmax)?uint64_t(gmax-b0):uint64_t((__int128)hi[c]-b0);
     if((int64_t)dlo>(int64_t)dhi || hi[c]<b0 || lo[c]>gmax){for(auto&x:mask)x=0;}
     else{
      if(dhi<uint64_t(gmax-b0)){ for(uint64_t w=0;w<lw;w++){eq[w]=valid[w];le[w]=0;} for(int j=int(bits)-1;j>=0;j--){const uint64_t* pj=local.data()+uint64_t(j)*lw;for(uint64_t w=0;w<lw;w++)p[w]=pj[w];if((dhi>>j)&1){for(uint64_t w=0;w<lw;w++){le[w]|=eq[w]&~p[w];eq[w]&=p[w];}}else{for(uint64_t w=0;w<lw;w++)eq[w]&=~p[w];}} for(uint64_t w=0;w<lw;w++)mask[w]&=le[w]|eq[w]; }
      if(dlo>0){ for(uint64_t w=0;w<lw;w++){eq[w]=valid[w];gt[w]=0;} for(int j=int(bits)-1;j>=0;j--){const uint64_t* pj=local.data()+uint64_t(j)*lw;for(uint64_t w=0;w<lw;w++)p[w]=pj[w];if((dlo>>j)&1){for(uint64_t w=0;w<lw;w++)eq[w]&=p[w];}else{for(uint64_t w=0;w<lw;w++){gt[w]|=eq[w]&p[w];eq[w]&=~p[w];}}} for(uint64_t w=0;w<lw;w++)mask[w]&=gt[w]|eq[w]; }
     }
    }
    coloff+=uint64_t(bits)*wg*8;
   }
   uint64_t nc=0; for(uint64_t w=0;w<lw;w++)nc+=__builtin_popcountll(mask[w]); if(!nc)continue; cnt[g]+=nc;
   const GH& hp=gh[g*C]; const uint64_t* pp=(const uint64_t*)(base+goff); std::vector<uint64_t> lpp(uint64_t(hp.bits)*lw); for(uint32_t jj=0;jj<hp.bits;jj++){const uint64_t* src=pp+uint64_t(jj)*wg+bw0; for(uint64_t ww=0;ww<lw;ww++) lpp[uint64_t(jj)*lw+ww]=src[ww];} __int128 s=(__int128)hp.base*nc;
   for(uint32_t j=0;j<hp.bits;j++){const uint64_t* pj=lpp.data()+uint64_t(j)*lw;uint64_t pc=0;for(uint64_t w=0;w<lw;w++)pc+=__builtin_popcountll(pj[w]&mask[w]);s+=(__int128)pc<<j;} sum[g]+=s;
  }
  { uint64_t sb=0; for(uint32_t cc=0;cc<C;cc++) sb+=gh[g*C+cc].bits; word_total+=wg*sb; } meta_idx+=nb;
 }
 auto t1=std::chrono::steady_clock::now(); FILE* f=fopen(argv[3],"w");fprintf(f,"{\"rows\":[");bool first=true;for(uint64_t g=0;g<G;g++){if(!cnt[g])continue;if(!first)fprintf(f,",");first=false;fprintf(f,"[\"%llu\",\"%lld\",\"%llu\"]",(unsigned long long)g,(long long)sum[g],(unsigned long long)cnt[g]);}fprintf(f,"]}\n");fclose(f);auto t2=std::chrono::steady_clock::now();fprintf(stderr,"{\"query_ms\":%.3f,\"output_ms\":%.3f,\"stored_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),std::chrono::duration<double,std::milli>(t2-t1).count(),std::chrono::duration<double,std::milli>(t2-t0).count());
}
