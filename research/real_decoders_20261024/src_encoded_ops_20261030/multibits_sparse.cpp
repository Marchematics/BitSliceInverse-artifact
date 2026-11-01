// Sparse price-index candidate path for multi-column predicates.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
int main(int argc,char**argv){
 if(argc<11){fprintf(stderr,"query ridx g p d q out plo phi dlo dhi qlo qhi\n");return 2;}
 auto t0=std::chrono::steady_clock::now();
 int fi=open(argv[1],O_RDONLY); struct stat si{}; fstat(fi,&si); const char* bi=(const char*)mmap(nullptr,si.st_size,PROT_READ,MAP_PRIVATE,fi,0); close(fi);
 uint64_t n; memcpy(&n,bi+8,8); const int32_t* ids=(const int32_t*)(bi+24);
 const char* names[4]={argv[2],argv[3],argv[4],argv[5]}; const int64_t* A[4];
 for(int k=0;k<4;k++){int f=open(names[k],O_RDONLY);struct stat s{};fstat(f,&s);A[k]=(const int64_t*)mmap(nullptr,s.st_size,PROT_READ,MAP_PRIVATE,f,0);close(f);}
 int64_t plo=std::stoll(argv[7]),phi=std::stoll(argv[8]),dlo=std::stoll(argv[9]),dhi=std::stoll(argv[10]),qlo=std::stoll(argv[11]),qhi=std::stoll(argv[12]);
 const int64_t* p=A[1]; const int32_t* l=std::lower_bound(ids,ids+n,plo,[&](int32_t id,int64_t v){return p[id]<v;}); const int32_t* r=std::upper_bound(ids,ids+n,phi,[&](int64_t v,int32_t id){return v<p[id];});
 auto t1=std::chrono::steady_clock::now(); int64_t sum[3]={0,0,0}; uint64_t cnt[3]={0,0,0};
 for(auto it=l;it<r;++it){int32_t id=*it; int64_t pv=p[id],dv=A[2][id],qv=A[3][id]; if(dv<dlo||dv>dhi||qv<qlo||qv>qhi)continue; int g=A[0][id]; sum[g]+=pv; cnt[g]++;}
 auto t2=std::chrono::steady_clock::now(); FILE* f=fopen(argv[6],"w");fprintf(f,"{\"rows\":[");bool first=true;for(int g=0;g<3;g++){if(!cnt[g])continue;if(!first)fprintf(f,",");first=false;fprintf(f,"[\"%d\",\"%lld\",\"%llu\"]",g,(long long)sum[g],(unsigned long long)cnt[g]);}fprintf(f,"]}\n");fclose(f);auto t3=std::chrono::steady_clock::now();fprintf(stderr,"{\"query_ms\":%.3f,\"output_ms\":%.3f,\"stored_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),std::chrono::duration<double,std::milli>(t3-t2).count(),std::chrono::duration<double,std::milli>(t3-t0).count());
}
