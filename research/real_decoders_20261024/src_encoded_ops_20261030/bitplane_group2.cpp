// Per-group bit-sliced SUM with per-group base: BSBITS02.
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
struct GH { uint64_t count, base; uint32_t bits; };
int main(int argc,char**argv){
 if(argc<4){return 2;} std::string mode=argv[1];
 if(mode=="build"){
  auto t0=std::chrono::steady_clock::now();
  auto read_i32=[](const char* p,std::vector<int32_t>&v){std::ifstream f(p,std::ios::binary);f.seekg(0,std::ios::end);size_t n=f.tellg()/4;f.seekg(0);v.resize(n);f.read((char*)v.data(),n*4);};
  std::vector<int32_t> fare,pass;read_i32(argv[2],fare);read_i32(argv[3],pass);size_t n=fare.size();const uint64_t G=16;
  std::vector<std::vector<int64_t>> vals(G);
  for(size_t i=0;i<n;i++){int g=pass[i];if(g<0||g>=int(G))continue;vals[g].push_back(fare[i]);}
  std::ofstream o(argv[4],std::ios::binary);o.write("BSBITS02",8);o.write((char*)&G,8);o.write((char*)&n,8);
  std::vector<GH> gh(G);
  for(int g=0;g<G;g++){auto&a=vals[g];uint64_t base=0;if(!a.empty()){base=uint64_t(*std::min_element(a.begin(),a.end()));uint64_t mx=0;for(auto v:a)mx=std::max<uint64_t>(mx,uint64_t(v)-base);uint32_t bits=0;while(mx){bits++;mx>>=1;}gh[g]={a.size(),base,bits};}else gh[g]={0,0,0};}
  o.write((char*)gh.data(),G*sizeof(GH));
  for(int g=0;g<G;g++){auto&a=vals[g];size_t c=a.size(),words=(c+63)/64;std::vector<uint64_t> plane(words);for(uint32_t j=0;j<gh[g].bits;j++){std::fill(plane.begin(),plane.end(),0);for(size_t i=0;i<c;i++){uint64_t d=uint64_t(a[i])-gh[g].base;if((d>>j)&1)plane[i>>6]|=1ULL<<(i&63);}o.write((char*)plane.data(),words*8);}}
  o.close();auto t1=std::chrono::steady_clock::now();fprintf(stderr,"build_ms=%.3f\n",std::chrono::duration<double,std::milli>(t1-t0).count());return 0;
 }
 auto t0=std::chrono::steady_clock::now();int fd=open(argv[2],O_RDONLY);struct stat st{};fstat(fd,&st);const char* base=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0);close(fd);
 char magic[8];memcpy(magic,base,8);if(memcmp(magic,"BSBITS02",8))return 1;uint64_t G,total;memcpy(&G,base+8,8);memcpy(&total,base+16,8);const GH* gh=(const GH*)(base+24);size_t off=24+G*sizeof(GH);std::vector<__int128> sum(G,0);
 for(uint64_t g=0;g<G;g++){uint64_t c=gh[g].count,words=(c+63)/64;const uint64_t* planes=(const uint64_t*)(base+off);__int128 s=(__int128)gh[g].base*c;for(uint32_t j=0;j<gh[g].bits;j++){uint64_t pc=0;const uint64_t* p=planes+uint64_t(j)*words;for(uint64_t w=0;w<words;w++)pc+=__builtin_popcountll(p[w]);if(pc)s+=(__int128)pc<<j;}sum[g]=s;off+=uint64_t(gh[g].bits)*words*8;}
 auto t1=std::chrono::steady_clock::now();FILE* f=fopen(argv[3],"w");fprintf(f,"{\"rows\":[");bool first=true;for(uint64_t g=0;g<G;g++){if(!gh[g].count)continue;if(!first)fprintf(f,",");first=false;fprintf(f,"[\"%llu\",\"%lld\",\"%llu\"]",(unsigned long long)g,(long long)sum[g],(unsigned long long)gh[g].count);}fprintf(f,"]}\n");fclose(f);auto t2=std::chrono::steady_clock::now();fprintf(stderr,"{\"query_ms\":%.3f,\"output_ms\":%.3f,\"stored_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),std::chrono::duration<double,std::milli>(t2-t1).count(),std::chrono::duration<double,std::milli>(t2-t0).count());
}
