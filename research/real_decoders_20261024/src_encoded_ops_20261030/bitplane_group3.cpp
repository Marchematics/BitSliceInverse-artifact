// Per-group bit-sliced SUM with per-group base: BSBITS02.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <charconv>
#include <fcntl.h>
#include <fstream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
struct GH { uint64_t count, base, max; uint32_t bits, pad; };
int main(int argc,char**argv){
 if(argc<4){return 2;} std::string mode=argv[1];
 if(mode=="build"){
  auto t0=std::chrono::steady_clock::now();
  auto read_i32=[](const char* p,std::vector<int32_t>&v){std::ifstream f(p,std::ios::binary);f.seekg(0,std::ios::end);size_t n=f.tellg()/4;f.seekg(0);v.resize(n);f.read((char*)v.data(),n*4);};
  std::vector<int32_t> fare,pass;read_i32(argv[2],fare);read_i32(argv[3],pass);size_t n=fare.size();const uint64_t G=argc>5?std::stoull(argv[5]):16;
  std::vector<std::vector<int64_t>> vals(G);
  for(size_t i=0;i<n;i++){int g=pass[i];if(g<0||g>=int(G))continue;vals[g].push_back(fare[i]);}
  std::ofstream o(argv[4],std::ios::binary);o.write("BSBITS03",8);o.write((char*)&G,8);o.write((char*)&n,8);
  std::vector<GH> gh(G);
  for(int g=0;g<G;g++){auto&a=vals[g];uint64_t base=0;if(!a.empty()){int64_t sb=*std::min_element(a.begin(),a.end());base=uint64_t(sb);uint64_t mx=0;for(auto v:a)mx=std::max<uint64_t>(mx,uint64_t(v)-base);uint32_t bits=0;while(mx){bits++;mx>>=1;}int64_t mxv=sb; for(auto x:a) mxv=std::max<int64_t>(mxv,x); gh[g]={a.size(),base,uint64_t(mxv),bits,0};}else gh[g]={0,0,0,0,0};}
  o.write((char*)gh.data(),G*sizeof(GH));
  for(int g=0;g<G;g++){auto&a=vals[g];size_t c=a.size(),words=(c+63)/64;std::vector<uint64_t> plane(words);for(uint32_t j=0;j<gh[g].bits;j++){std::fill(plane.begin(),plane.end(),0);for(size_t i=0;i<c;i++){uint64_t d=uint64_t(a[i])-gh[g].base;if((d>>j)&1)plane[i>>6]|=1ULL<<(i&63);}o.write((char*)plane.data(),words*8);}}
  o.close();auto t1=std::chrono::steady_clock::now();fprintf(stderr,"build_ms=%.3f\n",std::chrono::duration<double,std::milli>(t1-t0).count());return 0;
 }
 std::vector<uint64_t> allow; bool has_allow=false; bool binary=false; long double having_sum=-1e300L;
 for(int i=3;i<argc;i++){std::string a=argv[i]; if(a.rfind("where_in=",0)==0){has_allow=true;std::string t=a.substr(9);size_t p=0;while(p<=t.size()){size_t q=t.find(',',p);if(q==std::string::npos)q=t.size();allow.push_back(std::stoull(t.substr(p,q-p)));p=q+1;}} else if(a.rfind("having_sum>=",0)==0){having_sum=std::strtold(a.substr(12).c_str(),nullptr);} else if(a=="format=bin"){binary=true;}}
 auto t0=std::chrono::steady_clock::now();int fd=open(argv[2],O_RDONLY);struct stat st{};fstat(fd,&st);const char* base=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0);close(fd);
 char magic[8];memcpy(magic,base,8);if(memcmp(magic,"BSBITS03",8))return 1;uint64_t G,total;memcpy(&G,base+8,8);memcpy(&total,base+16,8);const GH* gh=(const GH*)(base+24);size_t off=24+G*sizeof(GH);std::vector<__int128> sum(G,0);
 for(uint64_t g=0;g<G;g++){ bool ok=!has_allow; for(auto x:allow)if(x==g)ok=true; uint64_t c=gh[g].count,words=(c+63)/64; if(!ok){off+=uint64_t(gh[g].bits)*words*8; continue;}const uint64_t* planes=(const uint64_t*)(base+off);__int128 s=(__int128)(int64_t)gh[g].base*(__int128)c;for(uint32_t j=0;j<gh[g].bits;j++){uint64_t pc=0;const uint64_t* p=planes+uint64_t(j)*words;for(uint64_t w=0;w<words;w++)pc+=__builtin_popcountll(p[w]);if(pc)s+=(__int128)pc<<j;}sum[g]=s;off+=uint64_t(gh[g].bits)*words*8;}
 auto t1=std::chrono::steady_clock::now(); if(binary){FILE* fb=fopen(argv[3],"wb"); for(uint64_t g=0;g<G;g++){if(!gh[g].count)continue; if(has_allow){bool ok=false;for(auto x:allow)if(x==g)ok=true;if(!ok)continue;} if(having_sum>-1e299L && (long double)sum[g]<having_sum)continue; struct {uint32_t g; int64_t s,c; int32_t mn,mx;} rec{uint32_t(g),(int64_t)sum[g],(int64_t)gh[g].count,(int32_t)(int64_t)gh[g].base,(int32_t)(int64_t)gh[g].max}; fwrite(&rec,sizeof(rec),1,fb);} fclose(fb); auto tb=std::chrono::steady_clock::now(); fprintf(stderr,"{\"query_ms\":%.3f,\"output_ms\":%.3f,\"stored_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),std::chrono::duration<double,std::milli>(tb-t1).count(),std::chrono::duration<double,std::milli>(tb-t0).count()); return 0;} std::string out; out.reserve(64*1024*1024); out += "{\"rows\":["; bool first=true; char nb[64];
 for(uint64_t g=0;g<G;g++){if(!gh[g].count)continue; if(has_allow){bool ok=false;for(auto x:allow)if(x==g)ok=true;if(!ok)continue;} if(having_sum>-1e299L && (long double)sum[g]<having_sum)continue; if(!first)out += ','; first=false;
  out += "[" ; auto add=[&](long long v){auto r=std::to_chars(nb,nb+sizeof(nb),v); out += '"'; out.append(nb,r.ptr-nb); out += '"';};
  add((long long)g); out += ','; add((long long)sum[g]); out += ','; add((long long)gh[g].count); out += ','; add((long long)(int64_t)gh[g].base); out += ','; add((long long)(int64_t)gh[g].max); out += ']'; }
 out += "]}\n"; FILE* f=fopen(argv[3],"wb"); fwrite(out.data(),1,out.size(),f); fclose(f); auto t2=std::chrono::steady_clock::now();fprintf(stderr,"{\"query_ms\":%.3f,\"output_ms\":%.3f,\"stored_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),std::chrono::duration<double,std::milli>(t2-t1).count(),std::chrono::duration<double,std::milli>(t2-t0).count());
}
