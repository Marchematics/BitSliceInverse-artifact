// Per-group bit-sliced SUM: build 32 bitplanes per group, query with popcount.
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
static constexpr uint64_t B = 0x80000000ULL;
struct H { char magic[8]; uint64_t groups, total; };
int main(int argc,char**argv){
    if(argc<4){fprintf(stderr,"usage: build FARE I32 PASS I32 OUT | query FILE OUT\n");return 2;}
    std::string mode=argv[1];
    if(mode=="build"){
        auto t0=std::chrono::steady_clock::now();
        auto read_i32=[](const char* p, std::vector<int32_t>& v){ std::ifstream f(p,std::ios::binary); f.seekg(0,std::ios::end); size_t n=f.tellg()/4; f.seekg(0); v.resize(n); f.read((char*)v.data(),n*4); };
        std::vector<int32_t> fare, pass; read_i32(argv[2],fare); read_i32(argv[3],pass);
        size_t n=fare.size(); const uint64_t G=16;
        std::vector<std::vector<uint32_t>> vals(G);
        for(size_t i=0;i<n;i++){ int g=pass[i]; if(g<0||g>=int(G)) continue; vals[g].push_back(uint32_t(int64_t(fare[i])+int64_t(B))); }
        std::ofstream o(argv[4],std::ios::binary); o.write("BSBITS01",8); uint64_t tot=n; o.write((char*)&G,8); o.write((char*)&tot,8);
        std::vector<uint64_t> counts(G); for(int g=0;g<G;g++) counts[g]=vals[g].size(); o.write((char*)counts.data(),G*8);
        for(int g=0;g<G;g++){
            size_t c=vals[g].size(), words=(c+63)/64; std::vector<uint64_t> plane(words);
            for(int j=0;j<32;j++){ std::fill(plane.begin(),plane.end(),0); for(size_t i=0;i<c;i++) if((vals[g][i]>>j)&1) plane[i>>6] |= 1ULL<<(i&63); o.write((char*)plane.data(),words*8); }
        }
        o.close(); auto t1=std::chrono::steady_clock::now();
        fprintf(stderr,"build_ms=%.3f\n",std::chrono::duration<double,std::milli>(t1-t0).count()); return 0;
    }
    auto t0=std::chrono::steady_clock::now();
    int fd=open(argv[2],O_RDONLY); struct stat st{}; fstat(fd,&st); const char* base=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0); close(fd);
    H h; memcpy(&h,base,sizeof(h)); if(memcmp(h.magic,"BSBITS01",8)) return 1; const uint64_t* counts=(const uint64_t*)(base+sizeof(H)); size_t off=sizeof(H)+8*h.groups;
    std::vector<__int128> sum(h.groups,0);
    for(uint64_t g=0;g<h.groups;g++){
        uint64_t c=counts[g], words=(c+63)/64; const uint64_t* planes=(const uint64_t*)(base+off);
        __int128 s=0; for(int j=0;j<32;j++){ uint64_t pc=0; const uint64_t* p=planes+j*words; for(uint64_t w=0;w<words;w++) pc+=__builtin_popcountll(p[w]); if(pc) s += (__int128)pc << j; }
        s -= (__int128)B*(__int128)c; sum[g]=s; off += 32*words*8;
    }
    auto t1=std::chrono::steady_clock::now(); FILE* f=fopen(argv[3],"w"); fprintf(f,"{\"rows\":["); bool first=true;
    for(uint64_t g=0;g<h.groups;g++){ if(!counts[g])continue; if(!first)fprintf(f,","); first=false; fprintf(f,"[\"%llu\",\"%lld\",\"%llu\"]",(unsigned long long)g,(long long)sum[g],(unsigned long long)counts[g]); }
    fprintf(f,"]}\n"); fclose(f); auto t2=std::chrono::steady_clock::now();
    fprintf(stderr,"{\"query_ms\":%.3f,\"output_ms\":%.3f,\"stored_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),std::chrono::duration<double,std::milli>(t2-t1).count(),std::chrono::duration<double,std::milli>(t2-t0).count()); }
