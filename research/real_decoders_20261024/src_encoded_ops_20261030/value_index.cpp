// Grouped sorted-value + prefix-sum index for exact range aggregates.
#include <algorithm>
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
#include <chrono>
struct H { char magic[8]; uint64_t ngroups, n; };
int main(int argc, char** argv) {
    if (argc < 4) { fprintf(stderr,"usage: value_index INDEX DICT OUT [range=lo,hi] [where_in=a,b]\n"); return 2; }
    auto t0=std::chrono::steady_clock::now();
    int fd=open(argv[1],O_RDONLY); struct stat st{}; if(fstat(fd,&st)) return 1;
    const char* base=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0); close(fd);
    H h; memcpy(&h,base,sizeof(h)); if(memcmp(h.magic,"BSVIDX01",8)) return 1;
    const uint64_t ng=h.ngroups, n=h.n; const uint64_t* counts=(const uint64_t*)(base+sizeof(H));
    size_t off=sizeof(H)+8*ng;
    std::vector<const int32_t*> vals(ng); std::vector<const int64_t*> pref(ng);
    for(uint64_t g=0;g<ng;g++){ vals[g]=(const int32_t*)(base+off); off+=counts[g]*4; pref[g]=(const int64_t*)(base+off); off+=(counts[g]+1)*8; }
    int64_t lo=INT64_MIN, hi=INT64_MAX; bool range=false;
    std::vector<std::string> in_names; std::string dict_path;
    for(int i=4;i<argc;i++){ std::string a=argv[i];
        if(a.rfind("range=",0)==0){ auto p=a.find(','); lo=std::stoll(a.substr(6,p-6)); hi=std::stoll(a.substr(p+1)); range=true; }
        else if(a.rfind("where_in=",0)==0){ std::string s=a.substr(9); size_t p=0; while(p<=s.size()){ size_t q=s.find(',',p); if(q==std::string::npos) q=s.size(); in_names.push_back(s.substr(p,q-p)); p=q+1; } }
    }
    dict_path=argv[2];
    std::ifstream f(dict_path,std::ios::binary); std::string d((std::istreambuf_iterator<char>(f)),{});
    if(d.size()<52||d.compare(0,8,"BSSTR01\n")) return 1; const char* p=d.data()+48;
    uint32_t k; memcpy(&k,p,4); p+=4; std::vector<std::string> names;
    for(uint32_t i=0;i<k;i++){ uint32_t len; memcpy(&len,p,4); p+=4; names.emplace_back(p,len); p+=len; }
    auto t1=std::chrono::steady_clock::now();
    std::vector<__int128> sum(k,0); std::vector<uint64_t> cnt(k,0); std::vector<int32_t> mn(k,INT32_MAX), mx(k,INT32_MIN);
    for(uint64_t g=0;g<ng&&g<k;g++){
        bool ok=true; for(auto& x:in_names) if(x!=names[g]) ok=false; if(!in_names.empty()&&!ok) continue;
        const int32_t* v=vals[g]; uint64_t c=counts[g];
        uint64_t a=std::lower_bound(v,v+c,(int32_t)std::max<int64_t>(lo,INT32_MIN))-v;
        uint64_t b=std::upper_bound(v,v+c,(int32_t)std::min<int64_t>(hi,INT32_MAX))-v;
        if(b<=a) continue; cnt[g]=b-a; sum[g]=(__int128)pref[g][b]-pref[g][a]; mn[g]=v[a]; mx[g]=v[b-1];
    }
    auto t2=std::chrono::steady_clock::now();
    std::ofstream o(argv[3]); o<<"{\"rows\":["; bool first=true;
    for(uint64_t g=0;g<ng&&g<k;g++){ if(!cnt[g]) continue; if(!first) o<<","; first=false;
        o<<"[\""<<names[g]<<"\",\""<<(long long)sum[g]<<"\",\""<<cnt[g]<<"\",\""<<mn[g]<<"\",\""<<mx[g]<<"\"]"; }
    o<<"]}\n"; o.close(); auto t3=std::chrono::steady_clock::now();
    auto ms=[](auto x,auto y){return std::chrono::duration<double,std::milli>(y-x).count();};
    fprintf(stderr,"{\"setup_ms\":%.4f,\"query_ms\":%.4f,\"output_ms\":%.4f,\"stored_ms\":%.4f}\n",ms(t0,t1),ms(t1,t2),ms(t2,t3),ms(t0,t3));
}
