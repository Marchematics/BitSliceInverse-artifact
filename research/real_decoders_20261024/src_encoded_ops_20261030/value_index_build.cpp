// Build BSVIDX02 sparse-prefix value index directly from an encoded tree.
#include "../src_patch_fusion/fullsql_reference.hpp"
#include "../src_fullsql_ig2_fast_20261029/streaming.hpp"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>
static constexpr U CODE_BIAS = 0x80000000U;
static U dict_count(const std::string& p) {
    std::ifstream f(p, std::ios::binary); std::string d((std::istreambuf_iterator<char>(f)), {});
    if (d.size() < 52 || d.compare(0,8,"BSSTR01\n")) throw std::runtime_error("dict");
    U k; std::memcpy(&k, d.data()+48, 4); return k;
}
int main(int argc,char**argv){
    if(argc<6){fprintf(stderr,"usage: build ENCODED PREP GID VID OUT\n");return 2;}
    auto t0=std::chrono::steady_clock::now();
    std::string prep=argv[2]; U gid=U(std::stoul(argv[3])), vid=U(std::stoul(argv[4]));
    std::ifstream bf(prep+"/binding.txt",std::ios::binary); std::string binding(41,'\0'); bf.read(binding.data(),40); binding.resize(40);
    bool parsed=false; StreamTable t=stream_load(argv[1],binding,parsed);
    if(!parsed) throw std::runtime_error("stream admission");
    const StreamColumn* g=nullptr; const StreamColumn* v=nullptr;
    for(auto&c:t.columns){if(c.id==gid)g=&c;if(c.id==vid)v=&c;}
    if(!g||!v) throw std::runtime_error("columns");
    U k=dict_count(prep+"/state.dict");
    std::vector<std::vector<int32_t>> vals(k);
    Scratch gd,vd;
    for(U b=0;b<g->leaves.size();++b){
        U off=b*V; U take=std::min<U>(V,g->n-off);
        stream_decode_block(*g,b,gd); stream_decode_block(*v,b,vd);
        for(U i=0;i<take;i++){
            if(!stream_valid_bit(g->valid,off+i)) continue;
            U code=gd.d[i]-CODE_BIAS; if(code>=k) throw std::runtime_error("group domain");
            int64_t val=int64_t(vd.d[i])-int64_t(CODE_BIAS); vals[code].push_back(int32_t(val));
        }
    }
    auto t1=std::chrono::steady_clock::now();
    std::vector<uint64_t> counts(k); for(U gidx=0;gidx<k;gidx++) counts[gidx]=vals[gidx].size();
    std::ofstream o(argv[5],std::ios::binary); o.write("BSVIDX02",8);
    uint64_t kk=k, nn=g->n; o.write((char*)&kk,8); o.write((char*)&nn,8); o.write((char*)counts.data(),k*8);
    for(U gidx=0;gidx<k;gidx++){
        auto& a=vals[gidx]; std::sort(a.begin(),a.end());
        o.write((char*)a.data(),a.size()*4);
        uint64_t blocks=a.size()/64+1; std::vector<int64_t> pb(blocks,0);
        int64_t s=0; for(size_t j=0;j<a.size();++j){ if(j && j%64==0) pb[j/64]=s; s+=a[j]; }
        if(a.size()%64==0) pb[a.size()/64]=s;
        // pb[b] must equal sum of first 64*b values
        o.write((char*)pb.data(),pb.size()*8);
    }
    o.close(); auto t2=std::chrono::steady_clock::now();
    auto ms=[](auto x,auto y){return std::chrono::duration<double,std::milli>(y-x).count();};
    fprintf(stderr,"{\"decode_ms\":%.3f,\"write_ms\":%.3f,\"groups\":%u,\"rows\":%llu}\n",ms(t0,t1),ms(t1,t2),k,(unsigned long long)nn);
}
