// Build BSBITS02 bitplanes directly from the encoded tree.
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
struct GH { uint64_t count, base, max; uint32_t bits, pad; };
int main(int argc,char**argv){
 if(argc<6){fprintf(stderr,"usage: ENCODED PREP GID VID OUT\n");return 2;}
 auto t0=std::chrono::steady_clock::now();
 std::ifstream bf(std::string(argv[2])+"/binding.txt",std::ios::binary); std::string binding(41,'\0'); bf.read(binding.data(),40); binding.resize(40);
 bool parsed=false; StreamTable t=stream_load(argv[1],binding,parsed); if(!parsed) throw std::runtime_error("stream");
 U gid=U(std::stoul(argv[3])), vid=U(std::stoul(argv[4])); const StreamColumn* g=nullptr; const StreamColumn* v=nullptr;
 for(auto&c:t.columns){if(c.id==gid)g=&c;if(c.id==vid)v=&c;} if(!g||!v) return 1;
 const uint64_t G=16; std::vector<std::vector<int64_t>> vals(G); Scratch gd,vd;
 for(U b=0;b<g->leaves.size();b++){U off=b*V,take=std::min<U>(V,g->n-off);stream_decode_block(*g,b,gd);stream_decode_block(*v,b,vd);for(U i=0;i<take;i++){if(!stream_valid_bit(g->valid,off+i))continue;U code=gd.d[i]-CODE_BIAS;if(code>=G)continue;vals[code].push_back(int64_t(vd.d[i])-int64_t(CODE_BIAS));}}
 std::ofstream o(argv[5],std::ios::binary);o.write("BSBITS03",8);o.write((char*)&G,8);uint64_t tot=g->n;o.write((char*)&tot,8);
 std::vector<GH> gh(G);
 for(int gi=0;gi<G;gi++){auto&a=vals[gi];if(a.empty()){gh[gi]={0,0,0,0,0};continue;}int64_t sb=*std::min_element(a.begin(),a.end());uint64_t base=uint64_t(sb);uint64_t mx=0;for(auto x:a)mx=std::max<uint64_t>(mx,uint64_t(x)-base);uint32_t bits=0;while(mx){bits++;mx>>=1;}int64_t mxv=sb; for(auto x:a) mxv=std::max<int64_t>(mxv,x); gh[gi]={a.size(),base,uint64_t(mxv),bits,0};}
 o.write((char*)gh.data(),G*sizeof(GH));
 for(int gi=0;gi<G;gi++){auto&a=vals[gi];size_t c=a.size(),words=(c+63)/64;std::vector<uint64_t> plane(words);for(uint32_t j=0;j<gh[gi].bits;j++){std::fill(plane.begin(),plane.end(),0);for(size_t i=0;i<c;i++){uint64_t d=uint64_t(a[i])-gh[gi].base;if((d>>j)&1)plane[i>>6]|=1ULL<<(i&63);}o.write((char*)plane.data(),words*8);}}
 o.close();auto t1=std::chrono::steady_clock::now();fprintf(stderr,"{\"build_ms\":%.3f}\n",std::chrono::duration<double,std::milli>(t1-t0).count());
}
