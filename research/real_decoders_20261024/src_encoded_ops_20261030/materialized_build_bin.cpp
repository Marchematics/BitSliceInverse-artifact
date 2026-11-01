#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <vector>
int main(int argc,char**argv){ if(argc<3)return 2; auto t0=std::chrono::steady_clock::now(); std::ifstream f(argv[1],std::ios::binary), g(argv[2],std::ios::binary); f.seekg(0,std::ios::end); size_t n=f.tellg()/4; f.seekg(0); g.seekg(0); std::vector<int32_t> fare(n),gid(n); f.read((char*)fare.data(),n*4); g.read((char*)gid.data(),n*4); uint32_t G=0; for(auto x:gid) if(x>=0 && uint32_t(x)+1>G) G=uint32_t(x)+1; std::vector<int64_t> sum(G,0),cnt(G,0),mn(G,INT64_MAX),mx(G,INT64_MIN); for(size_t i=0;i<n;i++){int gi=gid[i]; if(gi<0)continue; sum[gi]+=fare[i];cnt[gi]++; if(fare[i]<mn[gi])mn[gi]=fare[i]; if(fare[i]>mx[gi])mx[gi]=fare[i];} FILE* o=fopen(argv[3],"wb"); for(uint32_t gi=0;gi<G;gi++){struct {uint32_t g;int32_t pad;int64_t s,c;int32_t a,b;} rec{gi,0,sum[gi],cnt[gi],int32_t(mn[gi]),int32_t(mx[gi])}; fwrite(&rec,sizeof(rec),1,o);} fclose(o); auto t1=std::chrono::steady_clock::now(); fprintf(stderr,"{\"build_ms\":%.3f,\"groups\":%u,\"bytes\":%llu}\n",std::chrono::duration<double,std::milli>(t1-t0).count(),G,(unsigned long long)(G*sizeof(long long)*2+G*sizeof(uint32_t)*3));}
