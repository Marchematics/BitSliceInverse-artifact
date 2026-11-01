#include "../src_patch_fusion/fullsql_reference.hpp"
#include "../src_fullsql_ig2_fast_20261029/streaming.hpp"
#include <chrono>
#include <cstdio>
#include <fstream>
int main(int argc,char**argv){
 std::ifstream bf(argv[1]); std::string b(41,'\0'); bf.read(b.data(),40); b.resize(40);
 auto t0=std::chrono::steady_clock::now(); bool p=false; auto t=stream_load(argv[2],b,p);
 auto t1=std::chrono::steady_clock::now(); printf("parsed=%d cols=%zu leaves0=%zu ms=%.3f\n",p,t.columns.size(),t.columns.empty()?0:t.columns[0].leaves.size(),std::chrono::duration<double,std::milli>(t1-t0).count());
}
