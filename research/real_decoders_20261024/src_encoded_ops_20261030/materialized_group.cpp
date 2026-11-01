// Materialized group aggregate query: read a small group summary and emit all-four rows.
#include <chrono>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
int main(int argc,char**argv){
 if(argc<3)return 2; auto t0=std::chrono::steady_clock::now();
 std::ifstream f(argv[1]); std::string line; std::getline(f,line);
 FILE* o=fopen(argv[2],"w"); fprintf(o,"{\"rows\":["); bool first=true;
 while(std::getline(f,line)){ if(line.empty())continue; std::stringstream ss(line); std::string n,s,c,mn,mx;
  std::getline(ss,n,',');std::getline(ss,s,',');std::getline(ss,c,',');std::getline(ss,mn,',');std::getline(ss,mx,',');
  if(n.empty())continue; if(!first)fprintf(o,","); first=false; fprintf(o,"[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"]",n.c_str(),s.c_str(),c.c_str(),mn.c_str(),mx.c_str()); }
 fprintf(o,"]}\n"); fclose(o); auto t1=std::chrono::steady_clock::now();
 fprintf(stderr,"{\"stored_ms\":%.4f}\n",std::chrono::duration<double,std::milli>(t1-t0).count()); }
