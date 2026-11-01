#include <cstdio>
#include <cstdint>
#include <chrono>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
int main(int argc,char**argv){ if(argc<3)return 2; auto t0=std::chrono::steady_clock::now(); int fd=open(argv[1],O_RDONLY); struct stat st{}; fstat(fd,&st); const char* p=(const char*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0); close(fd); auto t1=std::chrono::steady_clock::now(); FILE* o=fopen(argv[2],"wb"); fwrite(p,1,st.st_size,o); fclose(o); auto t2=std::chrono::steady_clock::now(); auto ms=[](auto x,auto y){return std::chrono::duration<double,std::milli>(y-x).count();}; fprintf(stderr,"{\"read_ms\":%.3f,\"write_ms\":%.3f,\"stored_ms\":%.3f,\"bytes\":%lld}\n",ms(t0,t1),ms(t1,t2),ms(t0,t2),(long long)st.st_size);}
