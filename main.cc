#include "kvstore.h"
/*
编译指令：
g++ -o test main.cc kvstore.cc memtable.cc sstable.cc vlog.cc
*/
int main()
{
    KVStore *store;
    store = new KVStore("./", "./");
    store->reset();
    std::cout<<store->get(1)<<std::endl;
    store->put(1, "SE");
    std::cout<<store->get(1)<<std::endl;
    delete store;
}