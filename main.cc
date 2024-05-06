#include "kvstore.h"

/*
编译指令：
g++ -o test main.cc kvstore.cc memtable.cc sstable.cc vlog.cc
*/

KVStore *store = new KVStore("../data", "../data/vlog");;

std::string vlog = "../data/vlog";
#define MB (1024 * 1024)
const int max = 1024 * 48;
uint64_t i;
uint64_t gc_trigger = 1024;
uint64_t last_offset, cur_offset;

void check_gc(uint64_t size) {
    last_offset = utils::seek_data_block(vlog.c_str());
    store->gc(size);
    cur_offset = utils::seek_data_block(vlog.c_str());
    if(last_offset+size>cur_offset)
    {
//        std::cout<<"last offset is "<<last_offset<<" ;\ncur offset is "<<cur_offset<<"\n";
        std::cout << "wrong at test 2 , index is " << i << std::endl;
    }
}

int main() {
    std::cout<<"               start reset...\n";
    store->reset();
    std::cout<<"               start put...\n";
    for (i = 0; i < max; ++i) {
        store->put(i, std::string(i + 1, 's'));
    }
    for (i = 0; i < max; i++) {
//        std::cout << "                   testing " << i << "..." << std::endl;
        if (store->get(i) != std::string(i + 1, 's')) {
            std::cout << "wrong at test 1 , index is " << i << std::endl;
        }

        switch (i % 3) {
            case 0:
                store->put(i, std::string(i + 1, 'e'));
                break;
            case 1:
                store->put(i, std::string(i + 1, '2'));
                break;
            case 2:
                store->put(i, std::string(i + 1, '3'));
                break;
            default:
                assert(0);
        }

        if (i % gc_trigger == 0) [[unlikely]] {
            check_gc(16*MB);
        }
    }
}