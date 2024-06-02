//sstindex.h
#ifndef LSM_TREE_SSTINDEX_H
#define LSM_TREE_SSTINDEX_H

#include <cstdint>
#include "iostream"
#include "vector"
#include <fstream>

/*
 * 存储索引的一个元
 */
struct indexStruct {
    uint64_t key;//键
    uint64_t offset;//key对应的value在vLog文件中的偏移量
    uint32_t vlen;//value的长度
    indexStruct(uint64_t k, uint64_t o, uint32_t v) {
        key = k;
        offset = o;
        vlen = v;
    }
};

//SST的索引区域
class SSTindex {
private:
    std::vector<indexStruct> indexVec;
public:
    SSTindex() {};

    ~SSTindex() {};

    SSTindex(std::string path, uint32_t offset, size_t readKeyNum);

    int readFile(std::string path, uint32_t offset, size_t readKeyNum);

    uint32_t writeToFile(std::string path, uint32_t offset);

    uint64_t getKeyNum() { return indexVec.size(); }

    void insert(uint64_t newKey, uint64_t newOffset, uint32_t newVlen);

    uint64_t getKeyByIndex(uint64_t index);

    uint64_t getOffsetByIndex(uint64_t index);

    uint32_t getVlenByIndex(uint64_t index);

    uint64_t getKeyOrLargerIndexByKey(uint64_t key);

    uint64_t getKeyIndexByKey(uint64_t key);
};


#endif //LSM_TREE_SSTINDEX_H
