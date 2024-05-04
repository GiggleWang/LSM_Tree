#pragma once

#include "sstheader.h"
#include "bloomfilter.h"
#include "sstindex.h"
#include "config.h"
#include <list>
#include <map>
#include "utils.h"

class SSTable {
private:
    std::string path;
    uint32_t fileSize;
    /*
     * 0 表示Header是否被缓存
     * 1 表示BloomFilter是否被缓存
     * 2 表示SSTindex是否被缓存
     */
    bool cachePolicy[3];
    SSTheader *header = NULL;
    BloomFilter<uint64_t, sstable_bfSize> *bloomFilter = NULL;
    SSTindex *index = NULL;

    SSTheader *getHeaderPtr();

    BloomFilter<uint64_t, sstable_bfSize> *getBloomFilterPtr();

    SSTindex *getIndexPtr();

    void saveCachePolicy(bool (&saveTarget)[3]);

public:
    void refreshCachePolicy(bool setCachePolicy[3]);

    // 初始化一个SSTable, 从文件里面读取，并设置常态化缓存策略
    SSTable(std::string path, bool cachePolicy[3]);

    // 初始化一个SStable，从list读数据写入，完成之后立即落硬盘
    SSTable(uint64_t setTimeStamp,
            std::list<std::tuple<uint64_t, uint64_t, uint32_t>> &dataList,
            std::string setPath, bool cachePolicy[3]);

    ~SSTable();

    void clear();

    // 变量读取写在这里面
    uint64_t getSStableTimeStamp();

    uint64_t getSStableMinKey();

    uint64_t getSStableMaxKey();

    uint64_t getSStableKeyValNum();

    uint64_t getSStableKey(size_t index);

    uint64_t getSStableOffset(size_t index);

    uint32_t getSStableVlen(size_t index);

    uint64_t getKeyOrLargerIndexByKey(uint64_t key);

    void scan(uint64_t key1, uint64_t key2, std::map<uint64_t, std::map<uint64_t,std::map< uint64_t, uint32_t>>> &scanMap);

};