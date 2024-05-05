#pragma once

#include "kvstore_api.h"
#include "memtable.h"
#include "vlog.h"
#include "sstable.h"
#include "config.h"
#include <string>
#include <sstream>
#include <chrono>

class KVStore : public KVStoreAPI {
private:
    // 数据的目录
    std::string dataDir;
    // 保存每一层的sst文件的数量限制
    /*键是层的编号（从0开始，数字越大表示层越深），值是该层允许的最大SSTable数量。*/
    std::map<uint64_t, uint64_t> config_level_limit;
    // 保存每一层的类型，Tiering-0，Leveling-1
    std::map<uint64_t, bool> config_level_type;
    // 保存每一层的索引，ssTableIndex[level-i][写入的时间戳(微秒级)] = 指针
    /*键是层的编号，值是另一个映射，该映射的键是SSTable的写入时间戳，值是指向SSTable实例的指针*/
    std::map<uint64_t, std::map<uint64_t, SSTable *>> ssTableIndex;

    // 内存表
    MemTable *memTable;

    /*记录当前最大SSTable时间戳的变量*/
    uint64_t sstMaxTimeStamp = 0;

    //value存储vlog
    vLog *vlog;

    void readConfig(std::string path);

    void writeConfig(std::string path);

    void sstFileCheck(std::string dataPath);

public:
    KVStore(const std::string &dir, const std::string &vlog);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

    void gc(uint64_t chunk_size) override;

    int mergeCheck();

    void merge(uint64_t X);
};
