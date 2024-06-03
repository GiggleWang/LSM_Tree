//memtable.h
#pragma once
#include "skiplist.h"
#include "config.h"
#include "utils.h"
#include <map>
#include "fstream"
class MemTable
{
private:
    Skiplist<uint64_t, std::string> *skiplist;
    size_t sstSpaceSize;
    void writeLog(std::string path, operationID id, uint64_t key, std::string val);//
    void restoreFromLog(std::string path);
    void putKV(uint64_t key, const std::string &s);//
    bool delKV(uint64_t key);//
 

public:
    MemTable();//

    ~MemTable();//

    void put(uint64_t key, const std::string &s);//

    bool del(uint64_t key);//

    std::string get(uint64_t key);//

    void reset();//

    void scan(uint64_t key1, uint64_t key2, std::map<uint64_t, std::string> &resultMap);//

    bool putCheck(uint64_t key, const std::string &s);//

    void copyAll(std::list<std::pair<uint64_t, std::string> > &list);

    void tranverse(){this->skiplist->tranverse();}
};
