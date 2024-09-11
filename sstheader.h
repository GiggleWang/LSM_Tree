//ssheader.h
#pragma once
#include <string>
#include <iostream>
#include <fstream>
#include <cstdint>
class SSTheader
{
public:
    uint64_t timeStamp;  // 时间戳的序列号
    uint64_t keyValNum;  // 键值对的数量
    uint64_t minKey;     // 最小的键
    uint64_t maxKey;     // 最大的键
    int readFile(std::string path, uint32_t offset);
    uint32_t writeToFile(std::string path, uint32_t offset);
    SSTheader(){}
    ~SSTheader(){}
    SSTheader(std::string path, uint32_t offset);
};
