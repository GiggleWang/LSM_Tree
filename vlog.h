// vLog.h
#ifndef VLOG_H
#define VLOG_H

#include <string>
#include <cstdint>
#include <cstdint>
#include <iostream>

class vLog
{
private:
    //定义vLog entry的前两位是Magic
    const std::uint8_t MagicByte = 0xFF;
    std::string filename;
    uint64_t head;
    uint64_t tail;
    uint64_t getFileSizeInBits(const std::string& filePath);
    uint64_t findFirstValidDataPosition(const std::string& filePath);
public:
    vLog(const std::string &filename);
    ~vLog();
    uint64_t appendEntry(uint64_t key, const std::string &value);
};

#endif // VLOG_H
