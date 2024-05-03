// vLog.h
#ifndef VLOG_H
#define VLOG_H

#include <string>
#include <cstdint>
#include <cstdint>

class vLog
{
public:
    vLog(const std::string &filename);
    ~vLog();
    uint64_t getHead();
    void appendEntry(uint64_t key, const std::string &value);
private:
    //定义vLog entry的前两位是Magic
    const std::uint8_t MagicByte = 0xFF;
    std::string filename;
    uint64_t head;
    uint64_t tail;
};

#endif // VLOG_H
