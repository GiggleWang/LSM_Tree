#include "vlog.h"
#include "utils.h"
#include <fstream>

vLog::vLog(const std::string &filename)
    : filename(filename), head(0), tail(0)
{
}

vLog::~vLog()
{
}

/// @brief 执行vLog的插入操作，将在vLog文件中插入一个vlog_entry，并且更新head值
/// @param key 插入的键值对的键
/// @param value 插入键值对的值
void vLog::appendEntry(uint64_t key, const std::string &value)
{
    std::ofstream file(filename, std::ios::binary | std::ios::app);
    if (!file)
    {
        throw std::runtime_error("无法打开vLog文件进行写入。");
    }

    // Magic byte
    const std::uint8_t Magic = this->MagicByte;
    file.write(reinterpret_cast<const char *>(&Magic), sizeof(Magic));

    // Prepare the data for CRC computation
    std::vector<unsigned char> dataForCrc;
    
    // Insert the key bytes
    for (size_t i = 0; i < sizeof(key); ++i) {
        dataForCrc.push_back((key >> (i * 8)) & 0xFF);
    }

    // Insert the value length as bytes
    std::uint32_t vlen = static_cast<std::uint32_t>(value.size());
    for (size_t i = 0; i < sizeof(vlen); ++i) {
        dataForCrc.push_back((vlen >> (i * 8)) & 0xFF);
    }
    
    // Insert the value bytes
    dataForCrc.insert(dataForCrc.end(), value.begin(), value.end());

    // Calculate CRC
    std::uint16_t checksum = utils::crc16(dataForCrc);
    file.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));

    // Write the key
    file.write(reinterpret_cast<const char*>(&key), sizeof(key));

    // Write the length of the value
    file.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));

    // Write the value
    file.write(value.data(), value.size());

    if (!file) {
        throw std::runtime_error("写入vLog文件时发生错误。");
    }

    // Update the head pointer
    head = file.tellp();

    // Close the file
    file.close();
}

/// @brief 提取vLog中的head值
/// @return vLog中的head值
uint64_t vLog::getHead()
{
    return head;
}