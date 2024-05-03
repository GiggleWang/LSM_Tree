#include "vlog.h"
#include "utils.h"
#include <fstream>

vLog::vLog(const std::string &filename)
        : filename(filename), head(0), tail(0) {
    uint64_t fileLength = getFileSizeInBits(filename);
    if (fileLength > 0) {
        head = fileLength;
        //寻找tail值
        tail = findFirstValidDataPosition(filename);
        if (tail < 0) {
            tail = head;
        }
    }
}

vLog::~vLog() {
}

uint64_t vLog::appendEntry(uint64_t key, const std::string &value) {
    std::ofstream file(filename, std::ios::binary | std::ios::app);

    uint64_t ret = getFileSizeInBits(filename);

    if (ret == -1) {
        return ret;
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
    file.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

    // Write the key
    file.write(reinterpret_cast<const char *>(&key), sizeof(key));

    // Write the length of the value
    file.write(reinterpret_cast<const char *>(&vlen), sizeof(vlen));

    // Write the value
    file.write(value.data(), value.size());

    if (!file) {
        throw std::runtime_error("写入vLog文件时发生错误。");
    }

    // Update the head pointer
    head = file.tellp();
    // Close the file
    file.close();

    return ret;
}


uint64_t vLog::getFileSizeInBits(const std::string &filePath) {
    std::ifstream file(filePath, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "无法打开文件：" << filePath << std::endl;
        return -1; // 如果文件无法打开，返回错误值 -1
    }

    // 获取文件大小（字节单位）
    std::streamsize fileSize = file.tellg();
    file.close(); // 关闭文件流

    // 计算并返回文件大小（位单位）
    return fileSize * 8;
}

uint64_t vLog::findFirstValidDataPosition(const std::string &filePath) {
    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "无法打开文件：" << filePath << std::endl;
        return -1;
    }

    const uint8_t expectedMagic = 0xff;
    uint16_t readChecksum, calculatedChecksum;
    uint64_t key;
    uint32_t vlen;
    uint64_t position = 0;
    uint8_t tempByte;

    while (file.read(reinterpret_cast<char *>(&tempByte), 1)) {
        if (tempByte == expectedMagic) {
            std::vector<unsigned char> buffer; // 重新初始化buffer
            buffer.push_back(tempByte); // 加入Magic byte
            if (!file.read(reinterpret_cast<char *>(&readChecksum), 2) ||
                !file.read(reinterpret_cast<char *>(&key), 8) ||
                !file.read(reinterpret_cast<char *>(&vlen), 4)) {
                break;
            }

            // 加入Key和vlen
            buffer.insert(buffer.end(), reinterpret_cast<unsigned char *>(&readChecksum),
                          reinterpret_cast<unsigned char *>(&readChecksum) + 2);
            buffer.insert(buffer.end(), reinterpret_cast<unsigned char *>(&key),
                          reinterpret_cast<unsigned char *>(&key) + 8);
            buffer.insert(buffer.end(), reinterpret_cast<unsigned char *>(&vlen),
                          reinterpret_cast<unsigned char *>(&vlen) + 4);

            // 读取Value
            buffer.resize(buffer.size() + vlen);
            if (!file.read(reinterpret_cast<char *>(buffer.data() + buffer.size() - vlen), vlen)) {
                break;
            }

            // 计算校验和
            calculatedChecksum = utils::crc16(buffer);
            if (readChecksum == calculatedChecksum) {
                return position;
            }
        }
        position++;
        file.seekg(position);
    }

    file.close();
    return -1;
}
