#include "vlog.h"
#include "utils.h"
#include <fstream>

vLog::vLog(const std::string &filename) : filename(filename), head(0), tail(0) {
    std::fstream file(filename, std::ios::in | std::ios::out | std::ios::binary); // 尝试以读写模式打开文件
    if (!file.is_open()) { // 如果文件不存在或无法打开
//        std::cout<<"新建"<<std::endl;
        file.open(filename, std::ios::out | std::ios::binary); // 以写模式打开以创建文件
        file.close(); // 关闭文件流
        file.open(filename, std::ios::in | std::ios::out | std::ios::binary); // 重新以读写模式打开
    }

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open or create file: " + filename);
    }

    uint64_t fileLength = getFileSizeInByte(filename);
    if (fileLength == 0) {
//        std::cout << "New file created: " << filename << std::endl;
        head = fileLength;
        tail = findFirstValidDataPosition(filename);
//        std::cout << "head = " << head << " tail = " << tail << std::endl;
    } else {
        head = fileLength;
        tail = findFirstValidDataPosition(filename);
//        std::cout << "head = " << head << " tail = " << tail << std::endl;
    }
    file.close();  // Make sure to close the file
}


vLog::~vLog() {
}

uint64_t vLog::appendEntry(uint64_t key, const std::string &value) {
    std::ofstream file(filename, std::ios::binary | std::ios::app);

    uint64_t ret = getFileSizeInByte(filename);

//    if (ret == -1) {
//        return ret;
//    }

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


uint64_t vLog::getFileSizeInByte(const std::string &filePath) {
    std::ifstream file(filePath, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        return 0;  // If file cannot be opened or does not exist, return 0
    }
    return file.tellg();
}

uint64_t vLog::findFirstValidDataPosition(const std::string &filePath) {
    std::ifstream file(filePath, std::ios::binary);
    file.seekg(0, std::ios::beg);  // 将文件指针移动到文件的开头
    if (!file.is_open()) {
        std::cerr << "1无法打开文件：" << filePath << std::endl;
        return -1;
    }

    const uint8_t expectedMagic = MagicByte;
    uint16_t readChecksum, calculatedChecksum;
    uint64_t key;
    uint32_t vlen;
    uint64_t position = 0;
    uint8_t tempByte;

    while (file.read(reinterpret_cast<char *>(&tempByte), 1)) {
        if (tempByte == expectedMagic) {
            std::vector<unsigned char> buffer; // 重新初始化buffer
            if (!file.read(reinterpret_cast<char *>(&readChecksum), 2) ||
                !file.read(reinterpret_cast<char *>(&key), 8) ||
                !file.read(reinterpret_cast<char *>(&vlen), 4)) {
                break;
            }


            buffer.insert(buffer.end(), reinterpret_cast<unsigned char *>(&key),
                          reinterpret_cast<unsigned char *>(&key) + 8);
            buffer.insert(buffer.end(), reinterpret_cast<unsigned char *>(&vlen),
                          reinterpret_cast<unsigned char *>(&vlen) + 4);

            // 读取Value
            buffer.resize(12 + vlen);
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
    return getFileSizeInByte(filename);
}

void vLog::reset() {
    utils::rmfile(filename);
    std::fstream file(filename, std::ios::in | std::ios::out | std::ios::binary); // 尝试以读写模式打开文件
    if (!file.is_open()) { // 如果文件不存在或无法打开
//        std::cout<<"新建"<<std::endl;
        file.open(filename, std::ios::out | std::ios::binary); // 以写模式打开以创建文件
        file.close(); // 关闭文件流
        file.open(filename, std::ios::in | std::ios::out | std::ios::binary); // 重新以读写模式打开
    }
    head = 0;
    tail = 0;
}

std::string vLog::findValueByOffsetAndVlen(uint64_t offset, uint32_t vlen) {
    std::ifstream file(filename, std::ios::binary);  // 以二进制模式打开文件
    if (!file.is_open()) {
        std::cerr << "2无法打开文件：" << filename << std::endl;
        return file_cannot_open;  // 用于错误处理的字符串常量
    }

    // 移动到offset + 15位置
    file.seekg(offset + 15, std::ios::beg);
    if (file.fail()) {
        std::cerr << "定位文件位置失败" << std::endl;
        return "seek_error";
    }

    // 读取vlen个字节
    std::string data;
    data.resize(vlen);  // 预分配字符串大小
    file.read(&data[0], vlen);  // 读取数据
    if (file.fail() && !file.eof()) {
        std::cerr << "读取数据失败" << std::endl;
        return read_data_error;
    }

    file.close();  // 关闭文件
    return data;   // 返回读取到的数据
}

void vLog::refresh(uint64_t size) {
//    std::cout << "size = " << size << std::endl;
//    this->tail = this->findFirstValidDataPosition(this->filename);
//    std::cout << "tail = " << tail << std::endl;
    this->tail = size;
}

