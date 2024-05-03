#pragma once
#include <bitset>
#include <string>
#include <iostream>
#include <fstream>
#include "MurmurHash3.h" // 确保有MurmurHash3的实现

// 布隆过滤器的大小，单位是字节
constexpr size_t BloomFilterSize = 8192;

// K 是键的类型
template <typename K>
class BloomFilter
{
private:
    // 用于存储的位图，大小为8kB * 8位 = 65536位
    std::bitset<BloomFilterSize * 8> bloomFilterData;

public:
    // 插入元素
    void insert(const K &key);
    // 检查元素是否存在
    bool find(const K &key) const;

    // 读取文件，从文件获取数据（path是路径，offset是文件偏移量，单位是Byte）
    int readFile(std::string path, uint32_t offset);
    // 写入文件，把数据写入到文件（path是路径，offset是文件偏移量，单位是Byte）
    uint32_t writeToFile(std::string path, uint32_t offset);

    // 构造函数和析构函数
    BloomFilter() {}
    ~BloomFilter() {}

    BloomFilter(const std::string &path, uint32_t offset);
};

template <typename K>
BloomFilter<K>::BloomFilter(const std::string &path, uint32_t offset)
{
    readFile(path, offset);
}

template <typename K>
void BloomFilter<K>::insert(const K &key)
{
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (int i = 0; i < 4; ++i)
    {
        // 取余保证结果落在布隆过滤器大小内
        bloomFilterData.set(hash[i] % (BloomFilterSize * 8), true);
    }
}

template <typename K>
bool BloomFilter<K>::find(const K &key) const
{
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (int i = 0; i < 4; ++i)
    {
        if (!bloomFilterData.test(hash[i] % (BloomFilterSize * 8)))
        {
            return false; // 任一位不为1则表示不可能存在
        }
    }
    return true; // 所有位都为1可能存在
}


template<typename K>
int BloomFilter<K>::readFile(std::string path, uint32_t offset) {
    std::ifstream inFile(path, std::ios::in | std::ios::binary);
    if (!inFile) {
        return -1;
    }

    // 文件指针移动到末尾
    inFile.seekg(0, std::ios::end);
    uint32_t fileLimit = static_cast<uint32_t>(inFile.tellg());

    // 判断是否超过限度
    if (offset > fileLimit || BloomFilterSize + offset > fileLimit) {
        inFile.close();
        return -2;
    }

    inFile.clear();
    inFile.seekg(offset, std::ios::beg);

    // 创建一个临时的字符缓冲区用于读取位图数据
    std::vector<char> buffer(BloomFilterSize, 0);
    inFile.read(buffer.data(), BloomFilterSize);

    // 将缓冲区的内容加载到bloomFilterData中
    for (size_t i = 0; i < buffer.size(); ++i) {
        for (int j = 0; j < 8; ++j) {
            if (buffer[i] & (1 << j)) {
                bloomFilterData.set(i * 8 + j);
            }
        }
    }

    inFile.close();
    return 0;
}

template<typename K>
uint32_t BloomFilter<K>::writeToFile(std::string path, uint32_t offset) {
    // 判断文件是否存在，用尝试读取的方式打开，如果打开成功就认为文件存在
    bool ifFileExist = false;
    std::ifstream inFile(path, std::ios::in | std::ios::binary);
    if (inFile) {
        // 打开成功，获取文件大小
        inFile.seekg(0, std::ios::end);
        ifFileExist = true;
        inFile.close();
    }

    // 如果文件不存在或用户设置的偏移量超出文件大小，重置偏移量
    std::fstream outFile;
    if (!ifFileExist || offset > BloomFilterSize) {
        outFile.open(path, std::ios::out | std::ios::binary);
        offset = 0;
    } else {
        outFile.open(path, std::ios::out | std::ios::in | std::ios::binary);
    }

    if (!outFile) {
        return -1;
    }

    // 文件指针移动到偏移量
    outFile.seekp(offset, std::ios::beg);

    // 创建一个临时的字符缓冲区用于写入位图数据
    std::vector<char> buffer(BloomFilterSize, 0);
    for (size_t i = 0; i < BloomFilterSize * 8; ++i) {
        if (bloomFilterData.test(i)) {
            buffer[i / 8] |= (1 << (i % 8));
        }
    }

    // 写入位图数据到文件
    outFile.write(buffer.data(), BloomFilterSize);
    outFile.close();

    return offset;
}