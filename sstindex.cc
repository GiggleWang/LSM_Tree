//sstindex.cc
#include "sstindex.h"

SSTindex::SSTindex(std::string path, uint32_t offset, size_t readKeyNum) {
    readFile(path, offset, readKeyNum);
}
int SSTindex::readFile(std::string path, uint32_t offset, size_t keyCount) {
    std::ifstream inputFile(path, std::ios::in | std::ios::binary);
    if (!inputFile)
        return -1;
    inputFile.seekg(0, std::ios::end);
    uint32_t fileSize = inputFile.tellg();
    if (offset > fileSize || offset + (sizeof(uint64_t) + sizeof(uint32_t)) * keyCount > fileSize) {
        inputFile.close();
        return -2;
    }
    inputFile.clear();
    inputFile.seekg(offset, std::ios::beg);
    for (size_t idx = 0; idx < keyCount; ++idx) {
        uint64_t key;
        uint64_t offsetRead;
        uint32_t valueLength;
        inputFile.read(reinterpret_cast<char*>(&key), sizeof(key));
        inputFile.read(reinterpret_cast<char*>(&offsetRead), sizeof(offsetRead));
        inputFile.read(reinterpret_cast<char*>(&valueLength), sizeof(valueLength));
        indexStruct indexEntry(key, offsetRead, valueLength);
        indexVec.push_back(indexEntry);
    }
    inputFile.close();
    return 0;
}

uint32_t SSTindex::writeToFile(std::string path, uint32_t offset) {
    bool fileExists = false;
    uint32_t fileSize = 0;
    std::ifstream inputFile(path, std::ios::in | std::ios::binary);
    if (inputFile) {
        inputFile.seekg(0, std::ios::end);
        fileSize = inputFile.tellg();
        inputFile.close();
        fileExists = true;
    }
    if (!fileExists) {
        offset = 0;
        std::fstream createFile(path, std::ios::out | std::ios::binary);
        createFile.close();
    }
    else if (fileExists && (offset > fileSize)) {
        offset = fileSize;
    }
    std::fstream outputFile(path, std::ios::out | std::ios::in | std::ios::binary);
    if (!outputFile)
        return -1;
    outputFile.seekp(offset, std::ios::beg);
    for (const auto& entry : indexVec) {
        outputFile.write(reinterpret_cast<const char*>(&entry.key), sizeof(uint64_t));
        outputFile.write(reinterpret_cast<const char*>(&entry.offset), sizeof(uint64_t));
        outputFile.write(reinterpret_cast<const char*>(&entry.vlen), sizeof(uint32_t));
    }
    outputFile.close();
    return offset;
}
void SSTindex::insert(uint64_t key, uint64_t offset, uint32_t vlen) {
    indexStruct newIndexEntry(key, offset, vlen);
    indexVec.push_back(newIndexEntry);
}

uint64_t SSTindex::getKeyByIndex(uint64_t idx) {
    if (idx >= indexVec.size()) {
        std::cerr << "Index out of range in function getKeyByIndex" << std::endl;
        return UINT64_MAX; // 返回一个显而易见的错误值
    }
    return indexVec[idx].key;
}

uint64_t SSTindex::getOffsetByIndex(uint64_t idx) {
    if (idx >= indexVec.size()) {
        std::cerr << "Index out of range in function getOffsetByIndex" << std::endl;
        return UINT64_MAX; // 返回一个显而易见的错误值
    }
    return indexVec[idx].offset;
}

uint32_t SSTindex::getVlenByIndex(uint64_t idx) {
    if (idx >= indexVec.size()) {
        std::cerr << "Index out of range in function getVlenByIndex" << std::endl;
        return UINT32_MAX; // 返回一个显而易见的错误值
    }
    return indexVec[idx].vlen;
}

/**
 * 查找给定键或更大的最小键的索引。
 *
 * @param key 要查找的键。
 * @return 如果找到确切的键，则返回其索引；如果未找到，返回更大的最小键的索引；
 *         如果所有键都小于给定键或向量为空，返回 UINT64_MAX。
 */
uint64_t SSTindex::getKeyOrLargerIndexByKey(uint64_t searchKey) {
    if (indexVec.empty())
        return UINT64_MAX;
    if (searchKey < indexVec[0].key)
        return 0;
    if (searchKey > indexVec.back().key)
        return UINT64_MAX;

    uint64_t left = 0;
    uint64_t right = indexVec.size() - 1;
    uint64_t mid;
    uint64_t foundIndex = 0;

    while (left <= right) {
        mid = left + ((right - left) / 2);
        if (indexVec[mid].key == searchKey) {
            foundIndex = mid;
            break;
        } else if (indexVec[mid].key > searchKey) {
            right = mid - 1;
        } else {
            left = mid + 1;
            foundIndex = left;
        }
    }
    return foundIndex;
}

uint64_t SSTindex::getKeyIndexByKey(uint64_t searchKey) {
    if (indexVec.empty())
        return UINT64_MAX;
    if (searchKey < indexVec[0].key)
        return UINT64_MAX;
    if (searchKey > indexVec.back().key)
        return UINT64_MAX;

    uint64_t left = 0;
    uint64_t right = indexVec.size() - 1;
    uint64_t mid;
    uint64_t foundIndex = 0;
    bool isFound = false;

    while (left <= right) {
        mid = left + ((right - left) / 2);
        if (indexVec[mid].key == searchKey) {
            foundIndex = mid;
            isFound = true;
            break;
        } else if (indexVec[mid].key > searchKey) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    return isFound ? foundIndex : UINT64_MAX;
}



