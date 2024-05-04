#include "sstindex.h"

SSTindex::SSTindex(std::string path, uint32_t offset, size_t readKeyNum) {
    readFile(path, offset, readKeyNum);
}

int SSTindex::readFile(std::string path, uint32_t offset, size_t readKeyNum) {
    std::ifstream inFile(path, std::ios::in | std::ios::binary);
    if (!inFile)
        return -1;

    // 文件指针移动到末尾
    inFile.seekg(0, std::ios::end);
    uint32_t fileLimit = inFile.tellg();

    // 判断是否超过限度 读取起点越界或者终点越界，都会返回-1
    if (offset > fileLimit || offset + (sizeof(uint64_t) + sizeof(uint32_t)) * readKeyNum > fileLimit) {
        inFile.close();
        return -2;
    }

    inFile.clear();
    // 通过检查，文件指针移动到偏移量
    inFile.seekg(offset, std::ios::beg);

    for (size_t i = 0; i < readKeyNum; i++) {
        uint64_t keyread;
        uint64_t offsetread;
        uint32_t vlenread;
        inFile.read((char *) &keyread, sizeof(keyread));
        inFile.read((char *) &offsetread, sizeof(offsetread));
        inFile.read((char *) &vlenread, sizeof(vlenread));
        indexStruct indexstr(keyread, offsetread, vlenread);
        indexVec.push_back(indexstr);
    }

    inFile.close();
    return 0;
}

uint32_t SSTindex::writeToFile(std::string path, uint32_t offset) {
    // 判断文件是否存在，用尝试读取的方式打开，如果打开成功就认为文件存在
    bool ifFileExist = false;
    uint32_t fileLimit = 0;
    std::ifstream inFile(path, std::ios::in | std::ios::binary);

    if (inFile) {
        // 打开成功，获取文件大小
        inFile.seekg(0, std::ios::end);
        fileLimit = inFile.tellg();
        inFile.close();
        ifFileExist = true;
    }

    // 文件不存在
    if (!ifFileExist) {
        offset = 0;
        // 创建文件
        std::fstream createFile(path, std::ios::out | std::ios::binary);
        createFile.close();
    }
        // 文件存在且用户设置的偏移量大于文件最大的范围，以追加形式写入文件
    else if (ifFileExist && (offset > fileLimit)) {
        offset = fileLimit;
    }

    std::fstream outFile(path, std::ios::out | std::ios::in | std::ios::binary);

    if (!outFile)
        return -1;

    // 把文件指针移动到偏移量
    outFile.seekp(offset, std::ios::beg);

    // 开始写入文件
    for (int i = 0; i < indexVec.size(); ++i) {
        outFile.write((char *) &(indexVec[i].key), sizeof(uint64_t));
        outFile.write((char *) &(indexVec[i].offset), sizeof(uint64_t));
        outFile.write((char *) &(indexVec[i].vlen), sizeof(uint32_t));
    }
    outFile.close();
    return offset;
}

void SSTindex::insert(uint64_t newKey, uint64_t newOffset, uint32_t newVlen) {
    indexStruct indexstr(newKey, newOffset, newVlen);
    indexVec.push_back(indexstr);
}

uint64_t SSTindex::getKeyByIndex(uint64_t index) {
    if (index > indexVec.size()) {
        std::cout << "index out of size in function getKeyByIndex" << std::endl;
    }
    return indexVec[index].key;
}

uint64_t SSTindex::getOffsetByIndex(uint64_t index) {
    if (index > indexVec.size()) {
        std::cout << "index out of size in function getOffsetByIndex" << std::endl;
    }
    return indexVec[index].offset;
}

uint32_t SSTindex::getVlenByIndex(uint64_t index) {
    if (index > indexVec.size()) {
        std::cout << "index out of size in function getVlenByIndex" << std::endl;
    }
    return indexVec[index].vlen;
}


/**
 * 查找给定键或更大的最小键的索引。
 *
 * @param key 要查找的键。
 * @return 如果找到确切的键，则返回其索引；如果未找到，返回更大的最小键的索引；
 *         如果所有键都小于给定键或向量为空，返回 UINT64_MAX。
 */
uint64_t SSTindex::getKeyOrLargerIndexByKey(uint64_t key) {
    if (indexVec.size() == 0)
        return UINT64_MAX;
    if (key < indexVec[0].key)
        return 0;
    if (key > indexVec[indexVec.size() - 1].key)
        return UINT64_MAX;
    uint64_t left = 0;
    uint64_t right =  indexVec.size() - 1;
    uint64_t mid;
    uint64_t findIndex = 0;
    while(left <= right){
        mid = left + ((right - left) / 2);
        if(indexVec[mid].key == key){
            findIndex = mid;
            break;
        }
        else if(indexVec[mid].key > key){
            right = mid - 1;
        }
        else if(indexVec[mid].key < key){
            left = mid + 1;
            findIndex = left;
        }
    }
    return findIndex;
}


