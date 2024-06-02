//sstable.cc
#include "sstable.h"


SSTheader *SSTable::getHeaderPtr() {
    this->cachePolicy[0] = true;
    if (this->header != nullptr) {
        return this->header;
    }
    auto tempHeader = new SSTheader(this->path, sstable_fileOffset_header);
    this->header = tempHeader;
    return this->header;
}


BloomFilter<uint64_t, 8192> *SSTable::getBloomFilterPtr() {
    this->cachePolicy[1] = true;
    if (this->bloomFilter != nullptr) {
        return this->bloomFilter;
    }
    auto tempBloomFilter = new BloomFilter<uint64_t, sstable_bfSize>(this->path, sstable_fileOffset_bf);
    this->bloomFilter = tempBloomFilter;

    return this->bloomFilter;
}

SSTindex *SSTable::getIndexPtr() {
    this->cachePolicy[2] = true;
    if (this->index != nullptr) {
        return this->index;
    }

    // 警告，这里不需要保存，否则反而可能死循环
    uint64_t kvNum = (this->getHeaderPtr())->keyValNum;

    auto tempIndex = new SSTindex(this->path, sstable_fileOffset_key, kvNum);
    this->index = tempIndex;
    return this->index;
}

void SSTable::saveCachePolicy(bool (&target)[3]) {
    for (int idx = 0; idx < 3; ++idx) {
        target[idx] = this->cachePolicy[idx];
    }
}

void SSTable::refreshCachePolicy(bool setCachePolicy[3]) {
    // ************ Header ************
    if (!setCachePolicy[0]) {
        if (this->header != nullptr) {
            delete this->header;
            this->header = nullptr;
        }
        this->cachePolicy[0] = false;
    } else {
        // 只有当header为null的时候才会尝试调用
        if (this->header == nullptr) {
            getHeaderPtr();
        }
    }

    // ************ Bloom Filter ************
    if (!setCachePolicy[1]) {
        if (this->bloomFilter != nullptr) {
            delete this->bloomFilter;
            this->bloomFilter = nullptr;
        }
    } else {
        // 只有当Bloom Filter为null的时候才会尝试调用
        if (this->bloomFilter == nullptr) {
            getBloomFilterPtr();
        }
    }

    // ************ Index ************
    if (!setCachePolicy[2]) {
        if (this->index != nullptr) {
            delete this->index;
            this->index = nullptr;
        }
    } else {
        if (this->index == nullptr) {
            getIndexPtr();
        }
    }
}


SSTable::SSTable(std::string filePath, bool initialCachePolicy[3]) {
    this->path = filePath;
    this->bloomFilter = nullptr;
    this->index = nullptr;
    this->header = nullptr;
    bool defaultReadPolicy[3] = {true, true, true};
    this->refreshCachePolicy(defaultReadPolicy);
    std::ifstream inputFile(filePath, std::ios::in | std::ios::binary);
    if (inputFile) {
        inputFile.seekg(0, std::ios::end);
        this->fileSize = inputFile.tellg();
        inputFile.close();
    }
    this->refreshCachePolicy(initialCachePolicy);
}


SSTable::SSTable(uint64_t timestamp, std::list<std::tuple<uint64_t, uint64_t, uint32_t>> &keyValueList,
                 std::string filePath, bool initialCachePolicy[3]) {
    /*
     * 对于keyValueList里面的每个元素
     * 第一个元素是key
     * 第二个是offset
     * 第三个是vlen
     */
    this->path = filePath;
    this->bloomFilter = new BloomFilter<uint64_t, sstable_bfSize>();
    this->index = new SSTindex();
    this->header = new SSTheader();
    uint64_t minKey = UINT64_MAX;
    uint64_t maxKey = 0;

    for (const auto &kvPair : keyValueList) {
        uint64_t key = std::get<0>(kvPair);
        uint64_t offset = std::get<1>(kvPair);
        uint32_t valueLength = std::get<2>(kvPair);
        minKey = std::min(minKey, key);
        maxKey = std::max(maxKey, key);
        bloomFilter->insert(key);
        index->insert(key, offset, valueLength);
    }
    header->timeStamp = timestamp;
    header->minKey = minKey;
    header->maxKey = maxKey;
    header->keyValNum = keyValueList.size();

    header->writeToFile(filePath, sstable_fileOffset_header);
    bloomFilter->writeToFile(filePath, sstable_fileOffset_bf);
    index->writeToFile(filePath, sstable_fileOffset_key);
    this->refreshCachePolicy(initialCachePolicy);
    std::ifstream inputFile(filePath, std::ios::in | std::ios::binary);

    if (inputFile) {
        inputFile.seekg(0, std::ios::end);
        this->fileSize = inputFile.tellg();
        inputFile.close();
    }
}


SSTable::~SSTable() {
    bool policy[3] = {false, false, false};
    refreshCachePolicy(policy);
}

void SSTable::clear() {
    utils::rmfile(path);
}
uint64_t SSTable::getSStableTimeStamp() {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);
    uint64_t timeStamp = this->getHeaderPtr()->timeStamp;
    this->refreshCachePolicy(currentCachePolicy);
    return timeStamp;
}

uint64_t SSTable::getSStableMinKey() {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);
    uint64_t minKey = this->getHeaderPtr()->minKey;
    this->refreshCachePolicy(currentCachePolicy);
    return minKey;
}

uint64_t SSTable::getSStableMaxKey() {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);
    uint64_t maxKey = this->getHeaderPtr()->maxKey;
    this->refreshCachePolicy(currentCachePolicy);
    return maxKey;
}
uint64_t SSTable::getSStableKeyValNum() {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);
    uint64_t keyValueCount = this->getHeaderPtr()->keyValNum;
    this->refreshCachePolicy(currentCachePolicy);
    return keyValueCount;
}

uint64_t SSTable::getSStableKey(size_t idx) {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);
    uint64_t keyValue = UINT64_MAX;
    if (idx >= this->getHeaderPtr()->keyValNum) {
        this->refreshCachePolicy(currentCachePolicy);
        return UINT64_MAX;
    } else {
        keyValue = this->getIndexPtr()->getKeyByIndex(idx);
    }
    this->refreshCachePolicy(currentCachePolicy);
    return keyValue;
}

uint64_t SSTable::getSStableOffset(size_t idx) {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);
    if (idx >= this->getHeaderPtr()->keyValNum) {
        this->refreshCachePolicy(currentCachePolicy);
        return UINT64_MAX;
    }
    uint64_t offset = this->getIndexPtr()->getOffsetByIndex(idx);
    this->refreshCachePolicy(currentCachePolicy);
    return offset;
}

uint32_t SSTable::getSStableVlen(size_t idx) {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);
    if (idx >= this->getHeaderPtr()->keyValNum) {
        this->refreshCachePolicy(currentCachePolicy);
        return UINT32_MAX;
    }
    uint32_t valueLength = this->getIndexPtr()->getVlenByIndex(idx);
    this->refreshCachePolicy(currentCachePolicy);
    return valueLength;
}

uint64_t SSTable::getKeyOrLargerIndexByKey(uint64_t key) {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);
    uint64_t resultIndex = getIndexPtr()->getKeyOrLargerIndexByKey(key);
    this->refreshCachePolicy(currentCachePolicy);
    return resultIndex;
}

void SSTable::scan(uint64_t keyStart, uint64_t keyEnd,
                   std::map<uint64_t, std::map<uint64_t, std::map<uint64_t, uint32_t>>> &resultScanMap) {
/*
 * resultScanMap的结构如下：
 * <key,<timestamp,<offset,vlen>>>
 */

    uint64_t startKeyIndex = this->getKeyOrLargerIndexByKey(keyStart);
    // 比最大的还要大，或者这个SST没有数据，不处理
    if (startKeyIndex == UINT64_MAX)
        return;

    // 写入当前的时间戳 key
    uint64_t currentTimestamp = this->getSStableTimeStamp();
    for (size_t idx = startKeyIndex; idx < this->getSStableKeyValNum(); ++idx) {
        uint64_t currentKey = getSStableKey(idx);
        if (currentKey > keyEnd)
            break;
        uint64_t offset = getSStableOffset(idx);
        uint32_t valueLength = getSStableVlen(idx);
        resultScanMap[currentKey][currentTimestamp][offset] = valueLength;
    }
}
bool SSTable::checkIfKeyExist(uint64_t searchKey) {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);

    if (searchKey > getHeaderPtr()->maxKey || searchKey < getHeaderPtr()->minKey) {
        this->refreshCachePolicy(currentCachePolicy);
        return false;
    }

    if (this->cachePolicy[1]) {
        bool existsInBloomFilter = getBloomFilterPtr()->find(searchKey);
        this->refreshCachePolicy(currentCachePolicy);
        return existsInBloomFilter;
    }

    this->refreshCachePolicy(currentCachePolicy);
    return true;
}
uint64_t SSTable::getKeyIndexByKey(uint64_t searchKey) {
    bool currentCachePolicy[3];
    this->saveCachePolicy(currentCachePolicy);
    uint64_t keyIndex = getIndexPtr()->getKeyIndexByKey(searchKey);
    this->refreshCachePolicy(currentCachePolicy);
    return keyIndex;
}


