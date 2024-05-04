
#include "sstable.h"


SSTheader *SSTable::getHeaderPtr() {
    this->cachePolicy[0] = true;
    if (this->header != NULL) {
        return this->header;
    }
    this->header = new SSTheader(this->path, sstable_fileOffset_header);
    return this->header;
}


BloomFilter<uint64_t, 8192> *SSTable::getBloomFilterPtr() {
    this->cachePolicy[1] = true;
    if (this->bloomFilter != NULL)
        return this->bloomFilter;
    this->bloomFilter = new BloomFilter<uint64_t, sstable_bfSize>(this->path, sstable_fileOffset_bf);

    return this->bloomFilter;
}

SSTindex *SSTable::getIndexPtr() {
    this->cachePolicy[2] = true;
    if (this->index != NULL)
        return this->index;

    // 警告，这里不需要保存，否则反而可能死循环
    uint64_t kvNum = (this->getHeaderPtr())->keyValNum;


    this->index = new SSTindex(this->path, sstable_fileOffset_key, kvNum);
    return this->index;
}

void SSTable::saveCachePolicy(bool (&saveTarget)[3]) {
    for (int i = 0; i < 3; i++) {
        saveTarget[i] = cachePolicy[i];
    }
}

void SSTable::refreshCachePolicy(bool setCachePolicy[3]) {
    // ************ Header ************
    if (setCachePolicy[0] == false) {
        if (this->header != NULL) {
            delete this->header;
            this->header = NULL;
        }
        this->cachePolicy[0] = false;
    } else {
        // 只有当header为null的时候才会尝试调用
        if (this->header == NULL)
            getHeaderPtr();
    }


    // ************ BF ************
    if (setCachePolicy[1] == false) {
        if (this->bloomFilter != NULL) {
            delete this->bloomFilter;
            this->bloomFilter = NULL;
        }
    } else {
        // 只有当BF为null的时候才会尝试调用
        if (this->bloomFilter == NULL)
            getBloomFilterPtr();
    }


    // ************ index ************
    if (setCachePolicy[2] == false) {
        if (this->index != NULL) {
            delete this->index;
            this->index = NULL;
        }
    } else {
        if (this->index == NULL)
            getIndexPtr();
    }
}

SSTable::SSTable(std::string setPath, bool setCachePolicy[3]) {
    this->path = setPath;
    // 初始化的事情必须把这个指针指向NULL，因为后面判断会用到！
    this->bloomFilter = NULL;
    this->index = NULL;
    this->header = NULL;

    // 先按照读取文件的缓存策略，全部读取信息
    bool readFilePolicy[3] = {true, true, true};
    this->refreshCachePolicy(readFilePolicy);

    // 再亲自去读取一遍获取大小
    std::ifstream inFile(setPath, std::ios::in | std::ios::binary);

    if (inFile) {
        // 文件指针移动到末尾
        inFile.seekg(0, std::ios::end);
        this->fileSize = inFile.tellg();
        inFile.close();
    }
    // 最后按照设定的缓存策略，完成刷新
    this->refreshCachePolicy(setCachePolicy);
}

SSTable::SSTable(uint64_t setTimeStamp, std::list<std::tuple<uint64_t, uint64_t, uint32_t>> &dataList,
                 std::string setPath, bool cachePolicy[3]) {
    /*
     * 对于dataList里面的每个元素
     * 第一个元素是key
     * 第二个是offset
     * 第三个是vlen
     */
    this->path = setPath;
    // 先处理BF过滤器
    this->bloomFilter = new BloomFilter<uint64_t, sstable_bfSize>();
    this->index = new SSTindex();
    this->header = new SSTheader();

    // 变量记录最大最小值
    uint64_t curMinKey = UINT64_MAX;
    uint64_t curMaxKey = 0;

    for (auto iter = dataList.begin(); iter != dataList.end(); iter++) {
        uint64_t keyInList = std::get<0>(*iter);
        uint64_t offsetInList = std::get<1>(*iter);
        uint32_t vlenInList = std::get<2>(*iter);
        curMinKey = std::min(curMinKey, keyInList);
        curMaxKey = std::max(curMaxKey, keyInList);
        bloomFilter->insert(keyInList);
        index->insert(keyInList, offsetInList, vlenInList);
    }
    header->timeStamp = setTimeStamp;
    header->minKey = curMinKey;
    header->maxKey = curMaxKey;
    header->keyValNum = dataList.size();

    header->writeToFile(setPath, sstable_fileOffset_header);
    bloomFilter->writeToFile(setPath, sstable_fileOffset_bf);
    index->writeToFile(setPath, sstable_fileOffset_key);
    this->refreshCachePolicy(cachePolicy);

    // 再亲自去读取一遍获取文件大小！
    std::ifstream inFile(setPath, std::ios::in | std::ios::binary);

    if (inFile) {
        // 文件指针移动到末尾
        inFile.seekg(0, std::ios::end);
        this->fileSize = inFile.tellg();
        inFile.close();
    }
}

SSTable::~SSTable() {
    // 删除所有的指针
    bool policy[3] = {false, false, false};
    refreshCachePolicy(policy);
}

void SSTable::clear() {
    utils::rmfile(path);
}

uint64_t SSTable::getSStableTimeStamp() {
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);
    uint64_t retVal = this->getHeaderPtr()->timeStamp;
    this->refreshCachePolicy(savedCachePolicy);
    return retVal;
}

uint64_t SSTable::getSStableMinKey() {
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);
    uint64_t retVal = this->getHeaderPtr()->minKey;
    this->refreshCachePolicy(savedCachePolicy);
    return retVal;
}

uint64_t SSTable::getSStableMaxKey() {
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);
    uint64_t retVal = this->getHeaderPtr()->maxKey;
    this->refreshCachePolicy(savedCachePolicy);
    return retVal;
}

uint64_t SSTable::getSStableKeyValNum() {
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);
    uint64_t retVal = this->getHeaderPtr()->keyValNum;
    this->refreshCachePolicy(savedCachePolicy);
    return retVal;
}

uint64_t SSTable::getSStableKey(size_t index) {
    // 保存旧的缓存策略
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);

    uint64_t retVal = UINT64_MAX;

    if (index >= this->getHeaderPtr()->keyValNum) {
        this->refreshCachePolicy(savedCachePolicy);
        return UINT64_MAX;
    } else {
        retVal = this->getIndexPtr()->getKeyByIndex(index);
    }
    this->refreshCachePolicy(savedCachePolicy);
    return retVal;
}

uint64_t SSTable::getSStableOffset(size_t index) {
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);
    if (index >= this->getHeaderPtr()->keyValNum) {
        this->refreshCachePolicy(savedCachePolicy);
        return UINT64_MAX;
    }
    uint64_t retVal = this->getIndexPtr()->getOffsetByIndex(index);
    this->refreshCachePolicy(savedCachePolicy);
    return retVal;
}

uint32_t SSTable::getSStableVlen(size_t index) {
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);
    if (index >= this->getHeaderPtr()->keyValNum) {
        this->refreshCachePolicy(savedCachePolicy);
        return UINT32_MAX;
    }
    uint32_t retVal = this->getIndexPtr()->getVlenByIndex(index);
    this->refreshCachePolicy(savedCachePolicy);
    return retVal;
}

uint64_t SSTable::getKeyOrLargerIndexByKey(uint64_t key) {
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);

    uint64_t retVal = getIndexPtr()->getKeyOrLargerIndexByKey(key);

    this->refreshCachePolicy(savedCachePolicy);
    return retVal;
}

/**
 * @brief 扫描 SSTable 中指定键范围 [key1, key2] 的数据，并将结果存入提供的 scanMap 中。
 *
 * scanMap 结构为：<key, <timestamp, <offset, vlen>>>
 * 其中 key 是从 SSTable 扫描到的键，timestamp 是当前 SSTable 的时间戳，
 * offset 是数据在vlog的起始位置偏移，vlen 是数据的长度。
 *
 * @param key1 uint64_t 类型，指定扫描范围的起始键。
 * @param key2 uint64_t 类型，指定扫描范围的结束键。函数将扫描从 key1 到 key2 的所有键（包括边界）。
 * @param scanMap std::map<uint64_t, std::map<uint64_t, std::map<uint64_t, uint32_t>>>& 类型，
 *                引用传递，用于存储扫描结果。该结构存储了每个键及其对应的数据，包括时间戳、偏移和长度。
 *                修改后的 scanMap 将包含此次扫描过程中新增或更新的数据。
 *
 * @return void 该函数不返回任何值。
 *
 * @note 注意注意！！！这里我没有对数据进行筛选，最后应该筛选timestamp最大的并且值并不是delete_tag的值
 */
void SSTable::scan(uint64_t key1, uint64_t key2,
                   std::map<uint64_t, std::map<uint64_t, std::map<uint64_t, uint32_t>>> &scanMap) {
/*
 * scanMap的结构如下：
 * <key,<timestamp,<offset,vlen>>>
 */

    uint64_t startKeyIndex = this->getKeyOrLargerIndexByKey(key1);
    // 比最大的还要大，或者这个sst没有数据，不处理
    if (startKeyIndex == UINT64_MAX)
        return;
    // 写入当前的时间戳 key
    uint64_t curSStableTimeStamp = this->getSStableTimeStamp();
    for (size_t i = startKeyIndex; i < this->getSStableKeyValNum(); i++) {
        uint64_t curKey = getSStableKey(i);
        if (curKey > key2)
            break;
        uint64_t offset = getSStableOffset(i);
        uint32_t vlen = getSStableVlen(i);
        scanMap[curKey][curSStableTimeStamp][offset] = vlen;
    }
}

bool SSTable::checkIfKeyExist(uint64_t targetKey) {
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);

    if (targetKey > getHeaderPtr()->maxKey || targetKey < getHeaderPtr()->minKey) {
        this->refreshCachePolicy(savedCachePolicy);
        return false;
    }

    if (this->cachePolicy[1] == true) {
        bool retVal = getBloomFilterPtr()->find(targetKey);
        this->refreshCachePolicy(savedCachePolicy);
        return retVal;
    }

    this->refreshCachePolicy(savedCachePolicy);
    return true;
}

uint64_t SSTable::getKeyIndexByKey(uint64_t key) {
    bool savedCachePolicy[3];
    this->saveCachePolicy(savedCachePolicy);

    uint64_t retVal = getIndexPtr()->getKeyIndexByKey(key);

    this->refreshCachePolicy(savedCachePolicy);

    return retVal;
}



