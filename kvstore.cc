// kvstore.cc
#include "kvstore.h"

//bool cachePolicy[3] = {true, true, true};
//bool cachePolicy[3] = {true, true, false};
//bool cachePolicy[3] = {true, false,false};
bool cachePolicy[3] = {false, false, false};
//bool cachePolicy[3] = {true, true, true};

KVStore::KVStore(const std::string &dir, const std::string &vlogDir) : KVStoreAPI(dir, vlogDir) {
    // 初始化目录和已有的最大时间戳
    this->dataDir = dir;
    this->sstMaxTimeStamp = 0;
    // 读取配置文件
    this->readConfig(confFilePath);
    // 根据配置文件执行文件检查，如果存在文件，就读取到缓存，刷新sstMaxTimeStamp
    this->sstFileCheck(dir);
    // 创建MemTable和vLog
    this->memTable = new MemTable();
    this->vlog = new vLog(vlogDir);
}

KVStore::~KVStore() {
    delete this->memTable;
    delete this->vlog;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &value) {
    if (this->memTable->putCheck(key, value)) {
        this->memTable->put(key, value);
        return;
    }
    std::list<std::pair<uint64_t, std::string>> allData;
    this->memTable->copyAll(allData);
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());
    this->sstMaxTimeStamp++;
    std::string newFilePath = this->dataDir + "/level-0/" + std::to_string(timestamp.count()) + ".sst";
    /*
     * 将得到的allData分为两部分
     * 一部分是std::list<std::tuple<uint64_t, uint64_t, uint32_t>>
     *      即<key offset vlen>
     * 另一部分是value（直接插入即可)
     */
    std::list<std::tuple<uint64_t, uint64_t, uint32_t>> indexData;
    for (const auto &entry : allData) {
        uint64_t insertKey = entry.first;
        const std::string &insertValue = entry.second;
        uint32_t valueLen = insertValue.size();
        uint64_t valueOffset = vlog->appendEntry(insertKey, insertValue);
        indexData.push_back(std::make_tuple(insertKey, valueOffset, valueLen));
    }
    SSTable *newSST = new SSTable(sstMaxTimeStamp, indexData, newFilePath, cachePolicy);
    ssTableIndex[0][timestamp.count()] = newSST;
    // 内存表格重置
    this->memTable->reset();
    // 发起归并检查，递归执行
    int mergeLevel = this->mergeCheck();
    while (mergeLevel != -1) {
        this->merge(mergeLevel);
        mergeLevel = this->mergeCheck();
    }
    this->memTable->put(key, value);
}
int KVStore::mergeCheck() {
    for (auto iterX = ssTableIndex.begin(); iterX != ssTableIndex.end(); iterX++) {
        // iterX->second.size() 代表当前楼层的文件数量
        // iterX->first 代表当前的楼层
        if (config_level_limit[iterX->first] < iterX->second.size())
            return iterX->first;
    }
    return -1;
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    std::string result = this->memTable->get(key);
    if (result == memtable_already_deleted)
        return "";
    if (result != memtable_not_exist)
        return result;
    uint64_t latestTimeStamp = 0;
    for (auto &level : ssTableIndex) {
        bool found = false;
        for (auto sstIter = level.second.rbegin(); sstIter != level.second.rend(); ++sstIter) {
            SSTable *currentSST = sstIter->second;
            if (currentSST->checkIfKeyExist(key)) {
                uint64_t keyIndex = currentSST->getKeyIndexByKey(key);
                if (keyIndex == UINT64_MAX)
                    continue;
                uint64_t valueOffset = currentSST->getSStableOffset(keyIndex);
                uint32_t valueLength = currentSST->getSStableVlen(keyIndex);
                std::string value = vlog->findValueByOffsetAndVlen(valueOffset, valueLength);
                if (currentSST->getSStableTimeStamp() >= latestTimeStamp) {
                    latestTimeStamp = currentSST->getSStableTimeStamp();
                    result = value;
                    found = true;
                }
            }
        }
        if (found && result == delete_tag)
            return "";
        else if (found)
            return result;
    }
    if (result == delete_tag || result == memtable_not_exist)
        return "";
    return result;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    if (this->get(key).empty())
        return false;
    this->put(key, delete_tag);
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    utils::rmfile(logFilePath);
    this->memTable->reset();
    for (auto &level : ssTableIndex) {
        for (auto &sstEntry : level.second) {
            sstEntry.second->clear();
            delete sstEntry.second;
        }
    }
    this->ssTableIndex.clear();
    this->vlog->reset();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &resultList) {
    std::map<uint64_t, std::map<uint64_t, std::map<uint64_t, uint32_t>>> scanMap;
    std::map<uint64_t, std::string> mergedMap;
    /*
    * scanMap的结构如下：
    * <key,<timestamp,<offset,vlen>>>
    */
    for (auto levelIter = ssTableIndex.rbegin(); levelIter != ssTableIndex.rend(); ++levelIter) {
        for (auto sstIter = levelIter->second.begin(); sstIter != levelIter->second.end(); ++sstIter) {
            SSTable *currentTable = sstIter->second;
            currentTable->scan(key1, key2, scanMap);
        }
    }
    for (const auto &outerPair : scanMap) {
        uint64_t finalKey = outerPair.first;
        const auto &timeStampMap = outerPair.second;
        uint64_t latestTimeStamp = 0;
        std::string latestValue = "";
        for (const auto &middlePair : timeStampMap) {
            uint64_t currentTimestamp = middlePair.first;
            if (currentTimestamp < latestTimeStamp)
                continue;
            const auto &offsetMap = middlePair.second;
            std::string value;
            for (const auto &innerPair : offsetMap) {
                uint64_t offset = innerPair.first;
                uint32_t valueLength = innerPair.second;
                value = vlog->findValueByOffsetAndVlen(offset, valueLength);
            }

            if (value == delete_tag)
                continue;

            latestTimeStamp = currentTimestamp;
            latestValue = value;
        }

        if (!latestValue.empty()) {
            mergedMap[finalKey] = latestValue;
        }
    }
    std::map<uint64_t, std::string> memTableMap;
    this->memTable->scan(key1, key2, memTableMap);
    for (const auto &memPair : memTableMap) {
        mergedMap[memPair.first] = memPair.second;
    }
    for (const auto &mergedPair : mergedMap) {
        resultList.push_back({mergedPair.first, mergedPair.second});
    }
}

void KVStore::readConfig(std::string filePath) {
    std::ifstream inputFile(filePath);

    uint64_t level, limit;
    std::string levelType;

    while (inputFile >> level >> limit >> levelType) {
        config_level_limit[level] = limit;
        if (levelType == "Leveling")
            config_level_type[level] = Leveling;
        else if (levelType == "Tiering")
            config_level_type[level] = Tiering;
    }

    inputFile.close();
}

void KVStore::writeConfig(std::string filePath) {
    std::ofstream outputFile(filePath);

    for (const auto &entry : config_level_limit) {
        uint64_t levelID = entry.first;
        uint64_t levelLimit = entry.second;
        uint64_t levelTypeNum = config_level_type[levelID];
        std::string levelTypeStr = (levelTypeNum == Tiering) ? "Tiering" : "Leveling";

        outputFile << levelID << " " << levelLimit << " " << levelTypeStr << "\n";
    }
    outputFile.close();
}

void KVStore::sstFileCheck(std::string dataPath) {
    if (!utils::dirExists(dataPath))
        utils::mkdir(dataPath.c_str());

    for (const auto &levelConfig : config_level_limit) {
        std::string levelPath = dataPath + "/level-" + std::to_string(levelConfig.first);
        if (!utils::dirExists(levelPath)) {
            utils::mkdir(levelPath.c_str());
        }

        std::vector<std::string> files;
        utils::scanDir(levelPath, files);

        for (const auto &file : files) {
            std::string fileID = file.substr(0, file.find('.'));

            uint64_t fileIDNum = 0;
            std::istringstream iss(fileID);
            iss >> fileIDNum;

            std::string fullPath = levelPath + "/" + file;
            SSTable *newTable = new SSTable(fullPath, cachePolicy);
            this->sstMaxTimeStamp = std::max(newTable->getSStableTimeStamp(), this->sstMaxTimeStamp);

            ssTableIndex[levelConfig.first][fileIDNum] = newTable;
        }
    }
}

void KVStore::merge(uint64_t X) {
    if (config_level_limit.count(X + 1) == 0) {
        config_level_limit[X + 1] = config_level_limit[X] * 2;
        config_level_type[X + 1] = Leveling;

        std::string levelPathStr = this->dataDir + "/" + "level-" + std::to_string(X + 1);
        utils::mkdir(levelPathStr.c_str());

        this->writeConfig(confFilePath);
    }
    std::map<uint64_t, std::map<uint64_t, SSTable *> > ssTableSelect;
    if (config_level_type[X] == Tiering) {
        for (auto iter = ssTableIndex[X].begin(); iter != ssTableIndex[X].end(); iter++) {
            ssTableSelect[X][iter->first] = iter->second;
        }
    }
    else if (config_level_type[X] == Leveling) {
        int selectNum = ssTableIndex[X].size() - config_level_limit[X];
        int alreadyChooseNum = 0;
        std::map<uint64_t, std::map<uint64_t, SSTable *> > sortMap;
        std::map<uint64_t, std::map<uint64_t, uint64_t> > tableName;

        for (auto iter = ssTableIndex[X].begin(); iter != ssTableIndex[X].end(); iter++) {
            SSTable *curTable = iter->second;
            uint64_t timeStamp = curTable->getSStableTimeStamp();
            uint64_t minKey = curTable->getSStableMinKey();

            sortMap[timeStamp][minKey] = curTable;
            tableName[timeStamp][minKey] = iter->first;
        }

        for (auto iterX = sortMap.begin(); iterX != sortMap.end(); iterX++) {
            for (auto iterY = iterX->second.begin(); iterY != iterX->second.end(); iterY++) {
                if (alreadyChooseNum < selectNum) {
                    uint64_t curFileID = tableName[iterX->first][iterY->first];

                    ssTableSelect[X][curFileID] = iterY->second;
                    alreadyChooseNum++;
                }
            }
        }

    }
    if (config_level_type[X + 1] == Leveling) {

        if (ssTableSelect[X].size() > 0) {
            uint64_t LevelXminKey = UINT64_MAX;
            uint64_t LevelXmaxKey = 0;
            for (auto iter = ssTableSelect[X].begin(); iter != ssTableSelect[X].end(); iter++) {
                SSTable *curSStable = iter->second;
                LevelXminKey = std::min(LevelXminKey, curSStable->getSStableMinKey());
                LevelXmaxKey = std::max(LevelXmaxKey, curSStable->getSStableMaxKey());
            }
            for (auto iter = ssTableIndex[X + 1].begin(); iter != ssTableIndex[X + 1].end(); iter++) {

                SSTable *curSStable = iter->second;
                uint64_t curMinKey = curSStable->getSStableMinKey();
                uint64_t curMaxKey = curSStable->getSStableMaxKey();
                if (curMaxKey < LevelXminKey || curMinKey > LevelXmaxKey)
                    continue;
                ssTableSelect[X + 1][iter->first] = curSStable;
            }
        }
    }
    std::vector<SSTable *> ssTableSelectProcessed;
    std::map<uint64_t, std::map<uint64_t, std::pair<uint64_t, uint32_t> >> sortMap;
    uint64_t finalWriteFileTimeStamp = 0;

    for (auto iterX = ssTableSelect.rbegin(); iterX != ssTableSelect.rend(); iterX++) {
        for (auto iterY = ssTableSelect[iterX->first].rbegin(); iterY != ssTableSelect[iterX->first].rend(); iterY++) {
            SSTable *tableCur = iterY->second;
            ssTableSelectProcessed.push_back(tableCur);
            finalWriteFileTimeStamp = std::max(tableCur->getSStableTimeStamp(), finalWriteFileTimeStamp);
        }
    }
    for (size_t i = 0; i < ssTableSelectProcessed.size(); i++) {
        SSTable *curTablePt = ssTableSelectProcessed[i];
        uint64_t KVNum = curTablePt->getSStableKeyValNum();
        for (uint64_t i = 0; i < KVNum; i++) {
            uint64_t curKey = curTablePt->getSStableKey(i);
            uint64_t curOffset = curTablePt->getSStableOffset(i);
            uint32_t curVlen = curTablePt->getSStableVlen(i);
            uint64_t timeStamp = curTablePt->getSStableTimeStamp();
            sortMap[curKey][timeStamp] = std::make_pair(curOffset, curVlen);
        }
    }

    // 二次处理
    std::map<uint64_t, std::pair<uint64_t, uint32_t>> sortMapProcessed;
    for (auto iterX = sortMap.begin(); iterX != sortMap.end(); iterX++) {
        auto iterY = iterX->second.end();
        if (iterX->second.size() > 0) {
            iterY--;
            std::string tmpvalue = vlog->findValueByOffsetAndVlen(iterY->second.first, iterY->second.second);
            if (tmpvalue == delete_tag && config_level_limit.count(X + 2) == 0) {
                continue;
            }
            if (tmpvalue == delete_tag && ssTableIndex[X + 2].size() == 0) {
                continue;
            }
            sortMapProcessed[iterX->first] = std::make_pair(iterY->second.first, iterY->second.second);
        }
    }
    sortMap.clear();
    std::list<std::tuple<uint64_t, uint64_t, uint32_t>> list;
    uint listSSTfileSize = sstable_headerSize + sstable_bfSize;
    for (auto iter = sortMapProcessed.begin(); iter != sortMapProcessed.end(); iter++) {
        uint64_t curKey = iter->first;
        uint64_t curOffset = iter->second.first;
        uint32_t curVlen = iter->second.second;

        uint64_t addFileSize = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);
        if (addFileSize + listSSTfileSize <= sstable_maxSize) {
            listSSTfileSize += addFileSize;
            list.emplace_back(curKey, curOffset, curVlen);
        } else {
            std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
            std::chrono::microseconds msTime;
            msTime = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());

            std::string newFilePath =
                    this->dataDir + "/level-" + std::to_string(X + 1) + "/" + std::to_string(msTime.count()) + ".sst";
            SSTable *newSStable = new SSTable(finalWriteFileTimeStamp, list, newFilePath, cachePolicy);
            this->ssTableIndex[X + 1][msTime.count()] = newSStable;

            list.clear();
            listSSTfileSize = sstable_headerSize + sstable_bfSize;
            listSSTfileSize += addFileSize;
            list.emplace_back(curKey, curOffset, curVlen);
        }
    }
    if (list.size() > 0) {
        // 时间戳获取
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        std::chrono::microseconds msTime;
        msTime = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());

        std::string newFilePath =
                this->dataDir + "/level-" + std::to_string(X + 1) + "/" + std::to_string(msTime.count()) + ".sst";

        SSTable *newSStable = new SSTable(finalWriteFileTimeStamp, list, newFilePath, cachePolicy);
        this->ssTableIndex[X + 1][msTime.count()] = newSStable;

        list.clear();
        listSSTfileSize = sstable_headerSize + sstable_bfSize;
    }

    for (auto iterX = ssTableSelect.begin(); iterX != ssTableSelect.end(); iterX++) {
        for (auto iterY = ssTableSelect[iterX->first].begin(); iterY != ssTableSelect[iterX->first].end(); iterY++) {
            SSTable *tableCur = iterY->second;
            tableCur->clear();
            if (tableCur != NULL)
                delete tableCur;
            if (ssTableIndex[iterX->first].count(iterY->first) == 1) {
                ssTableIndex[iterX->first].erase(iterY->first);
            }
        }
    }
}
void setZeroInFile(const std::string& filepath, uint64_t startOffset, uint64_t length) {
    std::fstream fileStream(filepath, std::ios::in | std::ios::out | std::ios::binary);
    if (!fileStream.is_open()) {
        std::cerr << "Error opening file: " << filepath << std::endl;
        return;
    }
    fileStream.seekp(startOffset, std::ios::beg);
    if (!fileStream) {
        std::cerr << "Error seeking to offset: " << startOffset << std::endl;
        fileStream.close();
        return;
    }
    std::vector<char> zeroArray(length, 0);
    fileStream.write(zeroArray.data(), length);
    if (!fileStream) {
        std::cerr << "Error writing zero bytes to file" << std::endl;
    }
    fileStream.close();
}


/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
    std::string filename = vlog->getFileName();
    uint64_t tail = vlog->getTail();

    uint8_t magic;
    uint16_t checksum;
    uint64_t key;
    uint32_t vlen;
    std::string value;
    uint64_t searchedSize = 0;
    uint64_t offset = vlog->getTail();
    std::ifstream file(filename, std::ios::binary);  // 以二进制模式打开文件
//    std::cout<<"                               begin while ...\n";
    while (searchedSize < chunk_size) {
        file.seekg(offset, std::ios::beg);
        if (!file.is_open()) {
            std::cerr << "无法打开文件：" << filename << std::endl;
            return;
        }
        if (!file.read(reinterpret_cast<char *>(&magic), 1) ||
            !file.read(reinterpret_cast<char *>(&checksum), 2) ||
            !file.read(reinterpret_cast<char *>(&key), 8) ||
            !file.read(reinterpret_cast<char *>(&vlen), 4)) {
            std::cout << read_data_error << "1" << std::endl;
            return;
        }
        if (magic != 0xff) {
            std::cout << read_data_error << "2" << std::endl;
            return;
        }
        value.resize(vlen); // 调整字符串的容量以匹配将要读取的数据大小
        if (!file.read(&value[0], vlen)) {
            std::cout << read_data_error << "3" << std::endl;
            return;
        }
//        vlog->refresh();
        //如果在memtable中，那么回收
        //如果在表中查到，并且offset不是这里，那么回收
        //如果在表中查到，并且offset在这里，那么插入memtable
        if (this->memTable->get(key) != memtable_not_exist) {
            searchedSize += (15 + vlen);
            offset += (15 + vlen);
            continue;
        }
        //在ssttable里面找
        uint64_t trueOffset = this->getOffsetByKey(key);
        if (trueOffset != offset) {
//            std::cout << "trueOffset " << trueOffset << " offset: " << offset << std::endl;
            searchedSize += (1 + 2 + 8 + 4 + vlen);
            offset += (1 + 2 + 8 + 4 + vlen);
            continue;
        }
        //下面说明确实是在这里
        this->put(key, value);
        searchedSize += (1 + 2 + 8 + 4 + vlen);
        offset += (1 + 2 + 8 + 4 + vlen);
    }
//    std::cout<<"                               end while ...\n";
    //扫描完成，如果memtable还有数据，那么还要再进行一下组装sstable
    std::list<std::pair<uint64_t, std::string> > list;
    this->memTable->copyAll(list);
    if (!list.empty()) {
        // 时间戳获取
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        std::chrono::microseconds msTime;
        msTime = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());
        // 文件创建 [文件名称规则，为防止重复，用的时间戳作为文件名]
        this->sstMaxTimeStamp = this->sstMaxTimeStamp + 1;
        std::string newFilePath = this->dataDir + "/level-0/" + std::to_string(msTime.count()) + ".sst";
        /*
         * 将得到的dataAll分为两部分
         * 一部分是std::list<std::tuple<uint64_t, uint64_t, uint32_t>>
         *      即<key offset vlen>
         * 另一部分是value（直接插入即可)
         * */
        std::list<std::tuple<uint64_t, uint64_t, uint32_t>> data;
        for (const auto &element: list) {
            uint64_t keyInsert = element.first;
            std::string valueInsert = element.second;
            uint32_t vlenInsert = valueInsert.size();
            uint64_t offsetInsert = vlog->appendEntry(keyInsert, valueInsert);
            data.push_back(std::make_tuple(keyInsert, offsetInsert, vlenInsert));
        }
        SSTable *newSStable = new SSTable(sstMaxTimeStamp, data, newFilePath, cachePolicy);
        ssTableIndex[0][msTime.count()] = newSStable;
        // 内存表格重置
        this->memTable->reset();
        // 发起归并检查，递归执行
        int checkResult = this->mergeCheck();

        while (checkResult != -1) {
            this->merge(checkResult);
            checkResult = this->mergeCheck();
        }
        for (const auto &element: list) {
            this->put(element.first, element.second);
        }


    }
//    std::cout<<"               start alloc...\n";
    utils::de_alloc_file(filename, 0, searchedSize + tail);
//    std::cout<<"               end  alloc...\n";
    vlog->refresh(searchedSize + tail);
}

uint64_t KVStore::getOffsetByKey(uint64_t searchKey) {
    for (auto levelIter = ssTableIndex.begin(); levelIter != ssTableIndex.end(); ++levelIter) {
        for (auto sstIter = levelIter->second.rbegin(); sstIter != levelIter->second.rend(); ++sstIter) {
            SSTable *currentSST = sstIter->second;

            if (currentSST->checkIfKeyExist(searchKey)) {
                uint64_t keyIndex = currentSST->getKeyIndexByKey(searchKey);

                if (keyIndex != UINT64_MAX) {
                    return currentSST->getSStableOffset(keyIndex);
                }
            }
        }
    }
    return UINT64_MAX;
}
