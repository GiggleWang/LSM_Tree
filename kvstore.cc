#include "kvstore.h"

bool cachePolicy[3] = {true, true, true};


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
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    // 发起插入检查！通过插入检查之后，直接插入，退出
    if (this->memTable->putCheck(key, s)) {
        this->memTable->put(key, s);
        return;
    }
//    std::cout << key << "开始写入磁盘 \n";
    // 插入检查失败。发起写入内存
    // 从内存表里面拷贝数据
    std::list<std::pair<uint64_t, std::string> > dataAll;
    this->memTable->copyAll(dataAll);

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
    for (const auto &element: dataAll) {
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
    this->memTable->put(key, s);

}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    std::string result = this->memTable->get(key);

    // 内存表里面找完了，看是否存在，存在就返回
    // 发现内存表已经有了删了标记
    if (result == memtable_already_deleted)
        return "";

    if (result != memtable_not_exist)
        return result;
    // 不存在，那就进入sst查找模式
    uint64_t findLatestTimeStamp = 0;

    for (auto iterX = ssTableIndex.begin(); iterX != ssTableIndex.end(); iterX++) {
        bool ifFind = false;

        // 按照层查找，找到立马返回，
        for (auto iterY = iterX->second.rbegin(); iterY != iterX->second.rend(); iterY++) {
            SSTable *curSST = iterY->second;
            // 检查是否可能存在
            if (curSST->checkIfKeyExist(key)) {
                uint64_t indexResult = curSST->getKeyIndexByKey(key);
                // 不存在，下一个
                if (indexResult == UINT64_MAX)
                    continue;
                uint64_t offsetResult = curSST->getSStableOffset(indexResult);
                uint32_t vlenResult = curSST->getSStableVlen(indexResult);
                std::string valueGet = vlog->findValueByOffsetAndVlen(offsetResult, vlenResult);
                if (curSST->getSStableTimeStamp() >= findLatestTimeStamp) {
                    findLatestTimeStamp = curSST->getSStableTimeStamp();
                    result = valueGet;
                    ifFind = true;
                }
            }
        }

        if (ifFind && result == delete_tag)
            return "";
        else if (ifFind)
            return result;
    }

    // 所有的都没查找成功
    if (result == delete_tag || result == memtable_not_exist)
        return "";
    return result;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    if (this->get(key) == "")
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
    // 再针对文件index，一个一个删除
    for (auto iterX = ssTableIndex.begin(); iterX != ssTableIndex.end(); iterX++) {
        for (auto iterY = ssTableIndex[iterX->first].begin(); iterY != ssTableIndex[iterX->first].end(); iterY++) {
            iterY->second->clear();
            delete iterY->second;
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
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    std::map<uint64_t, std::map<uint64_t, std::map<uint64_t, uint32_t>>> scanMap;
    std::map<uint64_t, std::string> mergeList;
    /*
    * scanMap的结构如下：
    * <key,<timestamp,<offset,vlen>>>
    */
    for (auto iterX = ssTableIndex.rbegin(); iterX != ssTableIndex.rend(); iterX++) {
        for (auto iterY = iterX->second.begin(); iterY != iterX->second.end(); iterY++) {
            SSTable *curTable = iterY->second;
            curTable->scan(key1, key2, scanMap);
        }
    }

    for (const auto &outer_pair: scanMap) {
        uint64_t keyFinal = outer_pair.first;
        const auto &middleMap = outer_pair.second;
        uint64_t tmpTimeStamp = 0;
        std::string tmpValue = "";
        // 遍历中层map
        for (const auto &middle_pair: middleMap) {
            uint64_t timeStampFinal = middle_pair.first;
            if (timeStampFinal < tmpTimeStamp)
                continue;
            const auto &innerMap = middle_pair.second;
            // 遍历内层map
            std::string value;
            for (const auto &inner_pair: innerMap) {
                uint64_t offset = inner_pair.first;
                uint32_t vlen = inner_pair.second;
                value = vlog->findValueByOffsetAndVlen(offset, vlen);
            }
            if (value == delete_tag)
                continue;
            tmpTimeStamp = timeStampFinal;
            tmpValue = value;
        }
        if (tmpValue != "") {
            mergeList[keyFinal] = tmpValue;
        }
    }

    std::map<uint64_t, std::string> memList;
    this->memTable->scan(key1, key2, memList);
    for (const auto &outer_pair: memList) {
        mergeList[outer_pair.first] = outer_pair.second;
    }

    // 把结果返回
    for (auto iter = mergeList.begin(); iter != mergeList.end(); iter++) {
        list.push_back({iter->first, iter->second});
    }


}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
}

void KVStore::readConfig(std::string path) {
    std::ifstream infile;
    infile.open(path);


    uint64_t level, limit;
    std::string levelType;

    while (infile >> level >> limit >> levelType) {
        config_level_limit[level] = limit;
        if (levelType == "Leveling")
            config_level_type[level] = Leveling;
        else if (levelType == "Tiering")
            config_level_type[level] = Tiering;
    }

    infile.close();
}

void KVStore::writeConfig(std::string path) {
    std::ofstream infile;
    infile.open(path);

    for (auto iter = config_level_limit.begin(); iter != config_level_limit.end(); iter++) {
        uint64_t levelID = iter->first;
        uint64_t levelLimit = iter->second;
        uint64_t levelTypeNum = config_level_type[levelID];
        std::string levelTypeStr = (levelTypeNum == Tiering) ? "Tiering" : "Leveling";

        infile << levelID << " " << levelLimit << " " << levelTypeStr << "\n";
    }
    infile.close();
}

void KVStore::sstFileCheck(std::string dataPath) {
    // 检查dataPath是否存在，没有的就创建
    if (!utils::dirExists(dataPath))
        utils::mkdir(dataPath.c_str());

    // 根据配置文件，检查level-i文件夹是否存在
    for (auto iter = config_level_limit.begin(); iter != config_level_limit.end(); iter++) {
        // 拼接第level-i层的文件
        std::string levelPathStr = dataPath + "/" + "level-" + std::to_string(iter->first);
        // 判断目录是否存在，不存在创建
        if (!utils::dirExists(levelPathStr)) {
            utils::mkdir(levelPathStr.c_str());
        }

        std::vector<std::string> scanResult;
        utils::scanDir(levelPathStr, scanResult);

        // 针对扫描的所有文件，尝试读取
        for (size_t i = 0; i < scanResult.size(); i++) {
            std::string fileName = scanResult[i];
            std::string fileID = fileName.substr(0, fileName.find('.'));

            uint64_t fileIDNum = 0;
            std::istringstream iss(fileID);
            iss >> fileIDNum;

            std::string fullPath = levelPathStr + "/" + fileName;
            SSTable *newTable = new SSTable(fullPath, cachePolicy);

            // 初始化读取的时候，更新当前最大的时间戳
            this->sstMaxTimeStamp = std::max(newTable->getSStableTimeStamp(), this->sstMaxTimeStamp);

            ssTableIndex[iter->first][fileIDNum] = newTable;
        }
    }

}

int KVStore::mergeCheck() {
    for(auto iterX = ssTableIndex.begin(); iterX != ssTableIndex.end(); iterX ++){
        // iterX->second.size() 代表当前楼层的文件数量
        // iterX->first 代表当前的楼层
        if(config_level_limit[iterX->first] < iterX->second.size())
            return iterX->first;
    }
    return -1;
}

void KVStore::merge(uint64_t X) {

}

