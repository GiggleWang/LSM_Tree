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
    for (auto iterX = ssTableIndex.begin(); iterX != ssTableIndex.end(); iterX++) {
        // iterX->second.size() 代表当前楼层的文件数量
        // iterX->first 代表当前的楼层
        if (config_level_limit[iterX->first] < iterX->second.size())
            return iterX->first;
    }
    return -1;
}

void KVStore::merge(uint64_t X) {
    // 检查X+1层是否存在，不存在就创建一层
    if (config_level_limit.count(X + 1) == 0) {
        config_level_limit[X + 1] = config_level_limit[X] * 2;
        // 新建的层都默认是Leveling
        config_level_type[X + 1] = Leveling;

        // 新建的层还需要创建目录
        std::string levelPathStr = this->dataDir + "/" + "level-" + std::to_string(X + 1);
        utils::mkdir(levelPathStr.c_str());

        // 及时把层的变更写入到配置文件中
        this->writeConfig(confFilePath);
    }
    // 现在我们需要进入sstable选择阶段，选择好sstable用map存储
    // ssTableSelect[层数][时间戳]->sstable
    std::map<uint64_t, std::map<uint64_t, SSTable *> > ssTableSelect;
    // 如果X层是Tiering，那就需要全部选中
    if (config_level_type[X] == Tiering) {
        for (auto iter = ssTableIndex[X].begin(); iter != ssTableIndex[X].end(); iter++) {
            ssTableSelect[X][iter->first] = iter->second;
        }
    }
        // Leveling的话就需要选择时间戳最小的几个文件
    else if (config_level_type[X] == Leveling) {
        // 确定要选择的文件数量
        int selectNum = ssTableIndex[X].size() - config_level_limit[X];
        int alreadyChooseNum = 0;
        // auto iter = ssTableIndex[X].begin();

        // 需要根据文件里面的时间戳，选择前面的selectNum个
        // 创建一个排序用的表 sortMap[时间戳][minKey]
        std::map<uint64_t, std::map<uint64_t, SSTable *> > sortMap;
        std::map<uint64_t, std::map<uint64_t, uint64_t> > tableName;

        for (auto iter = ssTableIndex[X].begin(); iter != ssTableIndex[X].end(); iter++) {
            // iter->first 代表文件名.sst前面的数字
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

    // 如果X+1层是leveling
    if (config_level_type[X + 1] == Leveling) {

        if (ssTableSelect[X].size() > 0) {
            // 获取最小的key、最大的key！
            uint64_t LevelXminKey = UINT64_MAX;
            uint64_t LevelXmaxKey = 0;
            // iter 遍历 计算LevelX的最小最大key！

            for (auto iter = ssTableSelect[X].begin(); iter != ssTableSelect[X].end(); iter++) {
                SSTable *curSStable = iter->second;
                LevelXminKey = std::min(LevelXminKey, curSStable->getSStableMinKey());
                LevelXmaxKey = std::max(LevelXmaxKey, curSStable->getSStableMaxKey());
            }
            // 完成遍历后，去Level X+1 层查找！查找有重叠范围的！
            for (auto iter = ssTableIndex[X + 1].begin(); iter != ssTableIndex[X + 1].end(); iter++) {

                SSTable *curSStable = iter->second;
                uint64_t curMinKey = curSStable->getSStableMinKey();
                uint64_t curMaxKey = curSStable->getSStableMaxKey();
                // 如果区间没有重叠(那就要求当前的表的 max 小于上一层的 min 或者 当前表的 min 大于上一层的max)
                if (curMaxKey < LevelXminKey || curMinKey > LevelXmaxKey)
                    continue;
                // 反之有重叠，选择！
                ssTableSelect[X + 1][iter->first] = curSStable;
            }
        }
    }
    //there
    // ssTableSelect[层数][时间戳]->sstable
    // 现在，需要把map变成一维的 ssTableSelectProcessed[时间戳] = 指针
    std::vector<SSTable *> ssTableSelectProcessed;

    // 排序使用的内存空间
    //<key,<时间戳,<offset,vlen>>>
    std::map<uint64_t, std::map<uint64_t, std::pair<uint64_t, uint32_t> >> sortMap;
    // iterX 对应level - i

    // 遍历过程中，找到最终写到文件内部的时间戳（根据要求，是所有选择文件里面的最大的时间戳！）
    uint64_t finalWriteFileTimeStamp = 0;

    for (auto iterX = ssTableSelect.rbegin(); iterX != ssTableSelect.rend(); iterX++) {
        // iterY->first 对应 时间戳
        // iterY->second 对应指针
        for (auto iterY = ssTableSelect[iterX->first].rbegin(); iterY != ssTableSelect[iterX->first].rend(); iterY++) {
            SSTable *tableCur = iterY->second;
            ssTableSelectProcessed.push_back(tableCur);
            finalWriteFileTimeStamp = std::max(tableCur->getSStableTimeStamp(), finalWriteFileTimeStamp);
        }
    }
    for (size_t i = 0; i < ssTableSelectProcessed.size(); i++) {
        // iter->first 时间戳 iter->second 指针
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
            // 寻找最新的时间戳的信息
            iterY--;

            std::string tmpvalue = vlog->findValueByOffsetAndVlen(iterY->second.first, iterY->second.second);

            // 正好是删除tag，根据要求，只有当X+1是最后一层的时候，才能删除(说白了就是X+2层不存在！)
            if (tmpvalue == delete_tag && config_level_limit.count(X + 2) == 0) {
                continue;
            }
            // 如果是最后一层，那就删除掉deletetag！
            if (tmpvalue == delete_tag && ssTableIndex[X + 2].size() == 0) {
                continue;
            }
            // 不是删除tag，保留这个元素
            // 此时 iterY->second 指向最新的元素 iterX->first是key
            sortMapProcessed[iterX->first] = std::make_pair(iterY->second.first, iterY->second.second);
        }
    }
    // 释放空间
    sortMap.clear();
    //std::list<std::tuple<uint64_t, uint64_t, uint32_t>> &dataList,
    //开始做list
    std::list<std::tuple<uint64_t, uint64_t, uint32_t>> list;
    uint listSSTfileSize = sstable_headerSize + sstable_bfSize;
    for (auto iter = sortMapProcessed.begin(); iter != sortMapProcessed.end(); iter++) {
        uint64_t curKey = iter->first;
        uint64_t curOffset = iter->second.first;
        uint32_t curVlen = iter->second.second;


        // 添加之后的文件增量
        uint64_t addFileSize = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);

        // 尝试插入list
        if (addFileSize + listSSTfileSize <= sstable_maxSize) {
            listSSTfileSize += addFileSize;
            list.emplace_back(curKey, curOffset, curVlen);
        } else {
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
            listSSTfileSize += addFileSize;
            list.emplace_back(curKey, curOffset, curVlen);
        }
    }
// 如果缓存区域还有数据，继续进行sstable构建
    if(list.size() > 0){
        // 时间戳获取
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        std::chrono::microseconds msTime;
        msTime = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());

        std::string newFilePath = this->dataDir + "/level-" + std::to_string(X+1) + "/" + std::to_string(msTime.count()) +".sst";

        SSTable* newSStable = new SSTable(finalWriteFileTimeStamp, list, newFilePath, cachePolicy);
        this->ssTableIndex[X+1][msTime.count()] = newSStable;

        list.clear();
        listSSTfileSize = sstable_headerSize + sstable_bfSize;
    }

    for(auto iterX = ssTableSelect.begin(); iterX != ssTableSelect.end(); iterX++){
        // iterY->first 对应 时间戳
        // iterY->second 对应指针
        for(auto iterY = ssTableSelect[iterX->first].begin(); iterY != ssTableSelect[iterX->first].end(); iterY++){
            // 先删除文件
            SSTable * tableCur = iterY->second;
            tableCur->clear();

            if(tableCur != NULL)
                delete tableCur;
            if(ssTableIndex[iterX->first].count(iterY->first) == 1){
                ssTableIndex[iterX->first].erase(iterY->first);
            }
        }
    }
}





/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
}

