#include "memtable.h"

MemTable::MemTable() {
    skiplist = new Skiplist<uint64_t, std::string>();
    // 初始化占用的sst大小为 头部的header和bf过滤器的size
    sstSpaceSize = sstable_headerSize + sstable_bfSize;
    this->restoreFromLog(logFilePath);
}

MemTable::~MemTable() {
    utils::rmfile(logFilePath);
    delete skiplist;
}

void MemTable::put(uint64_t key, const std::string &s) {
    this->writeLog(logFilePath, PUT, key, s);
    this->putKV(key, s);
}

bool MemTable::del(uint64_t key) {
    this->writeLog(logFilePath, DELETE, key, "");
    return this->delKV(key);
}

std::string MemTable::get(uint64_t key) {
    auto tryFindNode = this->skiplist->find(key);
    if (tryFindNode != NULL) {
        std::string result = tryFindNode->val;
        // 检查result是否为删除标记
        if (result == delete_tag)
            return memtable_already_deleted;
        return result;
    }
    return memtable_not_found;
}


void MemTable::reset() {
    utils::rmfile(logFilePath);
    // 重置之后，首先清空跳表数据
    this->skiplist->clear();
    // 然后把当前维护的转换到sst的大小计算出来
    sstSpaceSize = sstable_headerSize + sstable_bfSize;
}

void MemTable::scan(uint64_t key1, uint64_t key2, std::map<uint64_t, std::string> &resultMap) {
    Node<uint64_t, std::string> *iter = this->skiplist->find(key1);
    // 如果key1不存在 那就手动插入一个 key 1然后再完成之后删除掉。
    if (iter == NULL) {
        iter = this->skiplist->insert(key1, "");
        // 跳过自己，因为已经证实自己不存在
        iter = iter->next[0];

        while (iter->type == nodeType_Data && iter->key <= key2) {
            // 不要把已经删除了的数据插入
            if (iter->val != delete_tag)
                resultMap[iter->key] = iter->val;
                // 删除的数据，必须强制抹掉！内存表时间优先级MAX！
            else
                resultMap.erase(iter->key);
            iter = iter->next[0];
        }
        // 查找完成后记得把自己添加的元素删除
        this->skiplist->remove(key1);
    }
        // 如果key1存在，那就正常查找
    else {
        // 增加检查是否为deleteTag！
        if (iter->key == key1 && iter->val != delete_tag) {
            resultMap[iter->key] = iter->val;
        }
        iter = iter->next[0];
        while (iter->type == nodeType_Data && iter->key <= key2) {
            if (iter->val != delete_tag)
                resultMap[iter->key] = iter->val;
            iter = iter->next[0];
        }
    }
}

void MemTable::writeLog(std::string path, operationID id, uint64_t key, std::string val) {
    std::ofstream outFile(path, std::fstream::out | std::fstream::app);

    switch (id) {
        case PUT:
            outFile << "PUT"
                    << " " << key << " " << val << "\n";
            break;
        case DELETE:
            outFile << "DEL"
                    << " " << key << "\n";
            break;
        default:
            break;
    }

    outFile.close();
}

void MemTable::putKV(uint64_t key, const std::string &s) {
    // 插入节点之前，先检查节点是否存在
    Node<uint64_t, std::string> *tryFind = this->skiplist->find(key);

    // 插入全新的key-value对，就需要把sstSpaceSize更新
    if (tryFind == NULL) {
        this->skiplist->insert(key, s);
        sstSpaceSize += (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t));
    }
        // 否则不需要更新sstSpaceSize
    else {
        this->skiplist->insert(key, s);
    }
}

bool MemTable::delKV(uint64_t key) {
    auto tryFind = this->skiplist->find(key);
    if (tryFind == NULL)
        return false;
    // 占用的空间要减回去
    sstSpaceSize -= (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t));
    this->skiplist->remove(key);
    return true;
}


/**
 * 从指定的日志文件中恢复 MemTable 的状态。
 * 这个函数读取一个日志文件，文件中记录了对 MemTable 的操作，包括插入和删除键值对。
 * 根据日志中记录的操作类型和键值对，这个函数将重建 MemTable 的内容。
 *
 * @param path 日志文件的路径，函数将从这个路径读取日志。
 *
 * 日志文件的每一行代表一个操作，操作类型可以是 "PUT" 或 "DEL"。
 * 对于 "PUT" 操作，行的格式为 "PUT key value"，表示将键值对插入到 MemTable 中。
 * 对于 "DEL" 操作，行的格式为 "DEL key"，表示从 MemTable 中删除对应的键。
 *
 * 函数首先打开文件，然后逐行读取操作。对于每一行，函数解析操作类型和键（以及值，如果有的话），
 * 并调用对应的 MemTable 方法来更新内存表的状态。
 */
void MemTable::restoreFromLog(std::string path) {
    std::ifstream inFile;
    inFile.open(path);

    std::string rowLog;
    std::string operationType;
    uint64_t key;
    std::string val;

    // 如果配置文件存在，那就读取
    if (inFile.is_open()) {
        while (std::getline(inFile, rowLog)) {
            std::vector<size_t> spaceIndex;

            // 扫描空格的位置
            for (size_t i = 0; i < rowLog.size(); i++) {
                if (rowLog[i] == ' ') {
                    spaceIndex.push_back(i);
                }

                if (spaceIndex.size() == 2)
                    break;
                if (spaceIndex.size() == 1 && rowLog.substr(0, spaceIndex[0]) == "DEL")
                    break;
            }

            if (spaceIndex.size() == 1) {
                operationType = rowLog.substr(0, spaceIndex[0]);
                std::stringstream keyStringStream(rowLog.substr(spaceIndex[0] + 1, spaceIndex[1] - spaceIndex[0] - 1));
                keyStringStream >> key;

                this->delKV(key);
            } else if (spaceIndex.size() == 2) {
                operationType = rowLog.substr(0, spaceIndex[0]);

                std::stringstream keyStringStream(rowLog.substr(spaceIndex[0] + 1, spaceIndex[1] - spaceIndex[0] - 1));
                keyStringStream >> key;

                val = rowLog.substr(spaceIndex[1] + 1);

                this->putKV(key, val);
            }
        }
    }
}

bool MemTable::putCheck(uint64_t key, const std::string &s) {
    size_t putSpace = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);

    if (sstSpaceSize + putSpace <= sstable_maxSize)
        return true;
    return false;

}

void MemTable::copyAll(std::list<std::pair<uint64_t, std::string>> &list) {
    this->skiplist->copyAll(list);
}