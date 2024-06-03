//memtable.cc
#include "memtable.h"

MemTable::MemTable() {
    skiplist = new Skiplist<uint64_t, std::string>();
    sstSpaceSize = sstable_headerSize + sstable_bfSize;
    restoreFromLog(logFilePath);
}

MemTable::~MemTable() {
    utils::rmfile(logFilePath);
    delete skiplist;
}

void MemTable::put(uint64_t key, const std::string &value) {
    writeLog(logFilePath, PUT, key, value);
    putKV(key, value);
}

bool MemTable::del(uint64_t key) {
    writeLog(logFilePath, DELETE, key, "");
    return delKV(key);
}

std::string MemTable::get(uint64_t searchKey) {
    auto foundNode = skiplist->find(searchKey);
    if (foundNode != nullptr) {
        std::string value = foundNode->val;
        if (value == delete_tag)
            return memtable_already_deleted;
        return value;
    }
    return memtable_not_exist;
}

void MemTable::reset() {
    utils::rmfile(logFilePath);
    // 清空跳表数据
    skiplist->clear();
    // 重新计算当前维护的转换到 SST 的大小
    sstSpaceSize = sstable_headerSize + sstable_bfSize;
}

void MemTable::scan(uint64_t startKey, uint64_t endKey, std::map<uint64_t, std::string> &resultMap) {
    Node<uint64_t, std::string> *currentNode = skiplist->find(startKey);

    // 如果 startKey 不存在，插入一个临时节点并跳过自己
    if (currentNode == nullptr) {
        currentNode = skiplist->insert(startKey, "");
        currentNode = currentNode->next[0];

        while (currentNode->type == nodeType_Data && currentNode->key <= endKey) {
            if (currentNode->val != delete_tag) {
                resultMap[currentNode->key] = currentNode->val;
            } else {
                resultMap.erase(currentNode->key);
            }
            currentNode = currentNode->next[0];
        }
        skiplist->remove(startKey);
    } else {
        if (currentNode->key == startKey && currentNode->val != delete_tag) {
            resultMap[currentNode->key] = currentNode->val;
        }
        currentNode = currentNode->next[0];

        while (currentNode->type == nodeType_Data && currentNode->key <= endKey) {
            if (currentNode->val != delete_tag) {
                resultMap[currentNode->key] = currentNode->val;
            }
            currentNode = currentNode->next[0];
        }
    }
}

void MemTable::writeLog(const std::string filepath, operationID opID, uint64_t key,  std::string value) {
    std::ofstream logFile(filepath, std::fstream::out | std::fstream::app);

    if (opID == PUT) {
        logFile << "PUT" << " " << key << " " << value << "\n";
    } else if (opID == DELETE) {
        logFile << "DEL" << " " << key << "\n";
    }

    logFile.close();
}

void MemTable::putKV(uint64_t key, const std::string &value) {
    Node<uint64_t, std::string> *foundNode = skiplist->find(key);

    if (foundNode == nullptr) {
        skiplist->insert(key, value);
        sstSpaceSize += (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t));
    } else {
        skiplist->insert(key, value);
    }
}

bool MemTable::delKV(uint64_t key) {
    auto foundNode = skiplist->find(key);
    if (foundNode == nullptr) {
        return false;
    }

    sstSpaceSize -= (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t));
    skiplist->remove(key);
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
void MemTable::restoreFromLog(const std::string filepath) {
    std::ifstream logFile(filepath);

    std::string logEntry;
    std::string operation;
    uint64_t key;
    std::string value;
    if (logFile.is_open()) {
        while (std::getline(logFile, logEntry)) {
            std::vector<size_t> spacePositions;
            for (size_t i = 0; i < logEntry.size(); ++i) {
                if (logEntry[i] == ' ') {
                    spacePositions.push_back(i);
                }
                if (spacePositions.size() == 2) {
                    break;
                }
                if (spacePositions.size() == 1 && logEntry.substr(0, spacePositions[0]) == "DEL") {
                    break;
                }
            }

            if (spacePositions.size() == 1) {
                operation = logEntry.substr(0, spacePositions[0]);
                std::stringstream keyStream(logEntry.substr(spacePositions[0] + 1));
                keyStream >> key;

                delKV(key);
            } else if (spacePositions.size() == 2) {
                operation = logEntry.substr(0, spacePositions[0]);

                std::stringstream keyStream(logEntry.substr(spacePositions[0] + 1, spacePositions[1] - spacePositions[0] - 1));
                keyStream >> key;

                value = logEntry.substr(spacePositions[1] + 1);

                putKV(key, value);
            }
        }
    }
}

bool MemTable::putCheck(uint64_t key, const std::string &value)  {
    size_t requiredSpace = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);

    return (sstSpaceSize + requiredSpace <= sstable_maxSize);
}

void MemTable::copyAll(std::list<std::pair<uint64_t, std::string>> &kvList)  {
    skiplist->copyAll(kvList);
}
