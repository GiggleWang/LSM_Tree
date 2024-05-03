#include "kvstore.h"

KVStore::KVStore(const std::string &dir, const std::string &vlogDir) : KVStoreAPI(dir, vlogDir)
{
	// 初始化目录和已有的最大时间戳
	this->dataDir = dir;
	// 创建MemTable和vLog
	this->memTable = new MemTable();
	this->vlog = new vLog(vlogDir);
}

KVStore::~KVStore()
{
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	this->memTable->put(key, s);
	// 发起插入检查！通过插入检查之后，直接插入，退出
	/* if(this->memTable->putCheck(key,s)){
		this->memTable->put(key, s);
		return;
	} */
	// std::cout <<key<< "开始写入磁盘 \n";
	// 插入检查失败。发起写入内存
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string ret = this->memTable->get(key);
	// if (ret == memtable_already_deleted)
	// 	return "";
	// else
		return ret;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	return this->memTable->del(key);
	return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    utils::rmfile(logFilePath);
	this->memTable->reset();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    std::map<uint64_t, std::string> mergeList;
    this->memTable->scan(key1, key2, mergeList);
    for(auto iter = mergeList.begin(); iter != mergeList.end(); iter++){
        list.push_back({iter->first, iter->second});
    }
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
}