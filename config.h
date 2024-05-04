#pragma once
#include <string>

#define logFilePath "./WAL.log"

// sstable的header的大小
#define sstable_headerSize 32

// sstable的bloom filter大小(Byte)
#define sstable_bfSize 8192

#define delete_tag "~DELETED~"

#define memtable_not_exist "~![ERROR] MemTable No Exist!~"

#define memtable_not_found ""

#define memtable_already_deleted "~![ERROR] ALREADY DELETED!~"

#define sstable_maxSize 16 * 1024

const bool Tiering = 0;

const bool Leveling = 1;

// 缓存策略
const std::string confFilePath = "./default.conf";

#define sstable_fileOffset_header 0

#define sstable_fileOffset_bf 32

#define sstable_fileOffset_key 8224
enum operationID
{
    PUT,
    DELETE
};