//sstheader.cc
#include "sstheader.h"

/**
 * 提供的一种构造函数，通过文件读写，获取header信息
 * @param path 文件路径 
 * @param offset 要读取的偏移量，默认为0
*/
SSTheader::SSTheader(std::string filePath, uint32_t fileOffset) {
    int readResult = readFile(filePath, fileOffset);
    if (readResult != 0) {
        std::cerr << "[Error] 崩溃！无法创建header" << readResult << std::endl;
    }
}
/**
 * 从文件读取Header数据
 * @param path 文件的路径
 * @param offset 要读取的偏移量，默认为0
 * @return 0代表正常返回，-1代表文件不存在 -2代表范围异常
 * 使用样例：readFile("./data", 0);
 */
int SSTheader::readFile(const std::string filePath, uint32_t fileOffset) {
    std::ifstream inputFile(filePath, std::ios::in | std::ios::binary);
    if (!inputFile)
        return -1;
    inputFile.seekg(0, std::ios::end);

    uint32_t fileSize = inputFile.tellg();
    if (fileOffset > fileSize || fileOffset + sizeof(SSTheader) > fileSize) {
        inputFile.close();
        return -2;
    }
    inputFile.clear();
    inputFile.seekg(fileOffset, std::ios::beg);
    inputFile.read(reinterpret_cast<char*>(&timeStamp), sizeof(timeStamp));
    inputFile.read(reinterpret_cast<char*>(&keyValNum), sizeof(keyValNum));
    inputFile.read(reinterpret_cast<char*>(&minKey), sizeof(minKey));
    inputFile.read(reinterpret_cast<char*>(&maxKey), sizeof(maxKey));

    inputFile.close();
    return 0;
}



/**
 * 把Header数据写入到文件里面
 * @param path 文件的路径
 * @param offset 要写入的偏移量，默认为0
 * @return 返回实际写入的偏移量，出现异常返回-1
 * 
 * @example readFile("./data", 0);
 * 如果需要写入到特定的文件的位置，函数会有判断逻辑：当offset != 0 的时候，会判断文件是否存在
 * 如果文件不存在，会创建一个文件，并把写入的起点offset设置为0
 * 如果文件存在，会判断文件的大小，当offset < 文件大小的时候，才会覆盖性的写入，反之以追加模式写入文件
 * 调用者可以通过[返回值] 获取到实际写入的偏移量（Byte单位），-1(也就是一个最大的int32)说明是异常
 */
uint32_t SSTheader::writeToFile(std::string path, uint32_t offset) {
    // 判断文件是否存在，用尝试读取的方式打开，如果打开成功就认为文件存在
    bool fileExists = false;
    uint32_t fileSize = 0;
    std::ifstream inputFile(path, std::ios::in | std::ios::binary);

    if (inputFile) {
        inputFile.seekg(0, std::ios::end);
        fileSize = inputFile.tellg();
        inputFile.close();
        fileExists = true;
    }
    if (!fileExists) {
        offset = 0;
        std::fstream createFile(path, std::ios::out | std::ios::binary);
        createFile.close();
    }
    else if (fileExists && (offset > fileSize)) {
        offset = fileSize;
    }

    std::fstream outputFile(path, std::ios::out | std::ios::in | std::ios::binary);

    if (!outputFile)
        return -1;
    outputFile.seekp(offset, std::ios::beg);
    outputFile.write(reinterpret_cast<char*>(&timeStamp), sizeof(timeStamp));
    outputFile.write(reinterpret_cast<char*>(&keyValNum), sizeof(keyValNum));
    outputFile.write(reinterpret_cast<char*>(&minKey), sizeof(minKey));
    outputFile.write(reinterpret_cast<char*>(&maxKey), sizeof(maxKey));

    outputFile.close();
    return offset;
}
