#include "kvstore.h"

/*
编译指令：
g++ -o test main.cc kvstore.cc memtable.cc sstable.cc vlog.cc sstheader.cc sstindex.cc
*/
class myTest {
private:
    KVStore *store;
    uint64_t number = 256 * 1024;
public:
    myTest(std::string dir1, std::string dir2);

    void prepare();

    void putTest(bool print = true);

    void deleteTest(bool print = true);

    void getTest(bool print = true);

    void reset() { store->reset(); }

    void mergeTest();

    void prepareAQuarter();
};

myTest::myTest(std::string dir1, std::string dir2) {
    store = new KVStore(dir1, dir2);
}

void myTest::prepare() {
    std::cout << "the test is start and the number is " << number << std::endl;
    std::cout << "start preparing for the initial data...\n";
    for (uint64_t i = 0; i < 2048; ++i) {
        store->put(i, std::string(i + 1, 's'));
    }
    std::cout << "end preparing for the initial data...\n";
}
void myTest::prepareAQuarter() {
    for (uint64_t i = 0; i < number; i+=4) {
        store->put(i, std::string(i + 1, 's'));
    }
}

void myTest::putTest(bool print) {
    auto start = std::chrono::high_resolution_clock::now();  // 开始计时

    // 执行 PUT 操作
    for (int i = 0; i < number; ++i) {
        store->put(i, std::string(i, 'e'));
    }
    auto end = std::chrono::high_resolution_clock::now();  // 结束计时
    std::chrono::duration<double> elapsed = end - start;  // 计算总耗时
    if (print) {
        std::cout << "Total elapsed time of put : " << elapsed.count() << " seconds\n";
        std::cout << "Throughput: " << number / elapsed.count() << " operations per second\n";
    }
}

void myTest::deleteTest(bool print) {
    auto start = std::chrono::high_resolution_clock::now();  // 开始计时

    // 执行 PUT 操作
    for (int i = 0; i < number; ++i) {
        store->del(i);
    }
    auto end = std::chrono::high_resolution_clock::now();  // 结束计时
    std::chrono::duration<double> elapsed = end - start;  // 计算总耗时
    if (print) {
        std::cout << "Total elapsed time of delete : " << elapsed.count() << " seconds\n";
        std::cout << "Throughput: " << number / elapsed.count() << " operations per second\n";
    }
}

void myTest::getTest(bool print) {
    auto start = std::chrono::high_resolution_clock::now();  // 开始计时

    // 执行 PUT 操作
    for (int i = 0; i < number; ++i) {
        store->get(i);
    }
    auto end = std::chrono::high_resolution_clock::now();  // 结束计时
    std::chrono::duration<double> elapsed = end - start;  // 计算总耗时
    if (print) {
        std::cout << "Total elapsed time of get : " << elapsed.count() << " seconds\n";
        std::cout << "Throughput: " << number / elapsed.count() << " operations per second\n";
    }
}


void myTest::mergeTest() {
    uint64_t maxNumber = 256 * 1024;
    uint64_t division = 4 * 1024;
    auto start = std::chrono::high_resolution_clock::now();  // 开始计时
    for (uint64_t i = 0; i < maxNumber; i++) {
        if (i % division == 0 && i != 0) {
            auto end = std::chrono::high_resolution_clock::now();  // 结束计时
            std::chrono::duration<double> elapsed = end - start;  // 计算总耗时
            std::cout << "Total elapsed time of get from " << (i - division) << " to " << i << ": " << elapsed.count()
                      << " seconds\n";
            std::cout << "Throughput: " << division / elapsed.count() << " operations per second\n";
        }
    }
}

void test1() {
    myTest test("./data", "./data/vlog");
    test.prepare();
    test.putTest();
    test.getTest();
    test.deleteTest();
}

void test2() {
    myTest test("./data", "./data/vlog");
    std::cout << "Nothing is cached" << std::endl;
//    std::cout << "Only cache the information of index" << std::endl;
//    std::cout << "Both cache the information of index and bloom filter" << std::endl;
    test.prepareAQuarter();
    test.getTest();
}

void test3() {
    myTest test("./data", "./data/vlog");
    test.mergeTest();
}

void test4_1(){
    myTest test("./data", "./data/vlog");
    test.prepareAQuarter();
    test.getTest();
}
void test4_2(){
    myTest test("./data", "./data/vlog");
    test.prepareAQuarter();
    test.putTest();
}
int main() {
    test4_1();
    test4_2();
}