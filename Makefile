
CXX = g++
CXXFLAGS = -std=c++20 -Wall -I./
LINK.o = $(LINK.cc)

# 对象文件列表
OBJS = kvstore.o memtable.o vlog.o sstable.o sstheader.o sstindex.o
# 你的所有可执行文件
EXECS = correctness persistence

all: $(EXECS)

# 依赖于所有对象文件的目标
correctness: correctness.o $(OBJS)
persistence: persistence.o $(OBJS)

# 通用规则，将.cc编译成.o
%.o: %.cc
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	-rm -f $(EXECS) *.o
	-rm -f ./data/level-*/*.sst
	-rm -f ./WAL.log
	-rm -f ./data/vlog

