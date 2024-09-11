#!/bin/bash

# 定义红色输出
RED='\033[0;31m'
NC='\033[0m' # No Color, 用于重置颜色

# 清理上一次的构建结果
make clean

# 编译所有文件
make all

# 记录./correctness的执行时间
echo "Running ./correctness ..."
start_time=$(date +%s)
./correctness
end_time=$(date +%s)
correctness_time=$((end_time - start_time))

# 记录./persistence的执行时间
echo "Running ./persistence ..."
start_time=$(date +%s)
./persistence
end_time=$(date +%s)
persistence_time=$((end_time - start_time))

# 清理构建结果
make clean

# 统一输出执行时间，并用红字显示
echo -e "${RED}Execution times:${NC}"
echo -e "${RED}./correctness execution time: $correctness_time seconds${NC}"
echo -e "${RED}./persistence execution time: $persistence_time seconds${NC}"

