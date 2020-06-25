# 编译加了libpmem库的源文件时加上-l pmem参数进行编译
# 在这个makefile中添加ycsb测试的相关编译命令，运行ycsb

all : 
	g++ ycsb.cpp -g -o ycsb_test -lpmem

clean :
	@rm -f *.o ycsb_test >/dev/null 2>&1 ../data/*