all : ycsb_test ycsb_threads_test

ycsb_test : pm_ehash.cpp
	g++ ycsb.cpp -g -o ycsb_test -lpmem

ycsb_threads_test : pm_ehash_threads.cpp
	g++ ycsb_threads.cpp -g -o ycsb_threads_test -lpmem -pthread

clean :
	@rm -f *.o ycsb_test ycsb_threads_test >/dev/null 2>&1 ../data/*
