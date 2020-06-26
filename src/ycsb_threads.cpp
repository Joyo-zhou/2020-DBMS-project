#include <iostream>
#include <string>
#include <dirent.h>
#include <vector>
#include <fstream>
#include <sstream>
#include <ctime>
#include <pthread.h>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <sys/time.h>
#include "pm_ehash_threads.cpp"
using namespace std;

const string workload = "../workloads/";

const string load[] = {"1w-rw-50-50-load.txt",
                        "10w-rw-0-100-load.txt",
                        "10w-rw-25-75-load.txt",
                        "10w-rw-50-50-load.txt",
                        "10w-rw-75-25-load.txt",
                        "10w-rw-100-0-load.txt",
                        "220w-rw-50-50-load.txt"};

const string run[] = {"1w-rw-50-50-run.txt",
                        "10w-rw-0-100-run.txt",
                        "10w-rw-25-75-run.txt",
                        "10w-rw-50-50-run.txt",
                        "10w-rw-75-25-run.txt",
                        "10w-rw-100-0-run.txt",
                        "220w-rw-50-50-run.txt"};

const int load_size = 7;
const int READ_WRITE_NUM = 350000;
const int max_load = 22e5 + 10;
kv kv_pair;
uint64_t  inserted, updated, t, operation_num, key_index;
uint64_t key[max_load];
int ifInsert[max_load];
mutex m;
double max_load_time, max_run_time;

void Load(PmEHash *ehash, uint64_t times)
{
    struct timespec start, finish;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < times; ++i)
    {
        lock_guard<mutex> lock(m);
        kv_pair.value = kv_pair.key = stoi(to_string(key[key_index]).substr(0, 8));
        ehash->insert(kv_pair);
        inserted++;
        key_index ++;
    }
    clock_gettime(CLOCK_MONOTONIC, &finish);
    double single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1e9;
    max_load_time = max(max_load_time, single_time);
    // printf("load cost time %fs\n", single_time);
}

void Run(PmEHash *ehash, uint64_t times)
{
    struct timespec start, finish;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < times; ++i)
    {
        lock_guard<mutex> lock(m);
        kv_pair.value = kv_pair.key = stoi(to_string(key[key_index]).substr(0, 8));
        // printf("Run opt = %ld\n", key_index);
        if (ifInsert[key_index] == 1) {
            ehash->insert(kv_pair);
            inserted++;
        } else if (ifInsert[key_index] == 2) {
            ehash->update(kv_pair);
            updated ++;
        } else {
            ehash->search(kv_pair.key, kv_pair.value);
        }
        operation_num++;
        key_index ++;
    }
    clock_gettime(CLOCK_MONOTONIC, &finish);
    double single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1e9;
    max_run_time = max(max_run_time, single_time);
    // printf("run cost time %fs\n", single_time);
}

int main()
{
    FILE *ycsb_load, *ycsb_run;
    double single_time;

    for (size_t i = 0; i < load_size; ++i)
    {
        max_load_time = max_run_time = 0;
        t = 0;
        PmEHash *ehash = new PmEHash;

        string load_file = workload + load[i];

        printf("===== loading data from %-30s =====\n", load_file.c_str());

        ycsb_load = fopen(load_file.c_str(), "r");
        if (ycsb_load == NULL) {
            printf("load file %20s failed\n", load_file.c_str());
            exit(1);
        } else {
            printf("load file %20s success\n", load_file.c_str());
        }
        char op[20];
        uint64_t k;
        while (fscanf(ycsb_load, "%s %ld\n", op, &k) != EOF) {
            key[t] = k;
            if (op[0] == 'I') ifInsert[t] = 1;
            else if (op[0] == 'U') ifInsert[t] = 2;
            else ifInsert[t] = 0;
            t++;
        }

        fclose(ycsb_load);

        // load the workload in the PmEHash
        // change the num_threads to see changes between threads
        uint64_t num_threads = 4;
        uint64_t times = t / num_threads;

        thread th[num_threads];

        inserted = 0, updated = 0, operation_num = 0;
        key_index = 0;
        for (int i = 0; i < num_threads; ++i)
            th[i] = thread(Load, ehash, times);
            // th[i].join();

        for (int i = 0; i < num_threads; ++i)
            th[i].join();

        printf("Load phase finishes: %ld items are inserted \n", inserted);
        printf("Load phase used time: %fs\n", max_load_time);
        printf("Load phase single insert time: %fns\n", max_load_time * 1e9 / inserted);

        string run_file = workload + run[i];

        printf("\n===== runing  data  from %-30s =====\n", run_file.c_str());
        
        printf("Run phase begins\n");

        
        inserted = 0, updated = 0, operation_num = 0, t = 0, key_index = 0;
        // read the ycsb_run
        ycsb_run = fopen(run_file.c_str(), "r");
        if (ycsb_run == NULL) {
            printf("load file %20s failed\n", run_file.c_str());
            exit(1);
        } else {
            printf("load file %20s success\n", run_file.c_str());
        }
        while (fscanf(ycsb_run, "%s %ld\n", op, &k) != EOF) {
            key[t] = k;
            if (op[0] == 'I') ifInsert[t] = 1;
            else if (op[0] == 'U') ifInsert[t] = 2;
            else ifInsert[t] = 0;
            t++;
        }
        fclose(ycsb_run);
        
        thread rth[num_threads];
        times = t / num_threads;

        for (int i = 0; i < num_threads; ++i)
            rth[i] = thread(Run, ehash, times);
            // rth[i].join();

        for (int i = 0; i < num_threads; ++i)
            rth[i].join();

        printf("Run phase used time: %fs\n", max_run_time);
        printf("Run phase finishes: %ld/%ld/%ld items are inserted/updated/searched\n", inserted, updated, operation_num - inserted - updated);
        printf("Run phase throughput: %f operations per second \n", operation_num/max_run_time);
        
        printf("\n\n\n\n");

        ehash->selfDestory();
    }


}
