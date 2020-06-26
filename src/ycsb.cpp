#include <iostream>
#include <string>
#include <dirent.h>
#include <vector>
#include <fstream>
#include <sstream>
#include <ctime>
#include"omp.h"  
#include "pm_ehash.cpp"
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

int main()
{
    uint64_t inserted = 0, updated = 0, t = 0;
    uint64_t *key = new uint64_t[max_load];
    int *ifInsert = new int[max_load];
    FILE *ycsb_load, *ycsb_run;
    struct timespec start, finish;
    double single_time;

    for (size_t i = 0; i < load_size; ++i)
    {
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

        kv kv_pair;

        // clock_gettime counts in nanoseconds(ns) = 1e-9 (s)
        clock_gettime(CLOCK_MONOTONIC, &start);

        // load the workload in the PmEHash
        // #pragma omp parallel for num_threads(2)  
        for (size_t i = 0; i < t; ++ i) {
            // printf("inserted %ld\n", i);
            kv_pair.value = kv_pair.key = stoi(to_string(key[i]).substr(0, 8));
            ehash->insert(kv_pair);
            inserted++;
        }    

        clock_gettime(CLOCK_MONOTONIC, &finish);
        single_time = (finish.tv_sec - start.tv_sec) * 1e9 + (finish.tv_nsec - start.tv_nsec);
        printf("Load phase finishes: %ld items are inserted \n", inserted);
        printf("Load phase used time: %fs\n", single_time / 1e9);
        printf("Load phase single insert time: %fns\n", single_time / inserted);

        string run_file = workload + run[i];

        printf("\n===== runing  data  from %-30s =====\n", run_file.c_str());
        
        printf("Run phase begins\n");

        int operation_num = 0;
        inserted = 0, updated = 0;
        t = 0;
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

        clock_gettime(CLOCK_MONOTONIC, &start);

        // operate the PmEHash
        // #pragma omp parallel for num_threads(2)  
        for (size_t i = 0; i < t; ++ i) {
            operation_num++;
            kv_pair.value = kv_pair.key = stoi(to_string(key[i]).substr(0, 8));
            // printf("opt = %ld\n", i);
            if (ifInsert[i] == 1) {
                ehash->insert(kv_pair);
                inserted++;
            } else if (ifInsert[i] == 2) {
                ehash->update(kv_pair);
                updated ++;
            } else {
                ehash->search(kv_pair.key, kv_pair.value);
            }
        }

        clock_gettime(CLOCK_MONOTONIC, &finish);
        double run_use_time = single_time = (finish.tv_sec - start.tv_sec) * 1e9 + (finish.tv_nsec - start.tv_nsec);
        single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1e9;
        printf("Run phase used time: %fs\n", run_use_time / 1e9);
        printf("Run phase finishes: %ld/%ld/%ld items are inserted/updated/searched\n", inserted, updated, operation_num - inserted - updated);
        printf("Run phase throughput: %f operations per second \n", operation_num/single_time);
        
        printf("\n\n\n\n");

        ehash->selfDestory();
    }


}