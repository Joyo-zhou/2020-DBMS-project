#include <iostream>
#include <string>
// #include <io.h>
// #include<sys/types.h>
#include<dirent.h>
#include <vector>
#include <fstream>
#include <sstream>
#include <ctime>

// #include "gtest/gtest.h"
#include "pm_ehash.cpp"

 // #pragma comment(linker, "/STACK:102400000,102400000")

using namespace std;

// int find_dir_file(const char *dir_name,vector<string>& v)
// {
//     DIR *dirp;
//     struct dirent *dp;
//     dirp = opendir(dir_name);
//     while ((dp = readdir(dirp)) != NULL) {
//         v.push_back(std::string(dp->d_name ));
//     }
//     (void) closedir(dirp);
//     return 0;
// }

const string workload = "../workloads/";
<<<<<<< HEAD
const string load = workload + "220w-rw-50-50-load.txt";
const string run  = workload + "220w-rw-50-50-run.txt";
=======
const string load = workload + "10w-rw-50-50-load.txt";
const string run  = workload + "10w-rw-50-50-run.txt";
>>>>>>> 167ca3bd9aa463fc96111d31dd23d92af1227d3f

const int READ_WRITE_NUM = 350000;

int main()
{
    uint64_t inserted = 0, search = 0, t = 0;
    uint64_t *key = new uint64_t[2200000];
    bool *ifInsert = new bool[2200000];
    FILE *ycsb_load, *ycsb_run;
    char *buf = NULL;
    size_t len = 0;
    struct timespec start, finish;
    double single_time;

    PmEHash *ehash = new PmEHash;

    printf("===== load =====\n");

    ycsb_load = fopen(load.c_str(), "r");
    if (ycsb_load == NULL) {
        printf("read load file failed\n");
        exit(1);
    } else {
        printf("read load file success\n");
    }
<<<<<<< HEAD
    char op[7], num[20];
=======
    char op[7];
>>>>>>> 167ca3bd9aa463fc96111d31dd23d92af1227d3f
    uint64_t k;
    while (fscanf(ycsb_load, "%s %ld\n", op, &k) != EOF) {
        key[t] = k;
        if (strcmp("INSERT", op) == 0) ifInsert[t] = true;
        else ifInsert[t] = false;
        t++;
    }

    fclose(ycsb_load);

    kv kv_pair;

    clock_gettime(CLOCK_MONOTONIC, &start);

    // load the workload in the fptree
    for (int i = 0; i < t; ++ i) {
        // printf("fptree insert %ld\n", key[i]);
<<<<<<< HEAD
        kv_pair.value = kv_pair.key = stoi(to_string(key[i]).substr(0, 8));
        // printf("before insert\n");
        // printf("kv_pair = {%ld, %ld}\n", kv_pair.key, kv_pair.value);
        ehash->insert(kv_pair);
        // printf("inserted = %ld, loading\n", inserted);
=======
        kv_pair.key = kv_pair.value = key[i];
        printf("before insert\n");
        printf("kv_pair = {%ld, %ld}\n", key[i], key[i]);
        ehash->insert(kv_pair);
        printf("inserted = %d, loading\n", inserted);
>>>>>>> 167ca3bd9aa463fc96111d31dd23d92af1227d3f
        inserted++;
    }    

    clock_gettime(CLOCK_MONOTONIC, &finish);
    single_time = (finish.tv_sec - start.tv_sec) * 1000000000.0 + (finish.tv_nsec - start.tv_nsec);
    printf("Load phase finishes: %ld items are inserted \n", inserted);
    printf("Load phase used time: %fs\n", single_time / 1000000000.0);
    printf("Load phase single insert time: %fns\n", single_time / inserted);


    printf("Run phase begins\n");

    int operation_num = 0;
    inserted = 0;
    // read the ycsb_run
    t = 0;
    ycsb_run = fopen(run.c_str(), "r");
    if (ycsb_run == NULL) {
        printf("read run file failed\n");
        exit(1);
    } else {
        printf("read run file success\n");
    }
    while (fscanf(ycsb_run, "%s %ld\n", op, &k) != EOF) {
        key[t] = k;
        if (strcmp("INSERT", op) == 0) ifInsert[t] = true;
        else ifInsert[t] = false;
        t++;
    }
    fclose(ycsb_run);

    clock_gettime(CLOCK_MONOTONIC, &start);

    // operate the fptree
    uint64_t value;
    // uint64_t max_value = MAX_VALUE;
    for (int i = 0; i < t; ++ i) {
        operation_num++;
<<<<<<< HEAD
        kv_pair.value = kv_pair.key = stoi(to_string(key[i]).substr(0, 8));
=======
        kv_pair.key = kv_pair.value = key[i];
>>>>>>> 167ca3bd9aa463fc96111d31dd23d92af1227d3f
        if (ifInsert[i]) {
            ehash->insert(kv_pair);
            inserted++;
        } else {
            ehash->search(kv_pair.key, kv_pair.value);
<<<<<<< HEAD
=======
            // if (value == max_value || value != key[i]) {
            //     cout << key[i] << " read failed" << endl;
            // }
>>>>>>> 167ca3bd9aa463fc96111d31dd23d92af1227d3f
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &finish);
    single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf("Run phase finishes: %ld/%ld items are inserted/searched\n", inserted, operation_num - inserted);
    printf("Run phase throughput: %f operations per second \n", READ_WRITE_NUM/single_time);

    ehash->selfDestory();
<<<<<<< HEAD
}
=======
}
>>>>>>> 167ca3bd9aa463fc96111d31dd23d92af1227d3f
