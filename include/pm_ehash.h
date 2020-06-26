#ifndef _PM_E_HASH_H
#define _PM_E_HASH_H

#include<cstdint>
#include<queue>
#include<map>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <libpmem.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#endif
#include <string>
#include <string.h>
#include <mutex>


#define BUCKET_SLOT_NUM               15
#define DEFAULT_CATALOG_SIZE      16
#define META_NAME                                "pm_ehash_metadata"
#define CATALOG_NAME                        "pm_ehash_catalog"
#define PM_EHASH_DIRECTORY        "/mnt/pmemdir/"        // add your own directory path to store the pm_ehash
// #define PM_EHASH_DIRECTORY        "/home/feng/Downloads/2020-DBMS-project-master/data/"        // add your own directory path to store the pm_ehash

#define meta_path PM_EHASH_DIRECTORY META_NAME
#define catalog_path PM_EHASH_DIRECTORY CATALOG_NAME

#define BUCKET_SIZE 256

#define setbit(x,y) x|=(1<<y) //将X的第Y位置1
#define clrbit(x,y) x&=~(1<<y) //将X的第Y位清0

using std::queue;
using std::map;
using std::to_string;
using std::string;

/* 
---the physical address of data in NVM---
fileId: 1-N, the data page name
offset: data offset in the file
*/
// 8 bytes
typedef struct pm_address
{
    uint32_t fileId;
    uint32_t offset;

    bool operator<(const pm_address &p) const;
} pm_address;

/*
the data entry stored by the  ehash
16 bytes
*/
typedef struct kv
{
    uint64_t key;
    uint64_t value;
} kv;

//256 bytes
typedef struct pm_bucket
{
    uint64_t local_depth;
    uint8_t  bitmap[BUCKET_SLOT_NUM / 8 + 1];      // one bit for each slot
    
    kv       slot[BUCKET_SLOT_NUM];                                // one slot for one kv-pair
} pm_bucket;

// in ehash_catalog, the virtual address of buckets_pm_address[n] is stored in buckets_virtual_address
// buckets_pm_address: open catalog file and store the virtual address of file
// buckets_virtual_address: store virtual address of bucket that each buckets_pm_address points to
// 8 bytes
typedef struct ehash_catalog
{
    pm_address* buckets_pm_address;         // pm address array of buckets
    pm_bucket** buckets_virtual_address;    // virtual address of buckets that buckets_pm_address point to
} ehash_catalog;

// 24 bytes
typedef struct ehash_metadata
{
    uint64_t max_file_id;      // next file id that can be allocated
    uint64_t catalog_size;     // the catalog size of catalog file(amount of data entry)
    uint64_t global_depth;   // global depth of PmEHash
} ehash_metadata;

#include"data_page.h"

class PmEHash
{
private:
    
    ehash_metadata*                               metadata;                    // virtual address of metadata, mapping the metadata file
    ehash_catalog                                      catalog;                        // the catalog of hash

    std::mutex mutex_;
    queue<pm_bucket*>                         free_list;                      //all free slots in data pages to store buckets
    map<pm_bucket*, pm_address> vAddr2pmAddr;       // map virtual address to pm_address, used to find specific pm_address
    map<pm_address, pm_bucket*> pmAddr2vAddr;       // map pm_address to virtual address, used to find specific virtual address
    

    bool isEmpty(pm_bucket *p);
    bool isFull(pm_bucket *p);

    uint64_t hashFunc(uint64_t key);
    uint64_t getBucketIndex(uint64_t key);

    pm_bucket* getFreeBucket(uint64_t key);
    // pm_bucket* getNewBucket();
    void freeEmptyBucket(pm_bucket* bucket);
    int getFreeKvSlot(pm_bucket* bucket);
    int getKvPlace(pm_bucket *p, uint64_t key);

    void splitBucket(uint64_t bucket_id);
    void mergeBucket(uint64_t bucket_id);

    void extendCatalog();
    void* getFreeSlot(pm_address& new_address);
    void allocNewPage();

    void recover();
    void mapAllPage();
    uint64_t getActualBucket(uint64_t bucket_id);
    void WirteMemory();

public:
    PmEHash();
    ~PmEHash();

    int insert(kv new_kv_pair);
    int remove(uint64_t key);
    int update(kv kv_pair);
    int search(uint64_t key, uint64_t& return_val);

    void printCatalog();
    void printMap();

    void selfDestory();
};

#endif
