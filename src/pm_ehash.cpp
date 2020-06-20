#include"pm_ehash.h"



bool pm_address::operator<(const pm_address &p) const { //注意这里的两个const
    return (fileId < p.fileId) || ((fileId == p.fileId) && offset < p.offset);
}

/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {
	int is_pmem;
	size_t map_len;

	// 如果map metadata文件不为NULL,则代表数据文件夹下无旧哈希的数据
	// 判断条件待测试
	if ((metadata = (ehash_metadata *)pmem_map_file(meta_path, sizeof(ehash_metadata),
			 	PMEM_FILE_CREATE|PMEM_FILE_EXCL,
			 	 0777, &map_len, &is_pmem)) != NULL) {
    	//     新建目录和元数据文件并映射		
    	// metadata = pmem_map_file(mypath, sizeof(ehash_metadata), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
    	metadata->max_file_id = 1;
    	metadata->catalog_size = DEFAULT_CATALOG_SIZE;
    	metadata->global_depth = 4;
    	
    	// printf("meta_path = %s\n", meta_path);

    	// 目录不是pmem,所以直接调用pmem_msync()
		pmem_msync(metadata, map_len);

		// ehash_catalog c;
		catalog.buckets_pm_address = new pm_address[metadata->catalog_size];
		catalog.buckets_virtual_address = new pm_bucket *[metadata->catalog_size];

		// invalid pm_adress = {0, 0};
		memset(catalog.buckets_pm_address, 0, sizeof(pm_address) * metadata->catalog_size);
		memset(catalog.buckets_virtual_address, 0, sizeof(pm_bucket *) * metadata->catalog_size);

		void *pmemaddr;

		// printf("catalog_path = %s\n", catalog_path);

		// 由于每次重启后buckets对应的虚拟地址都会改变,所以只存取catalog.buckets_pm_address
		if ((pmemaddr = pmem_map_file(catalog_path, sizeof(pm_address) * metadata->catalog_size, PMEM_FILE_CREATE,
			0777, &map_len, &is_pmem)) == NULL) {
			perror("pmem_map_file");
			exit(1);
		}

		// copy to mmap
		memcpy(pmemaddr, catalog.buckets_pm_address, sizeof(ehash_catalog) * metadata->catalog_size / 2);

		// flush to catalog_path
		pmem_msync(pmemaddr, map_len);

		// metadata的生存域是整个PmEhash,所以生成后不需要调用pmem_unmap()
	}
	// 如果为NULL的话,则代表数据文件夹下有旧哈希的数据,调用recover()
	else {
		// printf("meta_path = %s exists.\n", meta_path);
		recover();
	}
}

/**
 * @description: persist and munmap all data in NVM
 * @param NULL 
 * @return: NULL
 */
PmEHash::~PmEHash() {
	// 相当于selfDestory()
	selfDestory();
}

uint64_t PmEHash::getBucketIndex(uint64_t key) {
	return (key & ((1 << metadata->global_depth) - 1));
}

/**
 * @description: 桶内查找key的存放位置,查找到就返回index,否则返回-1
 * @param : 待寻找的桶 p, 和key
 * @return: $index if found, -1 if not found
 */
int PmEHash::getKvPlace(pm_bucket *p, uint64_t key) {
	int bitmap = (p->bitmap[0]) + (p->bitmap[1] << 8);
	for (int i = 0; i < BUCKET_SLOT_NUM; ++i)
	{
		if (((bitmap >> i) & 1) && (p->slot[i].key == key))
			return i;
	}
	return -1;
}

/**
 * @description: 判断一个桶是否为空
 * @param kv: 需要判断的桶
 * @return: ture if empty, false if not empty
 */
bool PmEHash::isEmpty(pm_bucket *p) {
	int bitmap = (p->bitmap[0]) + (p->bitmap[1] << 8);
	for (int i = 0; i < BUCKET_SLOT_NUM; ++i)
	{
		if (((bitmap >> i) & 1))
			return true;
	}
	return false;
}

/**
 * @description: 判断一个桶是否为满
 * @param kv: 需要判断的桶
 * @return: ture if full, false if not full
 */
bool PmEHash::isFull(pm_bucket *p) {
	int bitmap = (p->bitmap[0]) + (p->bitmap[1] << 8);
	for (int i = 0; i < BUCKET_SLOT_NUM; ++i)
	{
		if (!((bitmap >> i) & 1))
			return true;
	}
	return false;
}

/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
	uint64_t value;
	// uint64_t index = getBucketIndex(new_kv_pair.key);
	// 如果查找成功(= 0),说明有该值存在,不属于新键值对
    if (search(new_kv_pair.key, value) == 0)
        return -1;

    // 否则就找到 key 对应的桶
    pm_bucket* bucket = getFreeBucket(new_kv_pair.key);

    // 查找该桶的第一个未被占用的slot位
    int targetIndex = getFreeKvSlot(bucket, new_kv_pair.key);

    // 更改slot的kv
    // *freePlace = new_kv_pair;
    bucket->slot[targetIndex] = kv;

    // 将bitmap置1
    // bitmap(freePlace) = 1;
    int bitmap = (bucket->bitmap[0]) + (bucket->bitmap[1] << 8); 
    
    // 将bitmap的targetIndex位置1
    setbit(bitmap, targetIndex);

    // 将bitmap的新值memcpy回去
    memcpy(bucket->bitmap, &bitmap, sizeof(uint8_t) * 2);

    // 说不定要调用pmem_msync()
    
    return 0;
}

/**
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
	uint64_t value;

	uint64_t index = getBucketIndex(key);

	// 如果查找成功(= -1),说明该值不存在,直接返回-1,说明没有remove
    // if (search(key, value) == -1)
    //     return -1;

    // 如果key存在, 获得key对应的桶
	pm_bucket* bucket = catalog.buckets_virtual_address[index];
	
    // 找到桶中key对应的位置
    int targetIndex = getKvPlace(bucket, kv_pair.key);

    // 执行remove操作
    // remove(key, value) ==> bitmap(targetPlace) = 0;
    if (targetIndex == -1)
    	return -1;

    int bitmap = (bucket->bitmap[0]) + (bucket->bitmap[1] << 8); 

    // 使用宏命令将bitmap的targetIndex位清零
    clrbit(bitmap, targetIndex);

    // 再copy到对应的bitmap所在的内存
    memcpy(bucket->bitmap, bitmap, sizeof(uint8_t) * 2);

    // 说不定要调用pmem_msync()

    // 如果remove操作后桶为空,那么调用mergeBucket()
    if (isEmpty(bucket))
    	mergeBucket(index);

    return 0;
}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
	uint64_t value;

	uint64_t index = getBucketIndex(kv_pair.key);

	// 如果不存在,说明不是现存的键值对,不进行操作,直接返回-1代表未修改(数据不存在)
	if (search(kv_pair.key, value) == -1)
		return -1;

	// 存在就找到key存放的index
	pm_bucket* bucket = catalog.buckets_virtual_address[index];
	int targetIndex = getKvPlace(bucket, kv_pair.key);

	// 修改index所在的kv对应的value
	bucket->slot[targetIndex].value = kv_pair.value;

	// 返回0代表已修改
    return 0;
}
/**
 * @description: 查找目标键值对数据，将返回值放在参数里的引用类型进行返回
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist) 
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {

	uint64_t index = getBucketIndex(key);

	// 查找key对应的bucket
	pm_bucket* bucket = catalog.buckets_virtual_address[index];

	// 返回bucket的search
    int targetIndex = getKvPlace(bucket, key);

    if (targetIndex == -1)
    	// 不存在key就返回-1
    	return -1;

    // 存在就修改return_val 并返回0
	return_val = bucket->solt[targetIndex].value;

    return 0;
}

/**
 * @description: 用于对输入的键产生哈希值，然后取模求桶号(自己挑选合适的哈希函数处理)
 * @param uint64_t: 输入的键
 * @return: 返回键所属的桶号
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
	return std::hash<uint64_t>()(key);
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 带插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
	uint64_t index = getBucketIndex(key);
	pm_bucket* bucket = catalog.buckets_virtual_address[index];

	// 获得桶,判断桶是否为满，满就调用splitBucket();
	if (isFull(bucket))
		splitBucket(index);

	// 返回桶指针
	return bucket;
}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */
int PmEHash::getFreeKvSlot(pm_bucket* bucket) {
	int bitmap = (bucket->bitmap[0]) + (bucket->bitmap[1] << 8);
	size_t index;
	for (index = 0; index < BUCKET_SLOT_NUM; ++index)
	{
		// bitmap上该位为0,说明空闲
		// 返回index对应的slot,里面可以存放一个kv
		if (!((bitmap >> index) & 1))
			return index;
	}
	
	return -1;
	// 找到桶的第一个bit为0的位置
	// 返回位置指针
}

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
	// 获得需要分裂的桶的index,其实index对应的是输入的参数 bucket_id
	// uint64_t index = getBucketIndex(key);

	// 拿到旧桶
	pm_bucket* old_bucket = catalog.buckets_virtual_address[bucket_id];

	// 局部深度比较, = 全局深度调用extendCatalog(), < 不做操作
	if (old_bucket->local_depth == metadata->global_depth)
	{
		// 目录重新映射
		extendCatalog();
	}

	// 从free_list拿一个桶空间
	pm_address new_buckets_pm_address;
	pm_bucket *new_bucket = getFreeSlot(buckets_pm_address);

	// 旧桶局部深部自增
	old_bucket->local_depth ++;
	
	// 新桶初始化
	memset(new_bucket, 0, sizeof(new_bucket));
	new_bucket->local_depth = old_bucket->local_depth;

	// 桶指针重新分配
	uint64_t new_index = bucket_id + (1 << new_bucket->local_depth);
	catalog.buckets_virtual_address[new_index] = new_bucket;
	catalog.buckets_pm_address[new_index] = new_buckets_pm_address;

	// 桶数据重新哈希
	// 先把旧桶的bitmap全置零
	memset(old_bucket->bitmap, 0, sizeof(old_bucket->bitmap));
	for (int i = 0; i < BUCKET_SLOT_NUM; ++i)
	{
		insert(old_bucket->slot[i]);
	}
}

/**
 * @description: 桶空后，回收桶的空间，并设置相应目录项指针
 * @param uint64_t: 桶号
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
    // 与兄弟桶合并,局部深度减一,加入free_list
	// 可以是index小的桶先空,也可以是index大的桶先空,所以需要判断
	pm_bucket* empty_bucket = catalog.buckets_virtual_address[bucket_id];
	pm_address empty_pm_address = catalog.buckets_pm_address[bucket_id];
	uint64_t empty_local_depth = empty_bucket->local_depth;

	bool is_big = bucket_id & (1 << empty_local_depth);
	uint64_t brother_id = (is_big ? bucket - (1 << empty_local_depth) : bucket_id + (1 << empty_local_depth));
	pm_bucket* brother_bucket = catalog.buckets_virtual_address[brother_id];
	pm_address brother_pm_address = catalog.buckets_pm_address[brother_id];

	// 如果是index大的桶空了,那么只需要设置目录指针,然后回收空间
	// 如果是index小的桶空了,需要把大的桶的内容复制到小桶中,然后回收大桶的空间

	// 空的桶是小桶,需要复制大桶的内容到小桶中,并设置目录指针和回收大桶空间
	if (!is_big)
	{
		memcpy(empty_bucket, brother_bucket, sizeof(pm_bucket));

		// 设置目录指针,并且局部深度减一
		catalog.buckets_virtual_address[brother_id] = empty_bucket;
		catalog.buckets_pm_address[brother_id] = empty_pm_address;
		empty_bucket->local_depth --;

		// 将即将回收的桶清零, 可以不清
		memset(brother_bucket, 0, sizeof(pm_bucket));
		free_list.push(brother_bucket);
	}
	// 空的桶是大桶,不需要复制内容,但需要设置目录指针和回收大桶空间
	else
	{
		// 设置目录指针,并且局部深度减一
		catalog.buckets_virtual_address[bucket_id] = brother_bucket;
		catalog.buckets_pm_address[bucket_id] = brother_pm_address;		
		brother_bucket->local_depth --;

		memset(empty_bucket, 0, sizeof(pm_bucket));
		free_list.push(empty_bucket);
	}
}

/**
 * @description: 对目录进行倍增，需要重新生成新的目录文件并复制旧值，然后删除旧的目录文件
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {
	// 目录申请两倍空间
	pm_ehash_catalog new_catalog;
	new_catalog.buckets_pm_address = new pm_address[metadata->catalog_size * 2];
	new_catalog.buckets_virtual_address = new pm_bucket*[metadata->catalog_size * 2];

	// 复制旧值
	memcpy(new_catalog.buckets_pm_address, catalog.buckets_pm_address, sizeof(pm_address) * metadata->catalog_size);
	memcpy(new_catalog.buckets_virtual_address, catalog.buckets_virtual_address, sizeof(pm_bucket *) * metadata->catalog_size);

	// 新增的指针设置
	for (int i = 0; i < metadata->catalog_size; ++i)
	{
		new_catalog.buckets_pm_address[i + metadata->catalog_size] = new_catalog.buckets_pm_address[i];
		new_catalog.buckets_virtual_address[i + metadata->catalog_size] = new_catalog.buckets_virtual_address[i];
	}

	// 目录size翻倍
	metadata->catalog_size *= 2;

	size_t map_len;
	int is_pmem;

	// 删除旧文件,调用pmem_map_file打开已经翻倍的catalog,并将内容覆盖进去
	// 此处metadata->catalog_size 已经是新值
	void *pmemaddr = pmem_map_file(catalog_path, sizeof(ehash_catalog) * metadata->catalog_size, 
		PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);

	// 复制新目录到新打开的大目录文件映射的内存中
	memcpy(pmemaddr, &new_catalog, sizeof(ehash_catalog) * metadata->catalog_size);

	// 从映射的内存刷新目录文件的内容
	pmem_msync(pmemaddr, map_len);
}

/**
 * @description: 获得一个可用的数据页的新槽位供哈希桶使用，如果没有则先申请新的数据页
 * @param pm_address&: 新槽位的持久化文件地址，作为引用参数返回
 * @return: 新槽位的虚拟地址
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {
	// 判断free_list.empty(), free_list为空则调用allocNewPage()
	if (free_list.empty())
		allocNewPage();
	// 然后 auto bucket = free_list.front(); free_list.pop();
	auto bucket = free_list.front(); free_list.pop();
	// 更新桶对应的pm_address
	new_address = vAddr2pmAddr[bucket];
	// return bucket;
	return bucket;
}

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
	void *pmemaddr;
	size_t mapped_len;
	int is_pmem;

	int it = metadata->max_file_id;
	const char *page_name = to_string(it).c_str();

	// 申请新空间，开辟新文件"$max_file_id"
	if ((pmemaddr = pmem_map_file(page_name, sizeof(data_page), PMEM_FILE_CREATE,
				0666, &mapped_len, &is_pmem)) == NULL) {
		perror("pmem_map_file");
		exit(1);
	}	

	data_page *p = (data_page *)pmemaddr;
	memset(p, 0, sizeof(data_page));

	// 刷新文件值
	pmem_msync(pmemaddr, mapped_len);

	pm_address tmp = {it, 0};

    // map<pm_bucket*, pm_address> vAddr2pmAddr;       // map virtual address to pm_address, used to find specific pm_address
    // map<pm_address, pm_bucket*> pmAddr2vAddr;       // map pm_address to virtual address, used to find specific virtual address

	// 计算文件"$max_file_id"内的slot个数和地址，加入free_list
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i)
	{
		// 将新产生的所有bucket都加入map映射
		pmAddr2vAddr[tmp] = &p->buckets[i];
		vAddr2pmAddr[&p->buckets[i]] = tmp;
		tmp.offset += BUCKET_SIZE;
		free_list.push(&p->buckets[i]);
		// printf("p->buckets[%d] = %p\n", i, &p->buckets[i]);
	}

	// 然后使max_file_id自增
	metadata->max_file_id ++;
}

/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {
	size_t map_len;
	int is_pmem;
	// const char* meta_path = PM_EHASH_DIRECTORY META_NAME;
	// 读取metadata文件中的数据并内存映射 读取max_file_id
	metadata = pmem_map_file(meta_path, sizeof(ehash_metadata), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);

    // 读取catalog文件中的数据并内存映射 
	ehash_catalog *catalog_temp = pmem_map_file(catalog_path, sizeof(pm_address) * metadata->catalog_size, 
		PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);

	catalog.buckets_pm_address = new pm_address[metadata->catalog_size];
	catalog.buckets_virtual_address = new (pm_bucket *)[metadata->catalog_size];

	// catalog = *catalog_temp;
	memcpy(catalog.buckets_pm_address, catalog_temp, sizeof(pm_address) * metadata->catalog_size);
	memset(catalog.buckets_virtual_address, 0, sizeof(pm_bucket *) * metadata->catalog_size);

    // 读取所有数据页文件并内存映射 mapAllPage()
    mapAllPage();

    // 设置可扩展哈希的桶的虚拟地址指针
    for (int i = 0; i < metadata->catalog_size; ++i)
    {
    	// if isValid(pm_address[i])
    	if (catalog.buckets_pm_address[i] != {0, 0})
    		catalog.buckets_virtual_address[i] = pmAddr2vAddr[catalog.buckets_pm_address[i]];
    }

    // 初始化所有其他可扩展哈希的内存数据
    // ?
}

/**
 * @description: 重启时，将所有数据页进行内存映射，设置地址间的映射关系，空闲的和使用的槽位都需要设置 
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {
	// 从文件1读取到max_file_id - 1
	int page_index = 0;

	void *pmemaddr[metadata->max_file_id];
	size_t mapped_len;
	int is_pmem;

	while (++ page_index < metadata->max_file_id)
	{
		const char *page_name = to_string(page_index).c_str();

		// 申请新空间，开辟新文件"$max_file_id"
		if ((pmemaddr[page_index-1] = pmem_map_file(page_name, sizeof(data_page), PMEM_FILE_CREATE,
					0666, &mapped_len, &is_pmem)) == NULL) {
			perror("pmem_map_file");
			exit(1);
		}

		data_page *p = (data_page *)pmemaddr[page_index-1];
		// memset(p, 0, sizeof(data_page));

		// 刷新文件值
		// pmem_msync(pmemaddr, mapped_len);

		pm_address tmp = {page_index, 0};

	    // map<pm_bucket*, pm_address> vAddr2pmAddr;       // map virtual address to pm_address, used to find specific pm_address
	    // map<pm_address, pm_bucket*> pmAddr2vAddr;       // map pm_address to virtual address, used to find specific virtual address

		int bitmap = (p->bitmap[0]) + (p->bitmap[1] << 8);

		// 计算文件"$max_file_id"内的slot个数和地址，加入free_list
		for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i)
		{
			// 将新产生的所有bucket都加入map映射
			pmAddr2vAddr[tmp] = &p->buckets[i];
			vAddr2pmAddr[&p->buckets[i]] = tmp;
			tmp.offset += BUCKET_SIZE;

			// 该bitmap位为0,表示未被占用
			// 未占用的桶加入free_list
			if (!((bitmap >> i) & 1))
				free_list.push(&p->buckets[i]);
			// printf("p->buckets[%d] = %p\n", i, &p->buckets[i]);
		}
	}

	// 进行内存映射,将bitmap上已占用的桶进行初始化
	// ?
}

/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {
	// 持久化所有映射的数据
	// metadata
	pmem_msync(metadata, sizeof(ehash_metadata));

	// catalog
	int is_pmem;
	size_t map_len;
	ehash_catalog *catalog_temp = pmem_map_file(catalog_path, sizeof(ehash_catalog) * metadata->catalog_size, 
		PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);

	memcpy(catalog_temp, &catalog, sizeof(catalog) * metadata->catalog_size);

	pmem_msync(catalog_temp, sizeof(catalog) * metadata->catalog_size);


	// all page
	size_t index = 0;
	pm_address tmp = {index, 0};
	while(++ index < metadata->max_file_id)
	{
		pmem_msync(pmAddr2vAddr[tmp], sizeof(data_page));
		pmem_unmap(pmAddr2vAddr[tmp], sizeof(data_page));
	}

	// 解除所有内存映射
	pmem_unmap(catalog_temp, sizeof(catalog) * metadata->catalog_size);
	
	pmem_unmap(metadata, sizeof(ehash_metadata));

	vAddr2pmAddr.clear();
	pmAddr2vAddr.clear();
	while(!free_list.empty())
		free_list.pop();

	// 刷新磁盘内容
	// 清空所有内存数据
	// 解除所有内存映射
}