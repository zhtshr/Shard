
#include "hopscotch.h"
#include "smart/task.h"
#include "smart/backoff.h"

#define IS_BLOCKING
#define ENABLE_READ_DELEGATION
#define ENABLE_WRITE_COMBINING

thread_local sds::Backoff tl_backoff;

std::atomic<bool> lock1;
std::atomic<bool> lock2;
std::unordered_map<uint64_t, int> bin_map1;
std::unordered_map<uint64_t, int> bin_map2;

RemoteHopscotch::RemoteHopscotch(JsonConfig config, int max_threads) : config_(config), is_shared_(false) {
    node_ = new Initiator();
    if (!node_) {
        exit(EXIT_FAILURE);
    }

    node_id_ = 0;
    auto entry = config.get("memory_servers").get(node_id_);
    std::string domain_name = entry.get("hostname").get_str();
    uint16_t tcp_port = (uint16_t) entry.get("port").get_int64();
    if (node_->connect(node_id_, domain_name.c_str(), tcp_port, max_threads)) {
        exit(EXIT_FAILURE);
    }

    if (node_->get_root_entry(node_id_, 0, ht_addr_.raw)) {
        exit(EXIT_FAILURE);
    }

    ht_addr_.node = node_id_;

    uint64_t clients = config.get("client_num").get_uint64();
    uint64_t threads = config.get("nr_threads").get_uint64();
    uint64_t tasks = config.get("tasks_per_thread").get_uint64();
    for (int i = 0; i < kMaxThreads; ++i) {
        for (int j = 0; j < kMaxTasksPerThread; ++j) {
            if (init_task(i, j, clients * threads * tasks)) {
                exit(EXIT_FAILURE);
            }
        }
    }
    
}

RemoteHopscotch::RemoteHopscotch(JsonConfig config, int max_threads, Initiator *node, int node_id) :
        config_(config), node_(node), node_id_(node_id), is_shared_(true) {
    auto entry = config.get("memory_servers").get(node_id_);
    std::string domain_name = entry.get("hostname").get_str();
    uint16_t tcp_port = (uint16_t) entry.get("port").get_int64();
    if (node_->connect(node_id_, domain_name.c_str(), tcp_port, max_threads)) {
        exit(EXIT_FAILURE);
    }

    if (node_->get_root_entry(node_id_, 0, ht_addr_.raw)) {
        exit(EXIT_FAILURE);
    }

    ht_addr_.node = node_id_;
    
    uint64_t clients = config.get("client_num").get_uint64();
    uint64_t threads = config.get("nr_threads").get_uint64();
    uint64_t tasks = config.get("tasks_per_thread").get_uint64();
    for (int i = 0; i < kMaxThreads; ++i) {
        for (int j = 0; j < kMaxTasksPerThread; ++j) {
            if (init_task(i, j, clients * threads * tasks)) {
                exit(EXIT_FAILURE);
            }
        }
    }

}

RemoteHopscotch::RemoteHopscotch(JsonConfig config, Initiator *node, int node_id) :
        config_(config), node_(node), node_id_(node_id), is_shared_(true) {
    auto entry = config.get("memory_servers").get(node_id_);
    std::string domain_name = entry.get("hostname").get_str();
    uint16_t tcp_port = (uint16_t) entry.get("port").get_int64();
    if (node_->connect(node_id_, domain_name.c_str(), tcp_port, 1)) {
        exit(EXIT_FAILURE);
    }

    if (node_->get_root_entry(node_id_, 0, ht_addr_.raw)) {
        exit(EXIT_FAILURE);
    }

    ht_addr_.node = node_id_;
    
    uint64_t clients = config.get("client_num").get_uint64();
    uint64_t threads = config.get("nr_threads").get_uint64();
    uint64_t tasks = config.get("tasks_per_thread").get_uint64();
    if (init_task(GetThreadID(), GetTaskID(), clients * threads * tasks)) {
        exit(EXIT_FAILURE);
    }
    
}

RemoteHopscotch::~RemoteHopscotch() {
    node_->disconnect(node_id_);
    if (!is_shared_) {
        delete node_;
    }
}

int RemoteHopscotch::init_task(int thread, int task, uint64_t total_tasks) {
    TaskLocal &tl = tl_data_[thread][task];
    tl.cas_buf = (uint64_t *) node_->alloc_cache(sizeof(uint64_t));
    tl.hash_table = (HashTable *) node_->alloc_cache(sizeof(HashTable));
    tl.bucket_buf = (Bucket *) node_->alloc_cache(sizeof(Bucket) * (H_BUCKET + H_BUCKET - 1));
    tl.new_bucket_buf = (Bucket *) node_->alloc_cache(sizeof(Bucket) * (H_BUCKET + H_BUCKET - 1));
    tl.block_buf = (BlockHeader *) node_->alloc_cache(BLOCK_LEN);
    assert(tl.cas_buf && tl.hash_table && tl.bucket_buf && tl.new_bucket_buf && tl.block_buf);
    if (node_->read(tl.hash_table, ht_addr_, sizeof(HashTable), Initiator::Option::Sync)) {
        return -1;
    }

    return 0;
}

int64_t RemoteHopscotch::tot_balls() {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    return tl.hash_table->balls;
}

int64_t RemoteHopscotch::tot_capacity() {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    TableMetadata &metadata = tl.hash_table->metadata;
    return metadata.buckets * BUCKET_SLOTS;
}

double RemoteHopscotch::load_factor() {
    return (double)tot_balls() / tot_capacity();
}

int RemoteHopscotch::read_hash_table() {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    rc = node_->read(tl.hash_table, ht_addr_, sizeof(HashTable), Initiator::Option::Sync);
    assert(!rc);
    return 0;
}

int RemoteHopscotch::read_bucket(uint64_t bucket) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    uint64_t bucket_addr = tl.hash_table->bucket + bucket * sizeof(Bucket);
    rc = node_->read(tl.bucket_buf, GlobalAddress(node_id_, bucket_addr), sizeof(Bucket) * H_BUCKET, Initiator::Option::Sync);
    assert(!rc);
    return 0;
}

int RemoteHopscotch::read_bucket_neighbour(uint64_t bucket) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    uint64_t bucket_addr = tl.hash_table->bucket + (bucket + H_BUCKET) * sizeof(Bucket);
    rc = node_->read(tl.bucket_buf + H_BUCKET, GlobalAddress(node_id_, bucket_addr), sizeof(Bucket) * (H_BUCKET - 1), Initiator::Option::Sync);
    assert(!rc);
    return 0;
}

int RemoteHopscotch::lock_bucket(Bucket *bucket_buf, uint64_t bucket) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    // in local: tl.hash_table->lv1_bin[block][offset].slot[slot]
    uint64_t bucket_addr = tl.hash_table->bucket + bucket * sizeof(Bucket);
    Slot old_val = bucket_buf->slot[LOCK_ID];
    Slot new_val = old_val;
    new_val.lock = 1;
    while (true) {
        rc = node_->compare_and_swap(tl.cas_buf, GlobalAddress(node_id_, bucket_addr), old_val.raw, new_val.raw, Initiator::Option::Sync);
        assert(!rc);
        if (old_val.raw == ((Slot *)tl.cas_buf)->raw) break;
        old_val.raw = *tl.cas_buf;
    }
    bucket_buf->slot[LOCK_ID].lock = 1;
    return 0;
}

int RemoteHopscotch::unlock_bucket(Bucket *bucket_buf, uint64_t bucket) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    // in local: tl.hash_table->lv1_bin[block][offset].slot[slot]
    uint64_t bucket_addr = tl.hash_table->bucket + bucket * sizeof(Bucket);
    Slot old_val = bucket_buf->slot[LOCK_ID];
    Slot new_val = old_val;
    new_val.lock = 0;
    rc = node_->compare_and_swap(tl.cas_buf, GlobalAddress(node_id_, bucket_addr), old_val.raw, new_val.raw, Initiator::Option::Sync);
    assert(!rc);
    bucket_buf->slot[LOCK_ID].lock = 0;
    return 0;
}

int RemoteHopscotch::unlock_free_bucket(Bucket *bucket_buf, uint64_t bucket, uint64_t old_bucket_addr) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    // in local: tl.hash_table->lv1_bin[block][offset].slot[slot]
    uint64_t bucket_addr = old_bucket_addr + bucket * sizeof(Bucket);
    Slot old_val = bucket_buf->slot[LOCK_ID];
    Slot new_val = old_val;
    new_val.lock = 0;
    new_val.incar = 1;
    rc = node_->compare_and_swap(tl.cas_buf, GlobalAddress(node_id_, bucket_addr), old_val.raw, new_val.raw, Initiator::Option::Sync);
    assert(!rc);
    bucket_buf->slot[LOCK_ID].raw = new_val.raw;
    return 0;
}
    



int RemoteHopscotch::search_bucket(const std::string &key, std::string &value, const Bucket bucket, uint8_t fp) {
    for (int i = 1; i < BUCKET_SLOTS; i++) {
        Slot slot = bucket.slot[i];
        if (slot.len && slot.fp == fp) {
            int rc = read_block(key, value, slot);
            if (rc == 0) {
                return 0;
            }
            assert(rc == ENOENT);
        }
    }
    return ENOENT;
}

int RemoteHopscotch::search(const std::string &key, std::string &value) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    int rc;
    uint64_t hash = hash_fun(key);
    uint64_t bucket;
    uint8_t fp;
    Bucket *bucket_buf = tl.bucket_buf;

    
    split_hash(hash, hash_table->metadata.buckets, LOG_FPRINT, bucket, fp);
retry:

    read_bucket(bucket);
    for (int i =0; i < H_BUCKET; i++) {
        if (bucket_buf[i].slot[LOCK_ID].incar) {
            read_hash_table();
            goto retry;
        }
    }
    // check no on going write and cacheline consistency
    for (int i = 0; i < H_BUCKET; i++) {
        if (bucket_buf[i].slot[LOCK_ID].lock) {
            goto retry;
        }
        if (i < H_BUCKET && bucket_buf[i].slot[LOCK_ID].fp != bucket_buf[i+1].slot[LOCK_ID].len) {
            goto retry;
        }
    }
    for (int i = 0; i < H_BUCKET; i++) {
        int res = search_bucket(key, value, bucket_buf[i], fp);
        if (res == 0) return 0;
    }

    return ENOENT;
}

int RemoteHopscotch::find_empty_slot(Slot *slot_group, uint64_t begin) {
    for (int i = begin; i < BUCKET_SLOTS; i++) {
        Slot slot = slot_group[i];
        if (!slot.len) {
            return i;
        }
    }
    return -1;
}

int RemoteHopscotch::move_slot(Slot *slot_group, uint64_t bucket_pos, uint64_t pos, uint64_t begin) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    for (int i = begin; i < BUCKET_SLOTS; i++) {
        Slot slot = slot_group[i];
        if (slot.len) {
            std::string key, value;
            int rc = read_block_kv(key, value, slot);
            if (rc != 0) {
                continue;
            }
            uint64_t hash = hash_fun(key);
            uint64_t bucket;
            uint8_t fp;

            split_hash(hash, hash_table->metadata.buckets, LOG_FPRINT, bucket, fp);
            if (bucket + H_BUCKET - 1 >= bucket_pos + H_BUCKET) {  // can move with H
                slot_group[BUCKET_SLOTS + pos].raw = slot.raw;
                slot_group[i].raw = 0;
                uint64_t bucket_addr = tl.hash_table->bucket + (bucket_pos + H_BUCKET - 1) * sizeof(Bucket);
                node_->write(slot_group, GlobalAddress(node_id_, bucket_addr), sizeof(Bucket) * H_BUCKET);
                return 0;
            }
        }
    }
    return ENOENT;
}

int RemoteHopscotch::insert_bucket(const std::string &key, Slot new_slot, Slot *slot_group, uint64_t bucket_addr, uint8_t fp, uint64_t begin) {
    ThreadLocal &thl = thread_data[GetThreadID()];
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];

    // uniqueness check
    for (int i = begin; i < BUCKET_SLOTS; i++) {
        Slot slot = slot_group[i];
        if (slot.len && slot.fp == fp) {
            std::string old_value;
            if (slot.pointer > 16777216000ll) {
                SDS_INFO("%d %d %d %ld %ld", GetThreadID(), GetTaskID(), i, tl.hash_table->bucket, bucket_addr);
                for (int j = begin; j < BUCKET_SLOTS; j++) {
                    SDS_INFO("%d %d %ld", slot_group[j].len, slot_group[j].fp, slot_group[j].pointer);
                }
            }
            int rc = read_block(key, old_value, slot);
            if (rc == 0) {
                new_slot.lock = slot.lock;
                while (atomic_update_slot(bucket_addr + sizeof(Slot) * i, slot_group + i, new_slot));
                return EEXIST;
            }
        }
    }

    for (int i = begin; i < BUCKET_SLOTS; i++) {
        Slot slot = slot_group[i];
        if (!slot.len) {
            new_slot.lock = slot.lock;
            int rc = atomic_update_slot(bucket_addr + sizeof(Slot) * i, slot_group + i, new_slot);
            if (rc == 0) {
                return 0;
            }
            else {
                // check if any concurrent thread insert the same key
                slot = slot_group[i];
                if (!slot.len || slot.fp != fp) continue;
                std::string old_value;
                int rc = read_block(key, old_value, slot);
                if (rc == 0) {
                    while (atomic_update_slot(bucket_addr + sizeof(Slot) * i, slot_group + i, new_slot));
                    return 0;
                }
            }
        }
    }
    return ENOENT;
}

int RemoteHopscotch::insert(const std::string &key, const std::string &value) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    ThreadLocal &thl = thread_data[GetThreadID()];
    HashTable *hash_table = tl.hash_table;

    Slot new_slot;
    int rc;

    uint64_t hash = hash_fun(key);
    uint64_t bucket;
    uint8_t fp;
    Bucket *bucket_buf = tl.bucket_buf;

retry:
    split_hash(hash, hash_table->metadata.buckets, LOG_FPRINT, bucket, fp);
    write_block(key, value, fp, new_slot);

    read_bucket(bucket);
    // check resize is happening
    for (int i =0; i < H_BUCKET; i++) {
        if (bucket_buf[i].slot[LOCK_ID].incar) {
            read_hash_table();
            goto retry;
        }
    }
    uint64_t bucket_addr = hash_table->bucket + bucket * sizeof(Bucket);
    int i, res;
    for (i = 0; i < H_BUCKET; i++) {
        lock_bucket(bucket_buf + i, bucket + i);
        res = insert_bucket(key, new_slot, (Slot *)(bucket_buf + i), bucket_addr + i * sizeof(Bucket), fp, 1);
        if (res != ENOENT) break;
    }
    if (res != ENOENT) {
        for (int j = 0; j <= i; j++) {
            unlock_bucket(bucket_buf + j, bucket + j);
        }
        return 0;
    }

    // need to adjust
    read_bucket_neighbour(bucket);
    int j;
    for (j = H_BUCKET + 1; j < H_BUCKET + H_BUCKET; j++) {
        lock_bucket(bucket_buf + j, bucket + j);
        int pos = find_empty_slot((Slot *)(bucket_buf+j), 1);
        if (pos != -1) {
            res = move_slot((Slot *)(bucket_buf + H_BUCKET - 1), bucket, pos, 1);
            if (res == 0) break;
        }
    }
    if (res == 0) {
        for (int k = 0; k <= j; k++) {
            unlock_bucket(bucket_buf + k, bucket + k);
        }
        return 0;
    }

    resize();
    goto retry;

    SDS_INFO("insert overflow!, key %s, load factor %.2lf", key.c_str(), load_factor());

    return 0;
}

int RemoteHopscotch::update_bucket(const std::string &key, Bucket *bucket, Slot new_slot, uint8_t fp, uint64_t bucket_addr) {
    for (int i = 1; i < BUCKET_SLOTS; i++) {
        Slot slot = bucket->slot[i];
        if (slot.len && slot.fp == fp) {
            std::string old_value;
            int rc = read_block(key, old_value, slot);
            if (rc == 0) {
                while (atomic_update_slot(bucket_addr + sizeof(Slot) * i, &bucket->slot[i], new_slot));
                return 0;
            }
            assert(rc == ENOENT);
        }
    }
    return ENOENT;
}


int RemoteHopscotch::update(const std::string &key, const std::string &value) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    ThreadLocal &thl = thread_data[GetThreadID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;

    int rc;
    uint64_t hash = hash_fun(key);
    uint64_t bucket;
    uint8_t fp;
    Slot new_slot;
    Bucket *bucket_buf = tl.bucket_buf;

    
retry:
    split_hash(hash, hash_table->metadata.buckets, LOG_FPRINT, bucket, fp);
    write_block(key, value, fp, new_slot);

    read_bucket(bucket);
    for (int i =0; i < H_BUCKET; i++) {
        if (bucket_buf[i].slot[LOCK_ID].incar) {
            read_hash_table();
            goto retry;
        }
    }
    // check no on going write and cacheline consistency
    for (int i = 0; i < H_BUCKET; i++) {
        if (bucket_buf[i].slot[LOCK_ID].lock) {
            goto retry;
        }
        if (i < H_BUCKET && bucket_buf[i].slot[LOCK_ID].fp != bucket_buf[i+1].slot[LOCK_ID].len) {
            goto retry;
        }
    }
    uint64_t bucket_addr = hash_table->bucket + bucket * sizeof(Bucket);
    for (int i = 0; i < H_BUCKET; i++) {
        int res = update_bucket(key, &bucket_buf[i], new_slot, fp, bucket_addr);
        if (res == 0) return 0;
    }
    
    return ENOENT;
}

int RemoteHopscotch::rmw(const std::string &key, const std::function<std::string(const std::string &)> &transform) {
    uint64_t slot_addr = 0;
    return 0;
    Slot new_slot, *slot;
    BackoffGuard guard(tl_backoff);
    int rc;

    while (true) {
        std::string old_value;
        
        auto value = transform(old_value);
        GlobalAddress addr;
        write_block(key, value, 0, new_slot, addr);
        rc = atomic_update_slot(slot_addr, slot, new_slot);
        if (rc == 0) {
            return 0;
        }
        size_t block_len = get_kv_block_len(key, value);
        node_->free_memory(addr, block_len);
        guard.retry_task();
    }
    return 0;
}

int RemoteHopscotch::remove_bucket(const std::string &key, BackoffGuard &guard, int id, Slot *slot_group, uint64_t bin_addr, int slots, int begin, uint8_t fp) {
    ThreadLocal &thl = thread_data[GetThreadID()];
    Slot new_slot;
    new_slot.raw = 0;
    
    return ENOENT;
}


int RemoteHopscotch::remove(const std::string &key) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    ThreadLocal &thl = thread_data[GetThreadID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;


    return ENOENT;
}

bool RemoteHopscotch::move_bucket(Bucket *bucket, uint64_t bucket_id) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t old_bucket_size = hash_table->metadata.buckets / 2;
    uint64_t bucket_addr1 = hash_table->bucket + bucket_id * sizeof(Bucket);
    uint64_t bucket_addr2 = hash_table->bucket + (bucket_id+old_bucket_size) * sizeof(Bucket);
    int rc;

    // get lock, move bin
    uint64_t bucket_num1 = 0, bucket_num2 = 0;
    Bucket *bucket_buf = tl.bucket_buf;
    Bucket *new_bucket_buf = tl.new_bucket_buf;
    memset(bucket_buf, 0, sizeof(Bucket));
    memset(new_bucket_buf, 0, sizeof(Bucket));

    for (int i = 0; i < BUCKET_SLOTS; i++) {
        Slot slot = bucket->slot[i];
        if (!slot.len) continue;

        std::string key, value;
        int rc = read_block_kv(key, value, slot);
        if (rc != 0) {
            continue;
        }
        uint64_t hash = hash_fun(key);
        uint64_t new_bucket;
        uint8_t fp;

        split_hash(hash, hash_table->metadata.buckets, LOG_FPRINT, new_bucket, fp);
        if (new_bucket < old_bucket_size) {
            bucket_buf->slot[bucket_num1 + 1] = slot;
            bucket_num1++;
        }
        else {
            new_bucket_buf->slot[bucket_num2 + 1] = slot;
            bucket_num2++;
        }
    }
    // if (bucket_addr1 == 384192 || bucket_addr2 == 384192) {
    //     for (int i = 0; i < BUCKET_SLOTS; i++) {
    //         SDS_INFO("%d %d %ld", new_bucket_buf->slot[i].len, new_bucket_buf->slot[i].fp, new_bucket_buf->slot[i].pointer);
            
    //     }
    // }
    // for (int i = 0; i < BUCKET_SLOTS; i++) {
    //     if (bucket_buf->slot[i].pointer > 16777216000ll) {
    //         SDS_INFO("%ld", bucket_id);
    //     }
    //     if (new_bucket_buf->slot[i].pointer > 16777216000ll) {
    //         SDS_INFO("%ld", bucket_id);
    //     }
    // }
    // if (bucket_addr1 == 384192 || bucket_addr2 == 384192)
    // SDS_INFO("move bucket %ld: %ld %ld %ld %ld", bucket_id, bucket_num1, bucket_num2, bucket_addr1, bucket_addr2);
    node_->write(bucket_buf, GlobalAddress(node_id_, bucket_addr1), sizeof(Bucket));
    node_->write(new_bucket_buf, GlobalAddress(node_id_, bucket_addr2), sizeof(Bucket));
    rc = node_->sync();
    assert(!rc);
    // SDS_INFO("move bucket %ld: %ld %ld %ld %ld", bucket_id, bucket_num1, bucket_num2, bucket_addr1, bucket_addr2);

    return true;
}

int RemoteHopscotch::resize() {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    uint64_t &lock = tl.hash_table->metadata.lock;
    GlobalAddress addr = ht_addr_ + offsetof(HashTable, metadata.lock);
    addr.node = 0;
    timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);
    int rc = node_->compare_and_swap(&lock, addr, 0, 1, Initiator::Option::Sync);
    assert(!rc);
    if (lock != 0) {
        return 1;
    }
    addr.node = ht_addr_.node;
    lock = 1;
    SDS_INFO("begin to resize");

    uint64_t old_bucket = tl.hash_table->bucket;

    uint64_t buckets = tl.hash_table->metadata.buckets;
    uint64_t log_buckets = tl.hash_table->metadata.log_buckets;
    uint64_t resize_cnt = tl.hash_table->metadata.resize_cnt;
    uint64_t new_buckets = buckets << 1;
    uint64_t new_log_buckets = log_buckets + 1;
    uint64_t new_resize_cnt = resize_cnt + 1;
    

    uint64_t bucket_size = buckets * sizeof(Bucket);
    GlobalAddress new_bucket_addr = node_->alloc_memory(node_id_, bucket_size);

    Bucket *old_bucket_buf = (Bucket *)malloc(bucket_size);

    memset(old_bucket_buf, 0, bucket_size);

    // lock old buckets
    Bucket *bucket_buf = tl.bucket_buf;
    for (int i = 0; i < buckets; i++) {
        read_bucket(i);
        lock_bucket(bucket_buf, i);
        memcpy(old_bucket_buf + i, bucket_buf, sizeof(Bucket));
    }

    // write metadata
    tl.hash_table->bucket = new_bucket_addr.offset;
    addr = ht_addr_ + offsetof(HashTable, bucket);
    rc = node_->write(&tl.hash_table->bucket, addr, sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);

    tl.hash_table->metadata.buckets = new_buckets;
    tl.hash_table->metadata.log_buckets = new_log_buckets;
    tl.hash_table->metadata.resize_cnt = new_resize_cnt;
    addr = ht_addr_ + offsetof(HashTable, metadata);
    rc = node_->write(&tl.hash_table->metadata, addr, sizeof(TableMetadata), Initiator::Option::Sync);
    assert(!rc);
    SDS_INFO("resize: %d %d %ld %ld %ld", GetThreadID(), GetTaskID(), tl.hash_table->bucket,
            tl.hash_table->metadata.buckets, tl.hash_table->metadata.log_buckets);

    SDS_INFO("begin to move buckets");

    // begin to move buckets
    for (int i = 0; i < buckets; i++) {
        move_bucket(old_bucket_buf + i, i);
    }

    SDS_INFO("unlock buckets");


    // unlock old buckets
    for (int i = 0; i < buckets; i++) {
        unlock_free_bucket(bucket_buf, i, old_bucket);
    }

    addr = ht_addr_ + offsetof(HashTable, metadata.lock);
    lock = 0;
    rc = node_->write(&lock, addr, sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);

    SDS_INFO("end resizing");

    clock_gettime(CLOCK_REALTIME, &end);
    uint64_t latency = (end.tv_sec - start.tv_sec) * 1E9 + (end.tv_nsec - start.tv_nsec);
    SDS_INFO("resize latency %.3lf us", latency / 1000.0);

    return 0;
}

// int RemoteHopscotch::move_bins() {
//     TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
//     HashTable *hash_table = tl.hash_table;
//     Slot *slot_group = tl.lv1_slot_buf;
//     Slot *slot_group2 = tl.lv2_slot_buf[0];
//     uint64_t resize_cnt = hash_table->metadata.resize_cnt;
//     bool is_lock;
//     int bin_state;
//     int cnt1=0,cnt2=0;

//     for (int i = 0; i < resize_cnt; i++) {
//         for (uint64_t j = 0; j < bins_[i]; j++) {
//             read_lv1_slot_group(i, j);
//             is_lock = slot_group[LOCK_ID].lock;
//             bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
//             if (!is_lock && bin_state == ALL_DIRTY) {
//                 cnt1 += move_lv1_bin(i, j, slot_group);
//             }
//         }
//     }

//     for (int i = 0; i < resize_cnt; i++) {
//         for (uint64_t j = 0; j < bins_[i]; j++) {
//             read_lv2_slot_group(i, j, 0);
//             is_lock = slot_group2[LOCK_ID].lock;
//             bin_state = get_bin_state(slot_group, LV2_SLOTS, resize_cnt);
//             if (!is_lock && bin_state == ALL_DIRTY) {
//                 cnt2 += move_lv2_bin(i, j, slot_group2);
//             }
//         }
//     }
//     SDS_INFO("%d %d",cnt1,cnt2);

//     return 0;
// }

void RemoteHopscotch::global_lock() {
    return;
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    auto &lock = tl.hash_table->metadata.lock;
    GlobalAddress addr = ht_addr_ + ((char *) &lock - (char *) tl.hash_table);
    int retry_cnt = 0;
retry:
    int rc = node_->compare_and_swap(&lock, addr, 0, 1, Initiator::Option::Sync);
    assert(!rc);
    if (lock == 0) {
        return;
    }
    goto retry;
}

void RemoteHopscotch::global_unlock() {
    return;
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    auto &lock = tl.hash_table->metadata.lock;
    GlobalAddress addr = ht_addr_ + ((char *) &lock - (char *) tl.hash_table);
    lock = 0;
    int rc = node_->write(&lock, addr, sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);
}

int RemoteHopscotch::read_block_kv(std::string &key, std::string &value, Slot &slot) {
    size_t block_len = slot.len * BLOCK_UNIT;
    assert(block_len);
    BlockHeader *block = tl_data_[GetThreadID()][GetTaskID()].block_buf;

    int rc = node_->read(block, GlobalAddress(node_id_, slot.pointer),
                             block_len, Initiator::Option::Sync);
    assert(!rc);
    if (check_kv_block_crc32(block, block_len)) {
        return ENOENT;
    }

    const char *block_key = get_kv_block_key(block);
    const char *block_value = get_kv_block_value(block);

    key = std::string(block_key, block->key_len);
    value = std::string(block_value, block->value_len);
    return 0;
}

int RemoteHopscotch::read_block(const std::string &key, std::string &value, Slot &slot, bool retry) {
    size_t block_len = slot.len * BLOCK_UNIT;
    assert(block_len);
    BlockHeader *block = tl_data_[GetThreadID()][GetTaskID()].block_buf;

    if (retry) {
        do {
            int rc = node_->read(block, GlobalAddress(node_id_, slot.pointer),
                                 block_len, Initiator::Option::Sync);
            assert(!rc);
        } while (check_kv_block_crc32(block, block_len));
    } else {
        int rc = node_->read(block, GlobalAddress(node_id_, slot.pointer),
                             block_len, Initiator::Option::Sync);
        assert(!rc);
        if (check_kv_block_crc32(block, block_len)) {
            return ENOENT;
        }
    }

    const char *block_key = get_kv_block_key(block);
    const char *block_value = get_kv_block_value(block);
    if (strncmp(block_key, key.c_str(), block->key_len) != 0) {
        return ENOENT;
    }

    value = std::string(block_value, block->value_len);
    return 0;
}

int RemoteHopscotch::write_block(const std::string &key, const std::string &value, uint8_t fp, Slot &slot) {
    size_t block_len = get_kv_block_len(key, value);
    BlockHeader *block = tl_data_[GetThreadID()][GetTaskID()].block_buf;
    GlobalAddress addr = node_->alloc_memory(node_id_, block_len);
    assert(addr != NULL_GLOBAL_ADDRESS);
    block->key_len = key.size();
    block->value_len = value.size();
    strncpy(get_kv_block_key(block), key.c_str(), block->key_len);
    strncpy(get_kv_block_value(block), value.c_str(), block->value_len);
    update_kv_block_crc32(block, block_len);
    int rc = node_->write(block, addr, block_len);
    assert(!rc);
    slot.raw = 0;
    slot.fp = fp;
    slot.len = block_len / BLOCK_UNIT;
    slot.pointer = addr.offset;
    return 0;
}

int RemoteHopscotch::write_block(const std::string &key, const std::string &value, uint8_t fp,
                                   Slot &slot, sds::GlobalAddress &addr) {
    size_t block_len = get_kv_block_len(key, value);
    BlockHeader *block = tl_data_[GetThreadID()][GetTaskID()].block_buf;
    addr = node_->alloc_memory(node_id_, block_len);
    assert(addr != NULL_GLOBAL_ADDRESS);
    block->key_len = key.size();
    block->value_len = value.size();
    strncpy(get_kv_block_key(block), key.c_str(), block->key_len);
    strncpy(get_kv_block_value(block), value.c_str(), block->value_len);
    update_kv_block_crc32(block, block_len);
    int rc = node_->write(block, addr, block_len);
    assert(!rc);
    slot.raw = 0;
    slot.fp = fp;
    slot.len = block_len / BLOCK_UNIT;
    slot.pointer = addr.offset;
    return 0;
}

int RemoteHopscotch::atomic_update_slot(uint64_t addr, Slot *slot, Slot &new_val) {
    uint64_t old_val = slot->raw;
    int rc = node_->compare_and_swap(slot, GlobalAddress(node_id_, addr), old_val, new_val.raw,
                                     Initiator::Option::Sync);
    assert(!rc);
    return old_val == slot->raw ? 0 : EAGAIN;
}
