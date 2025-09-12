
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
    
    local_lock_table_ = new LocalLockTable();
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

    local_lock_table_ = new LocalLockTable();
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
    
    local_lock_table_ = new LocalLockTable();
}

RemoteHopscotch::~RemoteHopscotch() {
    node_->disconnect(node_id_);
    if (!is_shared_) {
        delete node_;
    }
    delete local_lock_table_;
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
    
// bool RemoteHopscotch::move_bucket(uint64_t block, uint64_t offset, Slot *slot_group) {
//     TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
//     HashTable *hash_table = tl.hash_table;
//     uint8_t resize_bit = (hash_table->metadata.resize_cnt - 1) & 3;
//     uint64_t lv1_bin_addr = hash_table->lv1_bin[block] + offset * sizeof(Lv1Bin);
//     Slot old_val = slot_group[LOCK_ID];
//     Slot new_val = old_val;
//     int rc;

//     // try to lock
//     new_val.lock = 1;
//     rc = node_->compare_and_swap(slot_group, GlobalAddress(node_id_, lv1_bin_addr), old_val.raw, new_val.raw, Initiator::Option::Sync);
//     assert(!rc);
//     if (slot_group[LOCK_ID].raw != old_val.raw) return false;

//     // get lock, move bin
//     uint64_t new_block = hash_table->metadata.resize_cnt;
//     uint64_t new_offset = (1 << block >> 1 << LOG_BINS) + offset;
//     uint64_t new_lv1_bin_addr = hash_table->lv1_bin[new_block] + new_offset * sizeof(Lv1Bin);
//     Slot *new_slot_group = tl.new_slot_buf;
//     uint64_t tot=0,cnt=0;
//     memset(new_slot_group, 0, sizeof(Slot) * LV1_SLOTS);

//     slot_group[LOCK_ID].lock = 0;
//     old_val = slot_group[DIRTY_ID];
//     for (int i = 0; i < LV1_SLOTS; i++) {
//         Slot slot = slot_group[i];
//         if (!slot.len) continue;
//         tot++;
//         if (!(slot.bid & (1 << resize_bit))) continue; // should not move
//         cnt++;
//         // TODO: if resize_bit == 3
        
//         new_slot_group[i] = slot_group[i];
//         slot_group[i].raw = 0;
//         slot_group[i].lock = new_slot_group[i].lock;
//     }
//     for (int i = 0; i < LV1_SLOTS; i += SLOT_GROUP) {
//         slot_group[i + DIRTY_ID].lock = new_slot_group[i + DIRTY_ID].lock = hash_table->metadata.resize_cnt & 1;
//     }
//     // SDS_INFO("block %ld offset %ld old addr %ld new addr %ld tot: %ld, move %ld", block, offset, lv1_bin_addr, new_lv1_bin_addr, tot, cnt);
// #ifdef IS_BLOCKING
//     node_->write(new_slot_group, GlobalAddress(node_id_, new_lv1_bin_addr), sizeof(Slot) * LV1_SLOTS);
//     node_->write(slot_group + 1, GlobalAddress(node_id_, lv1_bin_addr + sizeof(Slot)), sizeof(Slot) * (LV1_SLOTS - 1));
//     rc = node_->sync();
//     assert(!rc);
//     node_->write(slot_group, GlobalAddress(node_id_, lv1_bin_addr), sizeof(Slot), Initiator::Option::Sync);
//     assert(!rc);
// #else
//     node_->write(new_slot_group, GlobalAddress(node_id_, new_lv1_bin_addr), sizeof(Slot) * LV1_SLOTS);
//     node_->write(slot_group, GlobalAddress(node_id_, lv1_bin_addr), sizeof(Slot) * LV1_SLOTS);
//     rc = node_->sync();
//     assert(!rc);
// #endif

//     return true;
// }


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
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    int rc;
    // level1
    uint64_t hash = hash_fun(key);
    uint64_t bucket;
    uint8_t fp;
    Bucket *bucket_buf = tl.bucket_buf;
    // static int cnt1=0;
    // cnt1++;

    int search_res = 0;

    
    split_hash(hash, LOG_FPRINT, bucket, fp);
retry:
    read_bucket(bucket);
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

            split_hash(hash, LOG_FPRINT, bucket, fp);
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

    // uniqueness check
    for (int i = begin; i < BUCKET_SLOTS; i++) {
        Slot slot = slot_group[i];
        if (slot.len && slot.fp == fp) {
            std::string old_value;
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

    // if (load_factor() > RESIZE_THR) {
    //     resize();
    // }

    Slot new_slot;
    int rc;

    // level1
    uint64_t hash = hash_fun(key);
    uint64_t bucket;
    uint8_t fp;
    Bucket *bucket_buf = tl.bucket_buf;

    split_hash(hash, LOG_FPRINT, bucket, fp);
    write_block(key, value, fp, new_slot);

    read_bucket(bucket);
    uint64_t bucket_addr = hash_table->bucket + bucket * sizeof(Bucket);
    int i, res;
    for (i = 0; i < H_BUCKET; i++) {
        lock_bucket(bucket_buf, bucket + 1);
        res = insert_bucket(key, new_slot, (Slot *)bucket_buf, bucket_addr, fp, 1);
        if (res == 0) break;
    }
    if (res == 0) {
        for (int j = 0; j <= i; j++) {
            unlock_bucket(bucket_buf, bucket + j);
        }
        return 0;
    }

    // need to adjust
    read_bucket_neighbour(bucket);
    int j;
    for (j = H_BUCKET + 1; j < H_BUCKET + H_BUCKET; j++) {
        lock_bucket(bucket_buf, bucket + j);
        int pos = find_empty_slot((Slot *)bucket_buf, 1);
        if (pos != -1) {
            res = move_slot((Slot *)(bucket_buf + H_BUCKET - 1), bucket, pos, 1);
            if (res == 0) break;
        }
    }
    if (res == 0) {
        for (int k = 0; k <= j; k++) {
            unlock_bucket(bucket_buf, bucket + k);
        }
        return 0;
    }

    SDS_INFO("insert overflow!, key %s, load factor %.2lf", key.c_str(), load_factor());

    return 0;
}

int RemoteHopscotch::update_bucket(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, Slot *slot_group, uint64_t bin_addr, int slots, int begin, uint8_t fp) {
    ThreadLocal &thl = thread_data[GetThreadID()];
    for (int i = 0; i < slots; i++) {
        int idx = (begin + i) % slots;
        Slot slot = slot_group[idx];
        if (slot.len && slot.fp == fp) {
            if ((idx & (SLOT_GROUP - 1)) == DIRTY_ID) {
                new_slot.lock = slot.lock;
            }
            std::string old_value;
            int rc = read_block(key, old_value, slot);
            if (rc == 0) {
                // atomic_update_slot(bin_addr + sizeof(Slot) * idx, slot_group + idx, new_slot);
                while (atomic_update_slot(bin_addr + sizeof(Slot) * idx, slot_group + idx, new_slot)) {
                    // SDS_INFO("%lx", slot_group[idx].raw);
                    // guard.retry_task();
                }
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
    for (int i = slots - 1; i >= 0; i--) {
        int idx = (begin + i) % slots;
        Slot slot = slot_group[idx];
        if (slot.len && slot.fp == fp) {
            if ((idx & (SLOT_GROUP - 1)) == DIRTY_ID) {
                new_slot.lock = slot.lock;
            }
            std::string old_value;
            int rc = read_block(key, old_value, slot);
            if (rc == 0) {
                // atomic_update_slot(bin_addr + sizeof(Slot) * idx, slot_group + idx, new_slot);
                while (atomic_update_slot(bin_addr + sizeof(Slot) * idx, slot_group + idx, new_slot)) {
                    // SDS_INFO("%lx", slot_group[idx].raw);
                    // guard.retry_task();
                }
                return 0;
            }
            assert(rc == ENOENT);
        }
    }
    return ENOENT;
}


int RemoteHopscotch::remove(const std::string &key) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    ThreadLocal &thl = thread_data[GetThreadID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;


    return ENOENT;
}



// int RemoteHopscotch::resize() {
//     TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
//     uint64_t &lock = tl.hash_table->metadata.lock;
//     GlobalAddress addr = ht_addr_ + offsetof(HashTable, metadata.lock);
//     addr.node = 0;
//     int rc = node_->compare_and_swap(&lock, addr, 0, 1, Initiator::Option::Sync);
//     assert(!rc);
//     if (lock != 0) {
//         return 0;
//     }
//     addr.node = ht_addr_.node;
//     lock = 1;
//     SDS_INFO("begin to resize");

//     uint64_t bins = tl.hash_table->metadata.bins;
//     uint64_t log_bins = tl.hash_table->metadata.log_bins;
//     uint64_t resize_cnt = tl.hash_table->metadata.resize_cnt;
//     uint64_t new_bins = bins << 1;
//     uint64_t new_log_bins = log_bins + 1;
//     uint64_t new_resize_cnt = resize_cnt + 1;
    

//     uint64_t lv1_bin_size = bins * sizeof(Lv1Bin);
//     uint64_t lv2_bin_size = bins * sizeof(Lv2Bin);
//     GlobalAddress new_lv1_addr = node_->alloc_memory(node_id_, lv1_bin_size);
//     tl.hash_table->lv1_bin[new_resize_cnt] = new_lv1_addr.offset;
//     GlobalAddress new_lv2_addr = node_->alloc_memory(node_id_, lv2_bin_size);
//     tl.hash_table->lv2_bin[new_resize_cnt] = new_lv2_addr.offset;
//     addr = ht_addr_ + offsetof(HashTable, lv1_bin);
//     rc = node_->write(tl.hash_table->lv1_bin, addr, sizeof(uint64_t) * (RESIZE_LIM * 2 + 1));
//     assert(!rc);
//     addr = ht_addr_ + offsetof(HashTable, move_bins);
//     *tl.cas_buf = 0;
//     rc = node_->write(tl.cas_buf, addr, sizeof(uint64_t));
//     assert(!rc);
//     node_->sync();

//     tl.hash_table->metadata.bins = new_bins;
//     tl.hash_table->metadata.log_bins = new_log_bins;
//     tl.hash_table->metadata.resize_cnt = new_resize_cnt;
//     addr = ht_addr_ + offsetof(HashTable, metadata);
//     rc = node_->write(&tl.hash_table->metadata, addr, sizeof(TableMetadata), Initiator::Option::Sync);
//     assert(!rc);
//     SDS_INFO("resize: %d %d %ld %ld %ld", GetThreadID(), GetTaskID(),
//             tl.hash_table->metadata.bins, tl.hash_table->metadata.log_bins, tl.hash_table->metadata.resize_cnt);

//     // wait until all other task sync metadata
//     // BackoffGuard guard(tl_backoff);
//     // while (true) {
//     //     addr = ht_addr_ + offsetof(HashTable, sync_tasks);
//     //     addr.node = 0;
//     //     rc = node_->read(tl.cas_buf, addr, sizeof(uint64_t), Initiator::Option::Sync);
//     //     // SDS_INFO("%ld %ld", *tl.cas_buf, tl.hash_table->tasks);
//     //     if (*tl.cas_buf >= tl.hash_table->tasks - 1) break;
//     //     // guard.retry_task();
//     // }
    
//     // after this, other operation can sync
//     // begin to move bins
//     /*
//     SDS_INFO("begin to move");
//     uint64_t cnt=0;
//     // level1
//     for (int i = 0; i <= resize_cnt; i++) {
//         for (uint64_t j = 0; j < bins_[i]; j++) {
//             for (int k = 0; k < LV1_SLOTS; k++) {
//                 Slot old_slot, new_slot;
//                 // acquire slot lock
//                 while (true) {
//                     addr.offset = tl.hash_table->lv1_bin[i] + j * sizeof(Lv1Bin) + k * sizeof(Slot);
//                     rc = node_->read(old_data_buf_, addr, sizeof(Slot), Initiator::Option::Sync);
//                     assert(!rc);
//                     old_slot = *((Slot *)old_data_buf_);
//                     if (!old_slot.raw) break;
//                     new_slot = old_slot;
//                     new_slot.lock = 1;
//                     rc = node_->compare_and_swap(old_data_buf_, addr, old_slot.raw, new_slot.raw, Initiator::Option::Sync);
//                     assert(!rc);
//                     if (old_slot.raw == *((uint64_t *)old_data_buf_))  break;
//                 }
//                 if (!old_slot.raw) continue;
                
//                 BlockHeader *block = tl.block_buf;
//                 uint64_t block_len = new_slot.len * BLOCK_UNIT;
//                 do {
//                     rc = node_->read(block, GlobalAddress(node_id_, new_slot.pointer), block_len, Initiator::Option::Sync);
//                     assert(!rc);
//                 } while (check_kv_block_crc32(block, block_len));
//                 std::string key(get_kv_block_key(block)), value(get_kv_block_value(block));
//                 uint64_t hash1 = lv1_hash(key);
//                 uint64_t index, bin, slot_id, group, block_id, offset;
//                 uint8_t fp;
//                 split_hash(hash1, tl.hash_table->metadata.log_bins + LOG_SLOTS, LOG_FPRINT, index, fp);
//                 split_index(index, tl.hash_table->metadata.log_bins, LOG_SLOTS - LOG_GROUP, LOG_GROUP, bin, slot_id, group);
//                 split_bin(bin, LOG_BINS, block_id, offset);
//                 if (block_id < new_resize_cnt) continue;
//                 // SDS_INFO("%ld %ld %ld", cnt, block_id, offset);

//                 cnt++;
//                 Slot *slot_group = tl.lv1_slot_buf;
//                 uint64_t lv1_bin_addr = tl.hash_table->lv1_bin[block_id] + offset * sizeof(Lv1Bin);
//                 read_lv1_slot_group(block_id, offset, slot_id);
//                 for (int l = 0; l < LV1_SLOTS; l++) {
//                     int idx = (slot_id * SLOT_GROUP + group + l) % LV1_SLOTS;
//                     Slot slot = slot_group[idx];
//                     if (!slot.raw) {
//                         rc = atomic_update_slot(lv1_bin_addr + sizeof(Slot) * idx, slot_group + idx, old_slot);
//                         if (rc == 0) {
//                             break;
//                         }
//                         guard.retry_task();
//                     }
//                 }
//                 // insert(key, value); // TODO: use known info
//                 *(uint64_t *)new_data_buf_ = 0;
//                 addr.offset = tl.hash_table->lv1_bin[i] + j * sizeof(Lv1Bin) + k * sizeof(Slot);
//                 node_->write(new_data_buf_, addr, sizeof(Slot), Initiator::Option::Sync);
//                 // remove(key);        // TODO: use known info
//             }
//         }
//     }
//     SDS_INFO("resize: move %ld slots", cnt);

//     // level2
//     for (int i = 0; i <= resize_cnt; i++) {
//         for (uint64_t j = 0; j < bins_[i]; j++) {
//             for (int k = 0; k < LV2_SLOTS; k++) {
//                 Slot old_slot, new_slot;
//                 // acquire slot lock
//                 while (true) {
//                     addr.offset = tl.hash_table->lv2_bin[i] + j * sizeof(Lv2Bin) + k * sizeof(Slot);
//                     rc = node_->read(old_data_buf_, addr, sizeof(Slot), Initiator::Option::Sync);
//                     assert(!rc);
//                     old_slot = *((Slot *)old_data_buf_);
//                     if (!old_slot.raw) break;
//                     new_slot = old_slot;
//                     new_slot.lock = 1;
//                     rc = node_->compare_and_swap(old_data_buf_, addr, old_slot.raw, new_slot.raw, Initiator::Option::Sync);
//                     assert(!rc);
//                     if (old_slot.raw == *((uint64_t *)old_data_buf_))  break;
//                 }
//                 if (!old_slot.raw) continue;
                
//                 BlockHeader *block = tl.block_buf;
//                 uint64_t block_len = new_slot.len * BLOCK_UNIT;
//                 do {
//                     rc = node_->read(block, GlobalAddress(node_id_, new_slot.pointer), block_len, Initiator::Option::Sync);
//                     assert(!rc);
//                 } while (check_kv_block_crc32(block, block_len));
//                 std::string key(get_kv_block_key(block)), value(get_kv_block_value(block));
//                 uint64_t hash2;
//                 uint64_t index, bin, slot_id, block_id, offset, group;
//                 uint8_t fp;
//                 for (int l = 0; l < CHOICE; l++) {
//                     hash2 = lv2_hash(key, l);
//                     split_hash(hash2, tl.hash_table->metadata.log_bins + LOG_GROUP, LOG_FPRINT, index, fp);
//                     split_index(index, tl.hash_table->metadata.log_bins, 0, LOG_GROUP, bin, slot_id, group);
//                     split_bin(bin, LOG_BINS, block_id, offset);
//                     if (offset == j) {
//                         break;
//                     }
//                 }
//                 // uint64_t hash1 = lv1_hash(key);
//                 // uint64_t index, bin, slot_id, group, block_id, offset;
//                 // uint8_t fp;
//                 // split_hash(hash1, tl.hash_table->metadata.log_bins + LOG_SLOTS, LOG_FPRINT, index, fp);
//                 // split_index(index, tl.hash_table->metadata.log_bins, LOG_SLOTS - LOG_GROUP, LOG_GROUP, bin, slot_id, group);
//                 // split_bin(bin, LOG_BINS, block_id, offset);
//                 if (block_id < new_resize_cnt) continue;

//                 cnt++;
//                 Slot *slot_group = tl.lv2_slot_buf[0];
//                 uint64_t lv2_bin_addr = tl.hash_table->lv2_bin[block_id] + offset * sizeof(Lv2Bin);
//                 read_lv2_slot_group(block_id, offset, slot_id, 0);
//                 node_->sync();
//                 for (int l = 0; l < LV2_SLOTS; l++) {
//                     int idx = (slot_id * SLOT_GROUP + group + l) % LV2_SLOTS;
//                     Slot slot = slot_group[idx];
//                     if (!slot.raw) {
//                         rc = atomic_update_slot(lv2_bin_addr + sizeof(Slot) * idx, slot_group + idx, old_slot);
//                         if (rc == 0) {
//                             break;
//                         }
//                         guard.retry_task();
//                     }
//                 }
//                 // insert(key, value); // TODO: use known info
//                 *(uint64_t *)new_data_buf_ = 0;
//                 addr.offset = tl.hash_table->lv2_bin[i] + j * sizeof(Lv2Bin) + k * sizeof(Slot);
//                 node_->write(new_data_buf_, addr, sizeof(Slot), Initiator::Option::Sync);
//                 // remove(key);        // TODO: use known info
//             }
//         }
//     }
//     SDS_INFO("resize: move %ld slots", cnt);
//     */
//     /*
//     for (int i = 0; i < resize_cnt; i++) {
//         for (uint64_t j = 0; j < bins_[i]; j += DATA_BLOCK) {
//             addr.offset = tl.hash_table->lv1_bin[i] + j;
//             rc = node_->read(old_data_buf_, addr, DATA_BLOCK, Initiator::Option::Sync);
//             assert(!rc);
//             for (int k = 0; k < DATA_BLOCK; k += LV1_SLOTS) {
//                 Slot *slot = (Slot *)(old_data_buf_ + k);
//                 for (int l = 0; l < LV1_SLOTS; l++) {
//                     if (!slot[l].raw) continue;
//                     BlockHeader *block = tl.block_buf;
//                     uint64_t block_len = slot[l].len * BLOCK_UNIT;
//                     do {
//                         rc = node_->read(block, GlobalAddress(node_id_, slot[l].pointer), block_len, Initiator::Option::Sync);
//                         assert(!rc);
//                     } while (check_kv_block_crc32(block, block_len));
//                     std::string key(get_kv_block_key(block)), value(get_kv_block_value(block));
//                     uint64_t hash1 = lv1_hash(key);
//                     uint64_t index, bin, slot, group, block_id, offset;
//                     uint8_t fp;
//                     split_hash(hash1, tl.hash_table->metadata.log_bins + LOG_SLOTS, LOG_FPRINT, index, fp);
//                     split_index(index, tl.hash_table->metadata.log_bins, LOG_SLOTS - LOG_GROUP, LOG_GROUP, bin, slot, group);
//                     split_bin(bin, LOG_BINS, block_id, offset);
//                     if (block_id < new_resize_cnt - 1) continue;

//                     insert(key, value); // TODO: use known info
//                     remove(key);        // TODO: use known info
//                 }
//             }
//         }
//     }
//     */

//     // reset resize value and release lock
//     // addr = ht_addr_ + offsetof(HashTable, sync_tasks);
//     // addr.node = 0;
//     // *tl.cas_buf = 0;
//     // rc = node_->write(tl.cas_buf, addr, sizeof(uint64_t), Initiator::Option::Sync);
//     // assert(!rc);

//     addr = ht_addr_ + offsetof(HashTable, metadata.lock);
//     lock = 0;
//     rc = node_->write(&lock, addr, sizeof(uint64_t), Initiator::Option::Sync);
//     assert(!rc);
//     return 0;
// }

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

    do {
        int rc = node_->read(block, GlobalAddress(node_id_, slot.pointer),
                                block_len, Initiator::Option::Sync);
        assert(!rc);
    } while (check_kv_block_crc32(block, block_len));

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
