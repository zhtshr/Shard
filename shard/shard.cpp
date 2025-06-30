
#include "shard.h"
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

RemoteShard::RemoteShard(JsonConfig config, int max_threads) : config_(config), is_shared_(false) {
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
    bins_[0] = bins_[1] = INIT_BINS;
    for (int i = 2; i < RESIZE_LIM; i++) {
        bins_[i] = bins_[i - 1] << 1;
    }
    old_data_buf_ = (char *) node_->alloc_cache(DATA_BLOCK);
    new_data_buf_ = (char *) node_->alloc_cache(DATA_BLOCK);

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
    // uint64_t threads = config.get("nr_threads").get_uint64();
    // uint64_t tasks = config.get("tasks_per_thread").get_uint64();
    // uint64_t *tmp = (uint64_t *) node_->alloc_cache(sizeof(uint64_t));
    // GlobalAddress addr = ht_addr_ + (offsetof(HashTable, tasks));
    // int rc = node_->fetch_and_add(tmp, addr, (threads ) * tasks , Initiator::Option::Sync);
    // assert(!rc);
    
    local_lock_table_ = new LocalLockTable();
}

RemoteShard::RemoteShard(JsonConfig config, int max_threads, Initiator *node, int node_id) :
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
    old_data_buf_ = (char *) node_->alloc_cache(DATA_BLOCK);
    new_data_buf_ = (char *) node_->alloc_cache(DATA_BLOCK);
    
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
    // uint64_t threads = config.get("nr_threads").get_uint64();
    // uint64_t tasks = config.get("tasks_per_thread").get_uint64();
    // uint64_t *tmp = (uint64_t *) node_->alloc_cache(sizeof(uint64_t));
    // GlobalAddress addr = ht_addr_ + (offsetof(HashTable, tasks));
    // int rc = node_->fetch_and_add(tmp, addr, (threads ) * tasks , Initiator::Option::Sync);
    // assert(!rc);

    local_lock_table_ = new LocalLockTable();
}

RemoteShard::RemoteShard(JsonConfig config, Initiator *node, int node_id) :
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
    bins_[0] = bins_[1] = INIT_BINS;
    for (int i = 2; i < RESIZE_LIM; i++) {
        bins_[i] = bins_[i - 1] << 1;
    }
    old_data_buf_ = (char *) node_->alloc_cache(DATA_BLOCK);
    new_data_buf_ = (char *) node_->alloc_cache(DATA_BLOCK);
    
    uint64_t clients = config.get("client_num").get_uint64();
    uint64_t threads = config.get("nr_threads").get_uint64();
    uint64_t tasks = config.get("tasks_per_thread").get_uint64();
    if (init_task(GetThreadID(), GetTaskID(), clients * threads * tasks)) {
        exit(EXIT_FAILURE);
    }
    
    local_lock_table_ = new LocalLockTable();
}

RemoteShard::~RemoteShard() {
    node_->disconnect(node_id_);
    if (!is_shared_) {
        delete node_;
    }
    delete local_lock_table_;
}

void RemoteShard::split_hash_result(uint64_t hash, uint64_t log_bins, uint64_t log_slots, uint64_t &bin,
                                          uint64_t &block, uint64_t &offset, uint64_t &slot, uint8_t &fp) {
    uint64_t index;
    split_hash(hash, log_bins + log_slots, LOG_FPRINT, index, fp);
    split_index(index, log_bins, log_slots, bin, slot);
    split_bin(bin, LOG_BINS, block, offset);
}

int RemoteShard::init_task(int thread, int task, uint64_t total_tasks) {
    TaskLocal &tl = tl_data_[thread][task];
    tl.cas_buf = (uint64_t *) node_->alloc_cache(sizeof(uint64_t));
    tl.hash_table = (HashTable *) node_->alloc_cache(sizeof(HashTable));
    tl.lv1_slot_buf = (Slot *) node_->alloc_cache(sizeof(Slot) * LV1_SLOTS);
    for (int i = 0; i < CHOICE; i++)
        tl.lv2_slot_buf[i] = (Slot *) node_->alloc_cache(sizeof(Slot) * SLOT_GROUP);
    tl.lv3_slot_buf = (Slot *) node_->alloc_cache(sizeof(Slot) * SLOT_GROUP);
    tl.new_slot_buf = (Slot *) node_->alloc_cache(sizeof(Slot) * LV1_SLOTS);
    tl.block_buf = (BlockHeader *) node_->alloc_cache(BLOCK_LEN);
    // SDS_INFO("%ld", sizeof(uint64_t)+sizeof(HashTable)+sizeof(Slot) * LV1_SLOTS+sizeof(Slot) * SLOT_GROUP+sizeof(Slot) * SLOT_GROUP+sizeof(Slot) * LV1_SLOTS+BLOCK_LEN);
    assert(tl.cas_buf && tl.hash_table && tl.lv1_slot_buf && tl.lv3_slot_buf && tl.block_buf);
    for (int i = 0; i < CHOICE; i++)
        assert(tl.lv2_slot_buf[i]);
    if (node_->read(tl.hash_table, ht_addr_, sizeof(HashTable), Initiator::Option::Sync)) {
        return -1;
    }
    tl.hash_table->tasks = total_tasks;

    return 0;
}

int64_t RemoteShard::tot_balls() {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    return tl.hash_table->lv1_balls + tl.hash_table->lv2_balls + tl.hash_table->lv3_balls;
}

int64_t RemoteShard::tot_capacity() {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    TableMetadata &metadata = tl.hash_table->metadata;
    return metadata.bins * (LV1_SLOTS + LV2_SLOTS);
}

double RemoteShard::load_factor() {
    return (double)tot_balls() / tot_capacity();
}

int RemoteShard::check_sync() {
    int tid = GetTaskID();
    TaskLocal &tl = tl_data_[GetThreadID()][tid];
    ThreadLocal &thl = thread_data[GetThreadID()];
    double load_fac = load_factor();
    uint64_t bins = tl.hash_table->metadata.bins;
    
    thl.cnt.fetch_add(1, std::memory_order_relaxed);
    uint64_t sync_cnt = SYNC_T * (RESIZE_THR > load_fac ? RESIZE_THR - load_fac : 0);
    if (thl.cnt.load(std::memory_order_relaxed) >= sync_cnt) {
        if (thl.cnt.fetch_sub(sync_cnt, std::memory_order_relaxed) >= 0) {
            // get into critical zone
            int64_t lv1_cnt = thl.lv1_cnt.exchange(0, std::memory_order_relaxed);
            int64_t lv2_cnt = thl.lv2_cnt.exchange(0, std::memory_order_relaxed);
            int64_t lv3_cnt = thl.lv3_cnt.exchange(0, std::memory_order_relaxed);
            
            int64_t &lv1_balls = tl.hash_table->lv1_balls;
            int64_t &lv2_balls = tl.hash_table->lv2_balls;
            int64_t &lv3_balls = tl.hash_table->lv3_balls;

            GlobalAddress addr1 = ht_addr_ + offsetof(HashTable, lv1_balls);
            GlobalAddress addr2 = ht_addr_ + offsetof(HashTable, lv2_balls);
            GlobalAddress addr3 = ht_addr_ + offsetof(HashTable, lv3_balls);

            node_->fetch_and_add(&lv1_balls, addr1, lv1_cnt);
            node_->fetch_and_add(&lv2_balls, addr2, lv2_cnt);
            node_->fetch_and_add(&lv3_balls, addr3, lv3_cnt);
            node_->sync();

            // for (int i = 0; i < kMaxTasksPerThread; i++) {
            //     read_hash_table(i);
            // }
            // SDS_INFO("%ld %ld %ld %lf", lv1_balls, lv2_balls, lv3_balls, load_fac);
        }
        else {
            thl.cnt.fetch_add(sync_cnt);
        }
    }
    uint64_t sync_task = SYNC_ALPHA * bins * (RESIZE_THR > load_fac ? RESIZE_THR - load_fac : 0);
    if (++tl.op_cnt >= sync_task) {
        read_hash_table();
        tl.op_cnt -= sync_task;
    }
    return 0;
}

int RemoteShard::read_hash_table() {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    while (__sync_lock_test_and_set(&tl.lock, 1));
    uint64_t old_log_bins = tl.hash_table->metadata.log_bins;
    rc = node_->read(tl.hash_table, ht_addr_, sizeof(HashTable), Initiator::Option::Sync);
    assert(!rc);
    if (old_log_bins != tl.hash_table->metadata.log_bins) {
        // if (!tl.resize_flag) {
            // SDS_INFO("%d %d", GetThreadID(), GetTaskID());
        //     tl.resize_flag = 1;
            GlobalAddress addr = ht_addr_ + offsetof(HashTable, sync_tasks);
            addr.node = 0;
            rc = node_->fetch_and_add(tl.cas_buf, addr, 1, Initiator::Option::Sync);
            assert(!rc);
            // BackoffGuard guard(tl_backoff);
            while (*tl.cas_buf && *tl.cas_buf < tl.hash_table->tasks - 1 ) {
                // SDS_INFO("%d %d %ld %ld", GetThreadID(), GetTaskID(), *tl.cas_buf, tl.hash_table->tasks);
                rc = node_->read(tl.cas_buf, addr, sizeof(uint64_t), Initiator::Option::Sync);
                assert(!rc);
                // guard.retry_task();
            }
        // }
    }
    // else {
    //     tl.resize_flag = 0;
    // }
    __sync_lock_release(&tl.lock);
    return tl.resize_flag;
}

int RemoteShard::read_hash_table(int task_id) {
    TaskLocal &tl = tl_data_[GetThreadID()][task_id];
    if (tl.hash_table->metadata.lock) return 0;
    int rc;
    while (__sync_lock_test_and_set(&tl.lock, 1));
    uint64_t old_log_bins = tl.hash_table->metadata.log_bins;
    rc = node_->read(tl.hash_table, ht_addr_, sizeof(HashTable), Initiator::Option::Sync);
    assert(!rc);
    if (old_log_bins != tl.hash_table->metadata.log_bins) {
        if (!tl.resize_flag) {
            tl.resize_flag = 1;
            GlobalAddress addr = ht_addr_ + offsetof(HashTable, sync_tasks);
            addr.node = 0;
            rc = node_->fetch_and_add(tl.cas_buf, addr, 1, Initiator::Option::Sync);
            assert(!rc);
            // BackoffGuard guard(tl_backoff);
            while (*tl.cas_buf && *tl.cas_buf != tl.hash_table->tasks) {
                // SDS_INFO("%ld %d %ld %ld", GetThreadID(), task_id, *tl.cas_buf, tl.hash_table->tasks);
                rc = node_->read(tl.cas_buf, addr, sizeof(uint64_t), Initiator::Option::Sync);
                assert(!rc);
                // guard.retry_task();
            }
        }
    }
    else {
        tl.resize_flag = 0;
    }
    __sync_lock_release(&tl.lock);
    return tl.resize_flag;
}

int RemoteShard::read_lv1_slot_group(uint64_t block, uint64_t offset) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    // in local: tl.hash_table->lv1_bin[block][offset].slot[slot]
    uint64_t lv1_bin_addr = tl.hash_table->lv1_bin[block] + offset * sizeof(Lv1Bin);
    rc = node_->read(tl.lv1_slot_buf, GlobalAddress(node_id_, lv1_bin_addr), sizeof(Slot) * LV1_SLOTS, Initiator::Option::Sync);
    assert(!rc);
    return 0;
}

int RemoteShard::read_lv2_slot_group(uint64_t block, uint64_t offset, int i) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    // in local: tl.hash_table->lv1_bin[block][offset].slot[slot]
    uint64_t lv2_bin_addr = tl.hash_table->lv2_bin[block] + offset * sizeof(Lv2Bin);
    rc = node_->read(tl.lv2_slot_buf[i], GlobalAddress(node_id_, lv2_bin_addr), sizeof(Slot) * LV2_SLOTS, Initiator::Option::Sync);
    assert(!rc);
    return 0;
}

int RemoteShard::read_slot_group(uint64_t block, uint64_t offset, int id) {
    if (id == 0) {
        return read_lv1_slot_group(block, offset);
    }
    else {
        return read_lv2_slot_group(block, offset, id - 1);
    }
}
    
bool RemoteShard::move_lv1_bin(uint64_t block, uint64_t offset, Slot *slot_group) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint8_t resize_bit = (hash_table->metadata.resize_cnt - 1) & 3;
    uint64_t lv1_bin_addr = hash_table->lv1_bin[block] + offset * sizeof(Lv1Bin);
    Slot old_val = slot_group[LOCK_ID];
    Slot new_val = old_val;
    int rc;

    // try to lock
    new_val.lock = 1;
    rc = node_->compare_and_swap(slot_group, GlobalAddress(node_id_, lv1_bin_addr), old_val.raw, new_val.raw, Initiator::Option::Sync);
    assert(!rc);
    if (slot_group[LOCK_ID].raw != old_val.raw) return false;

    // get lock, move bin
    uint64_t new_block = hash_table->metadata.resize_cnt;
    uint64_t new_offset = (1 << block >> 1 << LOG_BINS) + offset;
    uint64_t new_lv1_bin_addr = hash_table->lv1_bin[new_block] + new_offset * sizeof(Lv1Bin);
    Slot *new_slot_group = tl.new_slot_buf;
    uint64_t tot=0,cnt=0;
    memset(new_slot_group, 0, sizeof(Slot) * LV1_SLOTS);

    slot_group[LOCK_ID].lock = 0;
    old_val = slot_group[DIRTY_ID];
    for (int i = 0; i < LV1_SLOTS; i++) {
        Slot slot = slot_group[i];
        if (!slot.len) continue;
        tot++;
        if (!(slot.bid & (1 << resize_bit))) continue; // should not move
        cnt++;
        // TODO: if resize_bit == 3
        
        new_slot_group[i] = slot_group[i];
        slot_group[i].raw = 0;
        slot_group[i].lock = new_slot_group[i].lock;
    }
    for (int i = 0; i < LV1_SLOTS; i += SLOT_GROUP) {
        slot_group[i + DIRTY_ID].lock = new_slot_group[i + DIRTY_ID].lock = hash_table->metadata.resize_cnt & 1;
    }
    // SDS_INFO("block %ld offset %ld old addr %ld new addr %ld tot: %ld, move %ld", block, offset, lv1_bin_addr, new_lv1_bin_addr, tot, cnt);
#ifdef IS_BLOCKING
    node_->write(new_slot_group, GlobalAddress(node_id_, new_lv1_bin_addr), sizeof(Slot) * LV1_SLOTS);
    node_->write(slot_group + 1, GlobalAddress(node_id_, lv1_bin_addr + sizeof(Slot)), sizeof(Slot) * (LV1_SLOTS - 1));
    rc = node_->sync();
    assert(!rc);
    node_->write(slot_group, GlobalAddress(node_id_, lv1_bin_addr), sizeof(Slot), Initiator::Option::Sync);
    assert(!rc);
#else
    node_->write(new_slot_group, GlobalAddress(node_id_, new_lv1_bin_addr), sizeof(Slot) * LV1_SLOTS);
    node_->write(slot_group, GlobalAddress(node_id_, lv1_bin_addr), sizeof(Slot) * LV1_SLOTS);
    rc = node_->sync();
    assert(!rc);
#endif

    return true;
}

bool RemoteShard::move_lv2_bin(uint64_t block, uint64_t offset, Slot *slot_group) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint8_t resize_bit = (hash_table->metadata.resize_cnt - 1) & 3;
    uint64_t lv2_bin_addr = hash_table->lv2_bin[block] + offset * sizeof(Lv2Bin);
    Slot old_val = slot_group[LOCK_ID];
    Slot new_val = old_val;
    int rc;

    new_val.lock = 1;
    rc = node_->compare_and_swap(slot_group, GlobalAddress(node_id_, lv2_bin_addr), old_val.raw, new_val.raw, Initiator::Option::Sync);
    assert(!rc);
    if (slot_group[LOCK_ID].raw != old_val.raw) return false;

    // move bin
    uint64_t new_block = hash_table->metadata.resize_cnt;
    uint64_t new_offset = (1 << block >> 1 << LOG_BINS) + offset;
    uint64_t new_lv2_bin_addr = hash_table->lv2_bin[new_block] + new_offset * sizeof(Lv2Bin);
    Slot *new_slot_group = tl.new_slot_buf;
    memset(new_slot_group, 0, sizeof(Slot) * LV2_SLOTS);

    slot_group[LOCK_ID].lock = 0;
    old_val = slot_group[DIRTY_ID];
    for (int i = 0; i < LV2_SLOTS; i++) {
        Slot slot = slot_group[i];
        if (!slot.len) continue;
        if (!(slot.bid & (1 << resize_bit))) continue; // should not move
        // TODO: if resize_bit == 3
        
        new_slot_group[i] = slot_group[i];
        slot_group[i].raw = 0;
        slot_group[i].lock = new_slot_group[i].lock;
    }
    slot_group[DIRTY_ID].lock = new_slot_group[DIRTY_ID].lock = hash_table->metadata.resize_cnt & 1;
#ifdef IS_BLOCKING
    node_->write(new_slot_group, GlobalAddress(node_id_, new_lv2_bin_addr), sizeof(Slot) * LV2_SLOTS);
    node_->write(slot_group + 1, GlobalAddress(node_id_, lv2_bin_addr + sizeof(Slot)), sizeof(Slot) * (LV2_SLOTS - 1));
    rc = node_->sync();
    assert(!rc);
    node_->write(slot_group, GlobalAddress(node_id_, lv2_bin_addr), sizeof(Slot), Initiator::Option::Sync);
    assert(!rc);
#else
    node_->write(new_slot_group, GlobalAddress(node_id_, new_lv2_bin_addr), sizeof(Slot) * LV2_SLOTS);
    node_->write(slot_group, GlobalAddress(node_id_, lv2_bin_addr), sizeof(Slot) * LV2_SLOTS);
    rc = node_->sync();
    assert(!rc);
#endif
    
    return true;
}

bool RemoteShard::move_bin(uint64_t block, uint64_t offset, Slot *slot_group, int id) {
    if (id == 0) {
        return move_lv1_bin(block, offset, slot_group);
    }
    else {
        return move_lv2_bin(block, offset, slot_group);
    }
}

// none dirty:0 part dirty:2 all dirty:3
int RemoteShard::get_bin_state(const Slot *slot_group, int slots, int resize_cnt) {
    bool zero = false, one = false;
    // zero=true:have clean
    // one =true:have dirty
    for (int i = 0; i < slots; i += SLOT_GROUP) {
        if (slot_group[i + DIRTY_ID].lock == (resize_cnt & 1)) {
            zero = true;
        }
        else {
            one = true;
        }
    }
    assert(zero | one);
    return (!zero) | (one << 1);
}

int RemoteShard::get_version(const Slot *slot_group) {
    int version = 0;
    for (int i = VER_BEGIN; i <= VER_END; i++) {
        version = (version << 1) | slot_group[i].lock;
    }
    return version;
}

int RemoteShard::search_bin(const std::string &key, std::string &value, const Slot *slot_group, int slots, int begin, uint8_t fp) {
    for (int i = 0; i < slots; i++) {
        int idx = (begin + i) % slots;
        Slot slot = slot_group[idx];
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

int RemoteShard::search_old_blocking(const std::string &key, std::string &value, int id, int slots, uint64_t old_block, uint64_t old_offset,
                                  uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(old_block, old_offset, id);
    bool is_lock = slot_group[LOCK_ID].lock;
    bool is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
    if (!is_lock) {
        if (!is_dirty) {
            read_slot_group(block, offset, id);
        }
        else {
            bool is_moved;
            if (id == 0) is_moved = move_lv1_bin(old_block, old_offset, slot_group);
            else is_moved = move_lv2_bin(old_block, old_offset, slot_group);

            if (!is_moved) {
                while (is_lock || is_dirty) {
                    // SDS_INFO("%ld %d %ld %ld %d %d", slot_group[LOCK_ID].lock, slots, block, offset, slot, fp);
                    read_slot_group(old_block, old_offset, id);
                    is_lock = slot_group[LOCK_ID].lock;
                    is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
                }
                return search_bin(key, value, slot_group, slots, slot, fp);     // TODO: have dirty
            }
            else {
                return search_bin(key, value, tl.new_slot_buf, slots, slot, fp);
            }
        }
    }
    else {
        while (is_lock) {
            // SDS_INFO("%ld %d %ld %ld %d %d", slot_group[LOCK_ID].lock, slots, block, offset, slot, fp);
            read_slot_group(old_block, old_offset, id);
            is_lock = slot_group[LOCK_ID].lock;
        }
        read_slot_group(block, offset, id);
    }
    return search_bin(key, value, slot_group, slots, slot, fp);
}

int RemoteShard::search_new_blocking(const std::string &key, std::string &value, int id, int slots, uint64_t old_block, uint64_t old_offset,
                                  uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    slot_group = id == 0 ? tl.lv1_slot_buf : tl.lv2_slot_buf[id - 1];

    read_slot_group(block, offset, id);
    bool is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
    if (!is_dirty) {
        return search_bin(key, value, slot_group, slots, slot, fp);
    }
    else {
        return search_old_blocking(key, value, id, slots, old_block, old_offset, block, offset, slot, fp);
    }
}

int RemoteShard::search_bins_blocking(const std::string &key, std::string &value, int id, int slots,
                                            uint64_t bin, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    slot_group = id == 0 ? tl.lv1_slot_buf : tl.lv2_slot_buf[id - 1];


    // read_slot_group(block, offset, id);
    // if (get_version(slot_group) == resize_cnt) {
    //     while (slot_group[LOCK_ID].lock) {
    //         read_slot_group(block, offset, id);
    //     }
    //     return search_bin(key, value, slot_group, slots, slot, fp);
    // }
    // else {
    //     // TODO: replace by binary seach

    //     // find the minimun dirty chunk
    //     int min_block = block, min_offset = offset;
    //     for (int i = block - 1; i >= 0; i--) {
    //         uint64_t old_block, old_offset;
    //         split_bin(bin & ((1 << (i + LOG_BINS)) - 1), LOG_BINS, old_block, old_offset);
    //         read_slot_group(old_block, old_offset, id);
    //         if (get_version(slot_group) == resize_cnt) break;
    //         min_block = old_block;
    //         min_offset = old_offset;
    //     }

    //     if (!slot_group[LOCK_ID].lock) {
    //         move_bin(min_block, min_offset, slot_group, id);
    //     }
    //     while (slot_group[LOCK_ID].lock) {
    //         read_slot_group(min_block, min_offset, id);
    //     }
    //     read_slot_group(block, offset, id);
    //     return search_bin(key, value, slot_group, slots, slot, fp);
    // }
    
    if (block < resize_cnt) {
        read_slot_group(block, offset, id);
        if (!slot_group[LOCK_ID].lock && slot_group[DIRTY_ID].lock != (resize_cnt & 1)) {
            move_bin(block, offset, slot_group, id);
        }
        while (slot_group[LOCK_ID].lock) {
            read_slot_group(block, offset, id);
        }
        return search_bin(key, value, slot_group, slots, slot, fp);
    }
    else {
        uint64_t old_block, old_offset;
        split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
        return search_new_blocking(key, value, id, slots, old_block, old_offset, block, offset, slot, fp);
    }

    return ENOENT;
}

int RemoteShard::search_old(const std::string &key, std::string &value, int id, int slots, uint64_t old_block, uint64_t old_offset,
                                  uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(old_block, old_offset, id);
    bool is_lock = slot_group[LOCK_ID].lock;
    int bin_state = get_bin_state(slot_group, slots, resize_cnt);
    if (!is_lock) {
        if (bin_state == NONE_DIRTY) {
            read_slot_group(block, offset, id);
        }
        else if (bin_state == PART_DIRTY) {
            while(bin_state != NONE_DIRTY) {
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
        else {
            bool is_moved;
            if (id == 0) is_moved = move_lv1_bin(old_block, old_offset, slot_group);
            else is_moved = move_lv2_bin(old_block, old_offset, slot_group);

            if (!is_moved) {
                return search_bin(key, value, slot_group, slots, slot, fp);     // TODO: have dirty
            }
            else {
                return search_bin(key, value, tl.new_slot_buf, slots, slot, fp);
            }
        }
    }
    else {
        if (bin_state == NONE_DIRTY) {
            read_slot_group(block, offset, id);
        }
        else if (bin_state == PART_DIRTY) {
            while(bin_state != NONE_DIRTY) {
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
        else {
            return search_bin(key, value, slot_group, slots, slot, fp);     // TODO: have dirty
        }
    }
    return search_bin(key, value, slot_group, slots, slot, fp);
}

int RemoteShard::search_new(const std::string &key, std::string &value, int id, int slots, uint64_t old_block, uint64_t old_offset,
                                  uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(block, offset, id);
    int bin_state = get_bin_state(slot_group, slots, resize_cnt);
    if (bin_state == NONE_DIRTY) {
        return search_bin(key, value, slot_group, slots, slot, fp);
    }
    else if (bin_state == PART_DIRTY) {
        while(bin_state != NONE_DIRTY) {
            read_slot_group(block, offset, id);
            bin_state = get_bin_state(slot_group, slots, resize_cnt);
        }
        return search_bin(key, value, slot_group, slots, slot, fp);
    }
    else {
        return search_old(key, value, id, slots, old_block, old_offset, block, offset, slot, fp);
    }
}

int RemoteShard::search(const std::string &key, std::string &value) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    // check_sync();
    int rc;
    // level1
    uint64_t hash1 = lv1_hash(key);
    uint64_t index, bin, slot, group, block, offset;
    uint8_t fp;
    Slot *slot_group = tl.lv1_slot_buf;
    // static int cnt1=0;
    // cnt1++;

    bool read_handover = false;
    std::pair<bool, bool> lock_res = std::make_pair(false, false);

    int search_res = 0;

#ifdef ENABLE_READ_DELEGATION
    lock_res = local_lock_table_->acquire_local_read_lock(key);
    read_handover = (lock_res.first && !lock_res.second);
#endif
    if (read_handover) {
        goto search_finish;
    }
    
    split_hash_result(hash1, hash_table->metadata.log_bins, LOG_SLOTS, bin, block, offset, slot, fp);
#ifdef IS_BLOCKING
    if (search_bins_blocking(key, value, 0, LV1_SLOTS, bin, block, offset, slot, fp) == 0) goto search_finish;
#else
    if (block < resize_cnt) {
        read_lv1_slot_group(block, offset);
        int bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
        if (!slot_group[LOCK_ID].lock && bin_state == ALL_DIRTY) {
            move_lv1_bin(block, offset, slot_group);
        }
        int res = search_bin(key, value, slot_group, LV1_SLOTS, slot, fp);  // TODO: dirty case
        if (res == 0) goto search_finish;
    }
    else {
        bool predict_first = DEFAULT_PRE;
        uint64_t old_block, old_offset;
        split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
        uint64_t bin_addr = tl.hash_table->lv1_bin[old_block] + old_offset * sizeof(Lv1Bin);
        // predict_first = !bin_map1.count(bin_addr);
        if (resize_cnt && predict_first) {
            int res = search_old(key, value, 0, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0) goto search_finish;
        }
        else {
            int res = search_new(key, value, 0, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0) goto search_finish;
        }
    }
#endif
    static int cnt2=0;
    cnt2++;
    // if(cnt2%(SYNC_T*10)==0){
    //     SDS_INFO("search overflow %d %d", cnt1,cnt2);
    // }
    if (!slot_group[OVERFLOW_ID].lock) {
        SDS_INFO("negative search");
        search_res = ENOENT;
        goto search_finish;
    }

    // level2
    uint64_t hash2[CHOICE];
    uint8_t dummy_fp;
    Slot *slots[2];
    for (int i = 0; i < CHOICE; i++) {
        hash2[i] = lv2_hash(key, i);
        slots[i] = tl.lv2_slot_buf[i];
        split_hash_result(hash2[i], hash_table->metadata.log_bins, LV2_SLOTS, bin, block, offset, slot, dummy_fp);
#ifdef IS_BLOCKING
        if (search_bins_blocking(key, value, i + 1, LV2_SLOTS, bin, block, offset, slot, fp) == 0) goto search_finish;
#else
        if (block < resize_cnt) {
            read_lv2_slot_group(block, offset, i);
            int bin_state = get_bin_state(slots[i], LV2_SLOTS, resize_cnt);
            if (!slots[i][LOCK_ID].lock && bin_state == ALL_DIRTY) {
                move_lv2_bin(block, offset, slots[i]);
            }
            int res = search_bin(key, value, slots[i], LV2_SLOTS, slot, fp);
            if (res == 0) goto search_finish;
        }
        else {
            bool predict_first = DEFAULT_PRE;
            uint64_t old_block, old_offset;
            split_bin(bin & ((1 << (resize_cnt - 1)) - 1), LOG_BINS, old_block, old_offset);
            uint64_t bin_addr = tl.hash_table->lv2_bin[old_block] + old_offset * sizeof(Lv2Bin);
            // predict_first = !bin_map2.count(bin_addr);
            if (resize_cnt && predict_first) {
                int res = search_old(key, value, i + 1, LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0) goto search_finish;
            }
            else {
                int res = search_new(key, value, i + 1, LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0) goto search_finish;
            }
        }
#endif
    }
    SDS_INFO("negative search");
    search_res = ENOENT;

search_finish:
#ifdef ENABLE_READ_DELEGATION
    local_lock_table_->release_local_read_lock(key, lock_res, search_res, value);
#endif

    return search_res;
}

int RemoteShard::insert_bin(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, Slot *slot_group, uint64_t bin_addr, int slots, int begin, uint8_t fp) {
    // static int cnt = 0;
    // double latency = 0;
    // timespec start, end;
    // cnt++;
    ThreadLocal &thl = thread_data[GetThreadID()];

#ifdef ENABLE_WRITE_COMBINING
    // local_lock_table_->get_combining_value(key, new_slot.raw);
#endif

    // uniqueness check
    for (int i = 0; i < slots; i++) {
        int idx = (begin + i) % slots;
        Slot slot = slot_group[idx];
        if (slot.len && slot.fp == fp) {
            std::string old_value;
            int rc = read_block(key, old_value, slot);
            if (rc == 0) {
                new_slot.lock = slot.lock;
                while (atomic_update_slot(bin_addr + sizeof(Slot) * idx, slot_group + idx, new_slot));
                return EEXIST;
            }
        }
    }

    for (int i = 0; i < slots; i++) {
        int idx = (begin + i) % slots;
        Slot slot = slot_group[idx];
        if (!slot.len) {
            // if ((idx & (SLOT_GROUP - 1)) == DIRTY_ID) {
                new_slot.lock = slot.lock;
            // }
            // clock_gettime(CLOCK_REALTIME, &start);
            int rc = atomic_update_slot(bin_addr + sizeof(Slot) * idx, slot_group + idx, new_slot);
            // clock_gettime(CLOCK_REALTIME, &end);
            // latency += (((end.tv_sec - start.tv_sec) * 1E9 + (end.tv_nsec - start.tv_nsec))) / 1000.0;
            // if(cnt%300000==0){
            //     SDS_INFO("%.3lf us", latency);
            // }
            if (rc == 0) {
                if (id == 0) {
                    thl.lv1_cnt.fetch_add(1, std::memory_order_relaxed);
                }
                else {
                    thl.lv2_cnt.fetch_add(1, std::memory_order_relaxed);
                }
                return 0;
            }
            else {
                // check if any concurrent thread insert the same key
                slot = slot_group[idx];
                if (!slot.len || slot.fp != fp) continue;
                std::string old_value;
                int rc = read_block(key, old_value, slot);
                if (rc == 0) {
                    while (atomic_update_slot(bin_addr + sizeof(Slot) * idx, slot_group + idx, new_slot));
                    return 0;
                }
            }
            // lv1_retry_cnt_++;
            // if (lv1_retry_cnt_ % 10000 == 0) {
            //     SDS_INFO("%d",lv1_retry_cnt_);
            // }
            // guard.retry_task();
        }
    }
    return ENOENT;
}

int RemoteShard::insert_old_blocking(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(old_block, old_offset, id);
    bool is_lock = slot_group[LOCK_ID].lock;
    bool is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
    if (!is_lock) {
        if (!is_dirty) {
            read_slot_group(block, offset, id);
        }
        else {
            bool is_moved;
            if (id == 0) is_moved = move_lv1_bin(old_block, old_offset, slot_group);
            else is_moved = move_lv2_bin(old_block, old_offset, slot_group);

            if (!is_moved) {
                while(is_lock || is_dirty) {
                    // SDS_INFO("%d %d %ld %ld %d %d", slot_group[LOCK_ID].lock, slots, block, offset, slot, fp);
                    read_slot_group(block, offset, id);
                    is_lock = slot_group[LOCK_ID].lock;
                    is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
                }
            }
            else {
                return insert_bin(guard, key, id, new_slot, tl.new_slot_buf, bin_addr, slots, slot, fp);
            }
        }
    }
    else {
        while (is_lock) {
            // SDS_INFO("%d %d %ld %ld %d %d", slot_group[LOCK_ID].lock, slots, block, offset, slot, fp);
            read_slot_group(old_block, old_offset, id);
            is_lock = slot_group[LOCK_ID].lock;
        }
        read_slot_group(block, offset, id);
    }
    return insert_bin(guard, key, id, new_slot, slot_group, bin_addr, slots, slot, fp);
}

int RemoteShard::insert_new_blocking(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(block, offset, id);
    bool is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
    if (!is_dirty) {
        return insert_bin(guard, key, id, new_slot, slot_group, bin_addr, slots, slot, fp);
    }
    else {
        return insert_old_blocking(guard, key, id, new_slot, bin_addr, slots, old_block, old_offset, block, offset, slot, fp);
    }
}

int RemoteShard::insert_old(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(old_block, old_offset, id);
    bool is_lock = slot_group[LOCK_ID].lock;
    int bin_state = get_bin_state(slot_group, slots, resize_cnt);
    if (!is_lock) {
        if (bin_state == NONE_DIRTY) {
            read_slot_group(block, offset, id);
        }
        else if (bin_state == PART_DIRTY) {
            while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%ld %d %ld %ld %d %d", slot_group[LOCK_ID].lock, slots, block, offset, slot, fp);
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
        else {
            bool is_moved;
            if (id == 0) is_moved = move_lv1_bin(old_block, old_offset, slot_group);
            else is_moved = move_lv2_bin(old_block, old_offset, slot_group);

            if (!is_moved) {
                while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%ld %d %ld %ld %d %d", slot_group[LOCK_ID].lock, slots, block, offset, slot, fp);
                    read_slot_group(block, offset, id);
                    bin_state = get_bin_state(slot_group, slots, resize_cnt);
                }
            }
            else {
                return insert_bin(guard, key, id, new_slot, tl.new_slot_buf, bin_addr, slots, slot, fp);
            }
        }
    }
    else {
        if (bin_state == NONE_DIRTY) {
            read_slot_group(block, offset, id);
        }
        else if (bin_state == PART_DIRTY) {
            while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%ld %d %ld %ld %d %d", slot_group[LOCK_ID].lock, slots, block, offset, slot, fp);
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
        else {
            while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%ld %d %ld %ld %d %d", slot_group[LOCK_ID].lock, slots, block, offset, slot, fp);
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
    }
    return insert_bin(guard, key, id, new_slot, slot_group, bin_addr, slots, slot, fp);
}

int RemoteShard::insert_new(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(block, offset, id);
    int bin_state = get_bin_state(slot_group, slots, resize_cnt);
    if (bin_state == NONE_DIRTY) {
        return insert_bin(guard, key, id, new_slot, slot_group, bin_addr, slots, slot, fp);
    }
    else if (bin_state == PART_DIRTY) {
        while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%ld %d %ld %ld %d %d", slot_group[LOCK_ID].lock, slots, block, offset, slot, fp);
            read_slot_group(block, offset, id);
            bin_state = get_bin_state(slot_group, slots, resize_cnt);
        }
        return insert_bin(guard, key, id, new_slot, slot_group, bin_addr, slots, slot, fp);
    }
    else {
        return insert_old(guard, key, id, new_slot, bin_addr, slots, old_block, old_offset, block, offset, slot, fp);
    }
}

int RemoteShard::insert(const std::string &key, const std::string &value) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    ThreadLocal &thl = thread_data[GetThreadID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;

    bool write_handover = false;
    std::pair<bool, bool> lock_res = std::make_pair(false, false);

    check_sync();
    if (load_factor() > RESIZE_THR) {
        resize();
    }

    BackoffGuard guard(tl_backoff);
    Slot new_slot;
    int rc;

    // level1
    uint64_t hash1 = lv1_hash(key);
    uint64_t index, bin, slot, group, block, offset;
    uint8_t fp, b_id;
    uint64_t pre_log_bins = LOG_BINS + ROUND_UP(hash_table->metadata.log_bins - LOG_BINS);
    Slot *slot_group = tl.lv1_slot_buf;
    uint64_t lv1_bin_addr;

    split_hash_result(hash1, pre_log_bins, LOG_SLOTS, bin, block, offset, slot, fp);
    b_id = bin >> (pre_log_bins - 4);
    split_bin(bin & ((1 << hash_table->metadata.log_bins) - 1), LOG_BINS, block, offset);
    write_block(key, value, fp, b_id, new_slot);
    lv1_bin_addr = hash_table->lv1_bin[block] + offset * sizeof(Lv1Bin);
    // SDS_INFO("%s %lx %lx %lx %lx %lx %lx %x %x", key.c_str(), hash1, index, bin,slot,block,offset, fp, b_id);

#ifdef ENABLE_WRITE_COMBINING
    lock_res = local_lock_table_->acquire_local_write_lock(key, new_slot.raw);
    write_handover = (lock_res.first && !lock_res.second);
#endif
    if (write_handover) {
        goto insert_finish;
    }

#ifdef IS_BLOCKING
    if (block < resize_cnt) {
        read_lv1_slot_group(block, offset);
        if (!slot_group[LOCK_ID].lock && slot_group[DIRTY_ID].lock != (resize_cnt & 1)) {
            move_lv1_bin(block, offset, slot_group);
        }
        while (slot_group[LOCK_ID].lock) {
            read_lv1_slot_group(block, offset);
        }
        int res = insert_bin(guard, key, 0, new_slot, slot_group, lv1_bin_addr, LV1_SLOTS, slot, fp);
        if (res == 0 || res == EEXIST) goto insert_finish;
    }
    else {
        bool predict_first = DEFAULT_PRE;
        uint64_t old_block, old_offset;
        split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
        uint64_t bin_addr = tl.hash_table->lv1_bin[old_block] + old_offset * sizeof(Lv1Bin);
        if (resize_cnt && predict_first) {
            int res = insert_old_blocking(guard, key, 0, new_slot, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0 || res == EEXIST) goto insert_finish;
        }
        else {
            int res = insert_new_blocking(guard, key, 0, new_slot, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0 || res == EEXIST) goto insert_finish;
        }
    }
#else
    if (block < resize_cnt) {
        read_lv1_slot_group(block, offset);
        int bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
        if (!slot_group[LOCK_ID].lock && bin_state == ALL_DIRTY) {
            bool is_moved = move_lv1_bin(block, offset, slot_group);
            if (!is_moved) {
                while(bin_state != NONE_DIRTY) {
                    read_lv1_slot_group(block, offset);
                    bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
                }
            }
        }
        else {
            while(bin_state != NONE_DIRTY) {
                read_lv1_slot_group(block, offset);
                bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
            }
        }
        int res = insert_bin(guard, key, 0, new_slot, slot_group, lv1_bin_addr, LV1_SLOTS, slot, fp);
        if (res == 0 || res == EEXIST) goto insert_finish;
    }
    else {
        bool predict_first = DEFAULT_PRE;
        uint64_t old_block, old_offset;
        split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
        uint64_t bin_addr = tl.hash_table->lv1_bin[old_block] + old_offset * sizeof(Lv1Bin);
        // predict_first = !bin_map1.count(bin_addr);
        if (resize_cnt && predict_first) {
            int res = insert_old(guard, key, 0, new_slot, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0 || res == EEXIST) goto insert_finish;
        }
        else {
            int res = insert_new(guard, key, 0, new_slot, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0 || res == EEXIST) goto insert_finish;
        }
    }
#endif

    if (!slot_group[OVERFLOW_ID].lock) {
        Slot new_val;
        while (true) {
            new_val = slot_group[OVERFLOW_ID];
            new_val.lock = true;
            // SDS_INFO("%x %x",slot_group[OVERFLOW_ID].raw,new_val.raw);
            rc = node_->compare_and_swap(tl.cas_buf, lv1_bin_addr + OVERFLOW_ID * sizeof(Slot), slot_group[OVERFLOW_ID].raw, new_val.raw, Initiator::Option::Sync);
            assert(!rc);
            slot_group[OVERFLOW_ID].raw = *(tl.cas_buf);
            if (slot_group[OVERFLOW_ID].lock) {
                break;
            }
        }
    }

    // level2
    uint64_t hash2[CHOICE];
    uint8_t b_id2[CHOICE];
    uint8_t dummy_fp;
    Slot *slots[2];
    uint64_t lv2_bin_addr[2];

    for (int i = 0; i < CHOICE; i++) {
        hash2[i] = lv2_hash(key, i);
        split_hash_result(hash2[i], pre_log_bins, LOG_GROUP, bin, block, offset, slot, dummy_fp);
        b_id2[i] = bin >> (pre_log_bins - 4);
        split_bin(bin & ((1 << hash_table->metadata.log_bins) - 1), LOG_BINS, block, offset);
        new_slot.bid = b_id2[i];
        slots[i] = tl.lv2_slot_buf[i];
        lv2_bin_addr[i] = hash_table->lv2_bin[block] + offset * sizeof(Lv2Bin);

#ifdef IS_BLOCKING
        if (block < resize_cnt) {
            read_lv2_slot_group(block, offset, i);
            if (!slots[i][LOCK_ID].lock && slots[i][DIRTY_ID].lock != (resize_cnt & 1)) {
                move_lv2_bin(block, offset, slots[i]);
            }
            while (slots[i][LOCK_ID].lock) {
                read_lv2_slot_group(block, offset, i);
            }
            int res = insert_bin(guard, key, i + 1, new_slot, slots[i], lv2_bin_addr[i], LV2_SLOTS, slot, fp);
            if (res == 0 || res == EEXIST) goto insert_finish;
        }
        else {
            bool predict_first = DEFAULT_PRE;
            uint64_t old_block, old_offset;
            split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
            uint64_t bin_addr = tl.hash_table->lv2_bin[old_block] + old_offset * sizeof(Lv2Bin);
            if (resize_cnt && predict_first) {
                int res = insert_old_blocking(guard, key, i + 1, new_slot, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0 || res == EEXIST) goto insert_finish;
            }
            else {
                int res = insert_new_blocking(guard, key, i + 1, new_slot, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0 || res == EEXIST) goto insert_finish;
            }
        }
#else
        if (block < resize_cnt) {
            read_lv2_slot_group(block, offset, i);
            int bin_state = get_bin_state(slots[i], LV2_SLOTS, resize_cnt);
            if (!slots[i][LOCK_ID].lock && bin_state == ALL_DIRTY) {
                bool is_moved = move_lv2_bin(block, offset, slots[i]);
                if (!is_moved) {
                    while(bin_state != NONE_DIRTY) {
                        read_lv2_slot_group(block, offset, i);
                        bin_state = get_bin_state(slots[i], LV2_SLOTS, resize_cnt);
                    }
                }
            }
            else {
                while(bin_state != NONE_DIRTY) {
                    read_lv2_slot_group(block, offset, i);
                    bin_state = get_bin_state(slots[i], LV2_SLOTS, resize_cnt);
                }
            }
            int res = insert_bin(guard, key, i + 1, new_slot, slots[i], lv2_bin_addr[i], LV2_SLOTS, slot, fp);
            if (res == 0 || res == EEXIST) goto insert_finish;
        }
        else {
            bool predict_first = DEFAULT_PRE;
            uint64_t old_block, old_offset;
            split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
            uint64_t bin_addr = tl.hash_table->lv2_bin[old_block] + old_offset * sizeof(Lv2Bin);
            // predict_first = !bin_map2.count(bin_addr);
            if (resize_cnt && predict_first) {
                int res = insert_old(guard, key, i + 1, new_slot, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0 || res == EEXIST) goto insert_finish;
            }
            else {
                int res = insert_new(guard, key, i + 1, new_slot, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0 || res == EEXIST) goto insert_finish;
            }
        }
#endif
    }

    SDS_INFO("insert overflow!, key %s, load factor %.2lf", key.c_str(), load_factor());

insert_finish:
#ifdef ENABLE_WRITE_COMBINING
  local_lock_table_->release_local_write_lock(key, lock_res);
#endif

    return 0;
}

int RemoteShard::update_bin(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, Slot *slot_group, uint64_t bin_addr, int slots, int begin, uint8_t fp) {
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

int RemoteShard::update_old_blocking(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(old_block, old_offset, id);
    bool is_lock = slot_group[LOCK_ID].lock;
    bool is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
    if (!is_lock) {
        if (!is_dirty) {
            read_slot_group(block, offset, id);
        }
        else {
            bool is_moved;
            if (id == 0) is_moved = move_lv1_bin(old_block, old_offset, slot_group);
            else is_moved = move_lv2_bin(old_block, old_offset, slot_group);

            if (!is_moved) {
                while(is_lock || is_dirty) {
                    read_slot_group(block, offset, id);
                    is_lock = slot_group[LOCK_ID].lock;
                    is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
                }
            }
            else {
                return update_bin(key, guard, id, new_slot, tl.new_slot_buf, bin_addr, slots, slot, fp);
            }
        }
    }
    else {
        while (is_lock) {
            read_slot_group(old_block, old_offset, id);
            is_lock = slot_group[LOCK_ID].lock;
        }
        read_slot_group(block, offset, id);
    }
    return update_bin(key, guard, id, new_slot, slot_group, bin_addr, slots, slot, fp);
}

int RemoteShard::update_new_blocking(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(block, offset, id);
    bool is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
    if (!is_dirty) {
        return update_bin(key, guard, id, new_slot, slot_group, bin_addr, slots, slot, fp);
    }
    else {
        return update_old_blocking(key, guard, id, new_slot, bin_addr, slots, old_block, old_offset, block, offset, slot, fp);
    }
}

int RemoteShard::update_old(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(old_block, old_offset, id);
    bool is_lock = slot_group[LOCK_ID].lock;
    int bin_state = get_bin_state(slot_group, slots, resize_cnt);
    if (!is_lock) {
        if (bin_state == NONE_DIRTY) {
            read_slot_group(block, offset, id);
        }
        else if (bin_state == PART_DIRTY) {
            while(bin_state != NONE_DIRTY) {
            SDS_INFO("%d %d %d %ld %ld %d %d", id, bin_state, slots, block, offset, slot, fp);
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
        else {
            bool is_moved;
            if (id == 0) is_moved = move_lv1_bin(old_block, old_offset, slot_group);
            else is_moved = move_lv2_bin(old_block, old_offset, slot_group);

            if (!is_moved) {
                while(bin_state != NONE_DIRTY) {
            SDS_INFO("%d %d %d %ld %ld %ld %ld %d %d", id, bin_state, slots, old_block, old_offset, block, offset, slot, fp);
                    read_slot_group(block, offset, id);
                    bin_state = get_bin_state(slot_group, slots, resize_cnt);
                }
            }
            else {
                return update_bin(key, guard, id, new_slot, tl.new_slot_buf, bin_addr, slots, slot, fp);
            }
        }
    }
    else {
        if (bin_state == NONE_DIRTY) {
            read_slot_group(block, offset, id);
        }
        else if (bin_state == PART_DIRTY) {
            while(bin_state != NONE_DIRTY) {
            SDS_INFO("%d %d %d %ld %ld %d %d",id, bin_state, slots, block, offset, slot, fp);
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
        else {
            while(bin_state != NONE_DIRTY) {
            SDS_INFO("%d %d %d %ld %ld %d %d", id, bin_state, slots, block, offset, slot, fp);
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
    }
    return update_bin(key, guard, id, new_slot, slot_group, bin_addr, slots, slot, fp);
}

int RemoteShard::update_new(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(block, offset, id);
    int bin_state = get_bin_state(slot_group, slots, resize_cnt);
    if (bin_state == NONE_DIRTY) {
        return update_bin(key, guard, id, new_slot, slot_group, bin_addr, slots, slot, fp);
    }
    else if (bin_state == PART_DIRTY) {
        while(bin_state != NONE_DIRTY) {
            SDS_INFO("%d %d %d %ld %ld %d %d", id, bin_state, slots, block, offset, slot, fp);
            read_slot_group(block, offset, id);
            bin_state = get_bin_state(slot_group, slots, resize_cnt);
        }
        return update_bin(key, guard, id, new_slot, slot_group, bin_addr, slots, slot, fp);
    }
    else {
        return update_old(key, guard, id, new_slot, bin_addr, slots, old_block, old_offset, block, offset, slot, fp);
    }
}

int RemoteShard::update(const std::string &key, const std::string &value) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    ThreadLocal &thl = thread_data[GetThreadID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;

    // check_sync();

    BackoffGuard guard(tl_backoff);
    Slot new_slot;
    int rc;

    // level1
    uint64_t hash1 = lv1_hash(key);
    uint64_t index, bin, slot, group, block, offset;
    uint8_t fp, b_id;
    uint64_t pre_log_bins = LOG_BINS + ROUND_UP(hash_table->metadata.log_bins - LOG_BINS);
    Slot *slot_group = tl.lv1_slot_buf;
    uint64_t lv1_bin_addr;

    split_hash_result(hash1, pre_log_bins, LOG_SLOTS, bin, block, offset, slot, fp);
    b_id = bin >> (pre_log_bins - 4);
    split_bin(bin & ((1 << hash_table->metadata.log_bins) - 1), LOG_BINS, block, offset);
    write_block(key, value, fp, b_id, new_slot);
    lv1_bin_addr = hash_table->lv1_bin[block] + offset * sizeof(Lv1Bin);
    // SDS_INFO("%ld %ld %ld %ld %d", bin,slot,block,offset, fp);
    
#ifdef IS_BLOCKING
    if (block < resize_cnt) {
        read_lv1_slot_group(block, offset);
        if (!slot_group[LOCK_ID].lock && slot_group[DIRTY_ID].lock != (resize_cnt & 1)) {
            move_lv1_bin(block, offset, slot_group);
        }
        while (slot_group[LOCK_ID].lock) {
            read_lv1_slot_group(block, offset);
        }
        int res = update_bin(key, guard, 0, new_slot, slot_group, lv1_bin_addr, LV1_SLOTS, slot, fp);
        if (res == 0) return 0;
    }
    else {
        bool predict_first = DEFAULT_PRE;
        uint64_t old_block, old_offset;
        split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
        uint64_t bin_addr = tl.hash_table->lv1_bin[old_block] + old_offset * sizeof(Lv1Bin);
        if (resize_cnt && predict_first) {
            int res = update_old_blocking(key, guard, 0, new_slot, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0) return 0;
        }
        else {
            int res = update_new_blocking(key, guard, 0, new_slot, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0) return 0;
        }
    }
#else
    if (block < resize_cnt) {
        read_lv1_slot_group(block, offset);
        int bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
        if (!slot_group[LOCK_ID].lock && bin_state == ALL_DIRTY) {
            bool is_moved = move_lv1_bin(block, offset, slot_group);
            if (!is_moved) {
                while(bin_state != NONE_DIRTY) {
            SDS_INFO("%d %ld %ld %ld %d", bin_state, block, offset, slot, fp);
                    read_lv1_slot_group(block, offset);
                    bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
                }
            }
        }
        int res = update_bin(key, guard, 0, new_slot, slot_group, lv1_bin_addr, LV1_SLOTS, slot, fp);
        if (res == 0) return 0;
    }
    else {
        bool predict_first = DEFAULT_PRE;
        uint64_t old_block, old_offset;
        split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
        uint64_t bin_addr = tl.hash_table->lv1_bin[old_block] + old_offset * sizeof(Lv1Bin);
        // predict_first = !bin_map1.count(bin_addr);
        if (resize_cnt && predict_first) {
            int res = update_old(key, guard, 0, new_slot, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0) return 0;
        }
        else {
            int res = update_new(key, guard, 0, new_slot, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
            if (res == 0) return 0;
        }
    }
#endif

    if (!slot_group[OVERFLOW_ID].lock)
        return 0;
    
    // level2
    uint64_t hash2[CHOICE];
    uint8_t b_id2[CHOICE];
    uint8_t dummy_fp;
    Slot *slots[2];
    uint64_t lv2_bin_addr[2];

    for (int i = 0; i < CHOICE; i++) {
        hash2[i] = lv2_hash(key, i);
        split_hash_result(hash2[i], pre_log_bins, LOG_GROUP, bin, block, offset, slot, dummy_fp);
        b_id2[i] = bin >> (pre_log_bins - 4);
        split_bin(bin & ((1 << hash_table->metadata.log_bins) - 1), LOG_BINS, block, offset);
        new_slot.bid = b_id2[i];
        slots[i] = tl.lv2_slot_buf[i];
        lv2_bin_addr[i] = hash_table->lv2_bin[block] + offset * sizeof(Lv2Bin);
        // SDS_INFO("%d %ld %ld %ld %ld %d",i, bin,slot,block,offset, fp);

#ifdef IS_BLOCKING
        if (block < resize_cnt) {
            read_lv2_slot_group(block, offset, i);
            if (!slots[i][LOCK_ID].lock && slots[i][DIRTY_ID].lock != (resize_cnt & 1)) {
                move_lv2_bin(block, offset, slots[i]);
            }
            while (slots[i][LOCK_ID].lock) {
                read_lv2_slot_group(block, offset, i);
            }
            int res = update_bin(key, guard, i + 1, new_slot, slots[i], lv2_bin_addr[i], LV2_SLOTS, slot, fp);
            if (res == 0) return 0;
        }
        else {
            bool predict_first = DEFAULT_PRE;
            uint64_t old_block, old_offset;
            split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
            uint64_t bin_addr = tl.hash_table->lv2_bin[old_block] + old_offset * sizeof(Lv2Bin);
            if (resize_cnt && predict_first) {
                int res = update_old_blocking(key, guard, i + 1, new_slot, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0) return 0;
            }
            else {
                int res = update_new_blocking(key, guard, i + 1, new_slot, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0) return 0;
            }
        }
#else
        if (block < resize_cnt) {
            read_lv2_slot_group(block, offset, i);
            int bin_state = get_bin_state(slots[i], LV2_SLOTS, resize_cnt);
            if (!slots[i][LOCK_ID].lock && bin_state == ALL_DIRTY) {
                bool is_moved = move_lv2_bin(block, offset, slots[i]);
                if (!is_moved) {
                    while(bin_state != NONE_DIRTY) {
                        SDS_INFO("%d %d %ld %ld %ld %d", i, bin_state, block, offset, slot, fp);
                        read_lv2_slot_group(block, offset, i);
                        bin_state = get_bin_state(slots[i], LV2_SLOTS, resize_cnt);
                    }
                }
            }
            int res = update_bin(key, guard, i + 1, new_slot, slots[i], lv2_bin_addr[i], LV2_SLOTS, slot, fp);
            if (res == 0) return 0;
        }
        else {
            bool predict_first = DEFAULT_PRE;
            uint64_t old_block, old_offset;
            split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
            uint64_t bin_addr = tl.hash_table->lv2_bin[old_block] + old_offset * sizeof(Lv2Bin);
            // predict_first = !bin_map2.count(bin_addr);
            if (resize_cnt && predict_first) {
                int res = update_old(key, guard, i + 1, new_slot, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0) return 0;
            }
            else {
                int res = update_new(key, guard, i + 1, new_slot, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
                if (res == 0) return 0;
            }
        }
#endif
    }
    return ENOENT;
}

int RemoteShard::rmw(const std::string &key, const std::function<std::string(const std::string &)> &transform) {
    uint64_t slot_addr = 0;
    return 0;
    Slot new_slot, *slot;
    BackoffGuard guard(tl_backoff);
    int rc;

    while (true) {
        std::string old_value;
        
        auto value = transform(old_value);
        GlobalAddress addr;
        write_block(key, value, 0, 0, new_slot, addr);
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

int RemoteShard::remove_bin(const std::string &key, BackoffGuard &guard, int id, Slot *slot_group, uint64_t bin_addr, int slots, int begin, uint8_t fp) {
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

int RemoteShard::remove_old_blocking(const std::string &key, BackoffGuard &guard, int id, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(old_block, old_offset, id);
    bool is_lock = slot_group[LOCK_ID].lock;
    bool is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
    if (!is_lock) {
        if (!is_dirty) {
            read_slot_group(block, offset, id);
        }
        else {
            bool is_moved;
            if (id == 0) is_moved = move_lv1_bin(old_block, old_offset, slot_group);
            else is_moved = move_lv2_bin(old_block, old_offset, slot_group);

            if (!is_moved) {
                while(is_lock || is_dirty) {
                    read_slot_group(block, offset, id);
                    is_lock = slot_group[LOCK_ID].lock;
                    is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
                }
            }
            else {
                return remove_bin(key, guard, id, tl.new_slot_buf, bin_addr, slots, slot, fp);
            }
        }
    }
    else {
        while (is_lock) {
            read_slot_group(old_block, old_offset, id);
            is_lock = slot_group[LOCK_ID].lock;
        }
        read_slot_group(block, offset, id);
    }
    return remove_bin(key, guard, id, slot_group, bin_addr, slots, slot, fp);
}

int RemoteShard::remove_new_blocking(const std::string &key, BackoffGuard &guard, int id, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(block, offset, id);
    bool is_dirty = slot_group[DIRTY_ID].lock != (resize_cnt & 1);
    if (!is_dirty) {
        return remove_bin(key, guard, id, slot_group, bin_addr, slots, slot, fp);
    }
    else {
        return remove_old_blocking(key, guard, id, bin_addr, slots, old_block, old_offset, block, offset, slot, fp);
    }
}

int RemoteShard::remove_old(const std::string &key, BackoffGuard &guard, int id, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(old_block, old_offset, id);
    bool is_lock = slot_group[LOCK_ID].lock;
    int bin_state = get_bin_state(slot_group, slots, resize_cnt);
    if (!is_lock) {
        if (bin_state == NONE_DIRTY) {
            read_slot_group(block, offset, id);
        }
        else if (bin_state == PART_DIRTY) {
            while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%d %d %d %ld %ld %d %d", id, bin_state, slots, block, offset, slot, fp);
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
        else {
            bool is_moved;
            if (id == 0) is_moved = move_lv1_bin(old_block, old_offset, slot_group);
            else is_moved = move_lv2_bin(old_block, old_offset, slot_group);

            if (!is_moved) {
                while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%d %d %d %ld %ld %ld %ld %d %d", id, bin_state, slots, old_block, old_offset, block, offset, slot, fp);
                    read_slot_group(block, offset, id);
                    bin_state = get_bin_state(slot_group, slots, resize_cnt);
                }
            }
            else {
                return remove_bin(key, guard, id, tl.new_slot_buf, bin_addr, slots, slot, fp);
            }
        }
    }
    else {
        if (bin_state == NONE_DIRTY) {
            read_slot_group(block, offset, id);
        }
        else if (bin_state == PART_DIRTY) {
            while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%d %d %d %ld %ld %d %d",id, bin_state, slots, block, offset, slot, fp);
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
        else {
            while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%d %d %d %ld %ld %d %d", id, bin_state, slots, block, offset, slot, fp);
                read_slot_group(block, offset, id);
                bin_state = get_bin_state(slot_group, slots, resize_cnt);
            }
        }
    }
    return remove_bin(key, guard, id, slot_group, bin_addr, slots, slot, fp);
}

int RemoteShard::remove_new(const std::string &key, BackoffGuard &guard, int id, uint64_t bin_addr, int slots,
                                  uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    Slot *slot_group;
    if (id == 0) {
        slot_group = tl.lv1_slot_buf;
    }
    else {
        slot_group = tl.lv2_slot_buf[id - 1];
    }

    read_slot_group(block, offset, id);
    int bin_state = get_bin_state(slot_group, slots, resize_cnt);
    if (bin_state == NONE_DIRTY) {
        return remove_bin(key, guard, id, slot_group, bin_addr, slots, slot, fp);
    }
    else if (bin_state == PART_DIRTY) {
        while(bin_state != NONE_DIRTY) {
            // SDS_INFO("%d %d %d %ld %ld %d %d", id, bin_state, slots, block, offset, slot, fp);
            read_slot_group(block, offset, id);
            bin_state = get_bin_state(slot_group, slots, resize_cnt);
        }
        return remove_bin(key, guard, id, slot_group, bin_addr, slots, slot, fp);
    }
    else {
        return remove_old(key, guard, id, bin_addr, slots, old_block, old_offset, block, offset, slot, fp);
    }
}

int RemoteShard::remove(const std::string &key) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    ThreadLocal &thl = thread_data[GetThreadID()];
    HashTable *hash_table = tl.hash_table;
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;

    // check_sync();

    BackoffGuard guard(tl_backoff);
    // Slot new_slot;
    int rc;

    uint64_t index, bin, slot, group, block, offset;
    uint8_t fp, b_id;
    uint64_t pre_log_bins = LOG_BINS + ROUND_UP(hash_table->metadata.log_bins - LOG_BINS);
    Slot *slot_group;
    
    // level2
    uint64_t hash2[CHOICE];
    uint8_t b_id2[CHOICE];
    uint8_t dummy_fp;
    Slot *slots[2];
    uint64_t lv2_bin_addr[2];

    for (int i = 0; i < CHOICE; i++) {
        hash2[i] = lv2_hash(key, i);
        split_hash_result(hash2[i], pre_log_bins, LOG_GROUP, bin, block, offset, slot, dummy_fp);
        b_id2[i] = bin >> (pre_log_bins - 4);
        split_bin(bin & ((1 << hash_table->metadata.log_bins) - 1), LOG_BINS, block, offset);
        slots[i] = tl.lv2_slot_buf[i];
        lv2_bin_addr[i] = hash_table->lv2_bin[block] + offset * sizeof(Lv2Bin);
        // SDS_INFO("%d %ld %ld %ld %ld %d",i, bin,slot,block,offset, fp);

#ifdef IS_BLOCKING
        if (block < resize_cnt) {
            read_lv2_slot_group(block, offset, i);
            if (!slots[i][LOCK_ID].lock && slots[i][DIRTY_ID].lock != (resize_cnt & 1)) {
                move_lv2_bin(block, offset, slots[i]);
            }
            while (slots[i][LOCK_ID].lock) {
                read_lv2_slot_group(block, offset, i);
            }
            int res = remove_bin(key, guard, i + 1, slots[i], lv2_bin_addr[i], LV2_SLOTS, slot, fp);
        }
        else {
            bool predict_first = DEFAULT_PRE;
            uint64_t old_block, old_offset;
            split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
            uint64_t bin_addr = tl.hash_table->lv2_bin[old_block] + old_offset * sizeof(Lv2Bin);
            if (resize_cnt && predict_first) {
                int res = remove_old_blocking(key, guard, i + 1, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
            }
            else {
                int res = remove_new_blocking(key, guard, i + 1, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
            }
        }
#else
        if (block < resize_cnt) {
            read_lv2_slot_group(block, offset, i);
            int bin_state = get_bin_state(slots[i], LV2_SLOTS, resize_cnt);
            if (!slots[i][LOCK_ID].lock && bin_state == ALL_DIRTY) {
                bool is_moved = move_lv2_bin(block, offset, slots[i]);
                if (!is_moved) {
                    while(bin_state != NONE_DIRTY) {
                        read_lv2_slot_group(block, offset, i);
                        bin_state = get_bin_state(slots[i], LV2_SLOTS, resize_cnt);
                    }
                }
            }
            int res = remove_bin(key, guard, i + 1, slots[i], lv2_bin_addr[i], LV2_SLOTS, slot, fp);
        }
        else {
            bool predict_first = DEFAULT_PRE;
            uint64_t old_block, old_offset;
            split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
            uint64_t bin_addr = tl.hash_table->lv2_bin[old_block] + old_offset * sizeof(Lv2Bin);
            // predict_first = !bin_map2.count(bin_addr);
            if (resize_cnt && predict_first) {
                int res = remove_old(key, guard, i + 1, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
            }
            else {
                int res = remove_new(key, guard, i + 1, lv2_bin_addr[i], LV2_SLOTS, old_block, old_offset, block, offset, slot, fp);
            }
        }
#endif
    }

    // level1
    uint64_t hash1 = lv1_hash(key);
    uint64_t lv1_bin_addr;
    slot_group = tl.lv1_slot_buf;

    split_hash_result(hash1, pre_log_bins, LOG_SLOTS, bin, block, offset, slot, fp);
    b_id = bin >> (pre_log_bins - 4);
    split_bin(bin & ((1 << hash_table->metadata.log_bins) - 1), LOG_BINS, block, offset);
    lv1_bin_addr = hash_table->lv1_bin[block] + offset * sizeof(Lv1Bin);
    // SDS_INFO("%ld %ld %ld %ld %d", bin,slot,block,offset, fp);
    
#ifdef IS_BLOCKING
    if (block < resize_cnt) {
        read_lv1_slot_group(block, offset);
        if (!slot_group[LOCK_ID].lock && slot_group[DIRTY_ID].lock != (resize_cnt & 1)) {
            move_lv1_bin(block, offset, slot_group);
        }
        while (slot_group[LOCK_ID].lock) {
            read_lv1_slot_group(block, offset);
        }
        int res = remove_bin(key, guard, 0, slot_group, lv1_bin_addr, LV1_SLOTS, slot, fp);
    }
    else {
        bool predict_first = DEFAULT_PRE;
        uint64_t old_block, old_offset;
        split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
        uint64_t bin_addr = tl.hash_table->lv1_bin[old_block] + old_offset * sizeof(Lv1Bin);
        if (resize_cnt && predict_first) {
            int res = remove_old_blocking(key, guard, 0, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
        }
        else {
            int res = remove_new_blocking(key, guard, 0, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
        }
    }
#else
    if (block < resize_cnt) {
        read_lv1_slot_group(block, offset);
        int bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
        if (!slot_group[LOCK_ID].lock && bin_state == ALL_DIRTY) {
            bool is_moved = move_lv1_bin(block, offset, slot_group);
            if (!is_moved) {
                while(bin_state != NONE_DIRTY) {
                    read_lv1_slot_group(block, offset);
                    bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
                }
            }
        }
        int res = remove_bin(key, guard, 0, slot_group, lv1_bin_addr, LV1_SLOTS, slot, fp);
    }
    else {
        bool predict_first = DEFAULT_PRE;
        uint64_t old_block, old_offset;
        split_bin(bin & ((1 << (hash_table->metadata.log_bins - 1)) - 1), LOG_BINS, old_block, old_offset);
        uint64_t bin_addr = tl.hash_table->lv1_bin[old_block] + old_offset * sizeof(Lv1Bin);
        // predict_first = !bin_map1.count(bin_addr);
        if (resize_cnt && predict_first) {
            int res = remove_old(key, guard, 0, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
        }
        else {
            int res = remove_new(key, guard, 0, lv1_bin_addr, LV1_SLOTS, old_block, old_offset, block, offset, slot, fp);
        }
    }
#endif
    return ENOENT;
}



int RemoteShard::resize() {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    uint64_t &lock = tl.hash_table->metadata.lock;
    GlobalAddress addr = ht_addr_ + offsetof(HashTable, metadata.lock);
    addr.node = 0;
    int rc = node_->compare_and_swap(&lock, addr, 0, 1, Initiator::Option::Sync);
    assert(!rc);
    if (lock != 0) {
        return 0;
    }
    addr.node = ht_addr_.node;
    lock = 1;
    SDS_INFO("begin to resize");

    uint64_t bins = tl.hash_table->metadata.bins;
    uint64_t log_bins = tl.hash_table->metadata.log_bins;
    uint64_t resize_cnt = tl.hash_table->metadata.resize_cnt;
    uint64_t new_bins = bins << 1;
    uint64_t new_log_bins = log_bins + 1;
    uint64_t new_resize_cnt = resize_cnt + 1;
    

    uint64_t lv1_bin_size = bins * sizeof(Lv1Bin);
    uint64_t lv2_bin_size = bins * sizeof(Lv2Bin);
    GlobalAddress new_lv1_addr = node_->alloc_memory(node_id_, lv1_bin_size);
    tl.hash_table->lv1_bin[new_resize_cnt] = new_lv1_addr.offset;
    GlobalAddress new_lv2_addr = node_->alloc_memory(node_id_, lv2_bin_size);
    tl.hash_table->lv2_bin[new_resize_cnt] = new_lv2_addr.offset;
    addr = ht_addr_ + offsetof(HashTable, lv1_bin);
    rc = node_->write(tl.hash_table->lv1_bin, addr, sizeof(uint64_t) * (RESIZE_LIM * 2 + 1));
    assert(!rc);
    addr = ht_addr_ + offsetof(HashTable, move_bins);
    *tl.cas_buf = 0;
    rc = node_->write(tl.cas_buf, addr, sizeof(uint64_t));
    assert(!rc);
    node_->sync();

    tl.hash_table->metadata.bins = new_bins;
    tl.hash_table->metadata.log_bins = new_log_bins;
    tl.hash_table->metadata.resize_cnt = new_resize_cnt;
    addr = ht_addr_ + offsetof(HashTable, metadata);
    rc = node_->write(&tl.hash_table->metadata, addr, sizeof(TableMetadata), Initiator::Option::Sync);
    assert(!rc);
    SDS_INFO("resize: %d %d %ld %ld %ld", GetThreadID(), GetTaskID(),
            tl.hash_table->metadata.bins, tl.hash_table->metadata.log_bins, tl.hash_table->metadata.resize_cnt);

    // wait until all other task sync metadata
    BackoffGuard guard(tl_backoff);
    while (true) {
        addr = ht_addr_ + offsetof(HashTable, sync_tasks);
        addr.node = 0;
        rc = node_->read(tl.cas_buf, addr, sizeof(uint64_t), Initiator::Option::Sync);
        // SDS_INFO("%ld %ld", *tl.cas_buf, tl.hash_table->tasks);
        if (*tl.cas_buf >= tl.hash_table->tasks - 1) break;
        // guard.retry_task();
    }
    
    // after this, other operation can sync
    // begin to move bins
    /*
    SDS_INFO("begin to move");
    uint64_t cnt=0;
    // level1
    for (int i = 0; i <= resize_cnt; i++) {
        for (uint64_t j = 0; j < bins_[i]; j++) {
            for (int k = 0; k < LV1_SLOTS; k++) {
                Slot old_slot, new_slot;
                // acquire slot lock
                while (true) {
                    addr.offset = tl.hash_table->lv1_bin[i] + j * sizeof(Lv1Bin) + k * sizeof(Slot);
                    rc = node_->read(old_data_buf_, addr, sizeof(Slot), Initiator::Option::Sync);
                    assert(!rc);
                    old_slot = *((Slot *)old_data_buf_);
                    if (!old_slot.raw) break;
                    new_slot = old_slot;
                    new_slot.lock = 1;
                    rc = node_->compare_and_swap(old_data_buf_, addr, old_slot.raw, new_slot.raw, Initiator::Option::Sync);
                    assert(!rc);
                    if (old_slot.raw == *((uint64_t *)old_data_buf_))  break;
                }
                if (!old_slot.raw) continue;
                
                BlockHeader *block = tl.block_buf;
                uint64_t block_len = new_slot.len * BLOCK_UNIT;
                do {
                    rc = node_->read(block, GlobalAddress(node_id_, new_slot.pointer), block_len, Initiator::Option::Sync);
                    assert(!rc);
                } while (check_kv_block_crc32(block, block_len));
                std::string key(get_kv_block_key(block)), value(get_kv_block_value(block));
                uint64_t hash1 = lv1_hash(key);
                uint64_t index, bin, slot_id, group, block_id, offset;
                uint8_t fp;
                split_hash(hash1, tl.hash_table->metadata.log_bins + LOG_SLOTS, LOG_FPRINT, index, fp);
                split_index(index, tl.hash_table->metadata.log_bins, LOG_SLOTS - LOG_GROUP, LOG_GROUP, bin, slot_id, group);
                split_bin(bin, LOG_BINS, block_id, offset);
                if (block_id < new_resize_cnt) continue;
                // SDS_INFO("%ld %ld %ld", cnt, block_id, offset);

                cnt++;
                Slot *slot_group = tl.lv1_slot_buf;
                uint64_t lv1_bin_addr = tl.hash_table->lv1_bin[block_id] + offset * sizeof(Lv1Bin);
                read_lv1_slot_group(block_id, offset, slot_id);
                for (int l = 0; l < LV1_SLOTS; l++) {
                    int idx = (slot_id * SLOT_GROUP + group + l) % LV1_SLOTS;
                    Slot slot = slot_group[idx];
                    if (!slot.raw) {
                        rc = atomic_update_slot(lv1_bin_addr + sizeof(Slot) * idx, slot_group + idx, old_slot);
                        if (rc == 0) {
                            break;
                        }
                        guard.retry_task();
                    }
                }
                // insert(key, value); // TODO: use known info
                *(uint64_t *)new_data_buf_ = 0;
                addr.offset = tl.hash_table->lv1_bin[i] + j * sizeof(Lv1Bin) + k * sizeof(Slot);
                node_->write(new_data_buf_, addr, sizeof(Slot), Initiator::Option::Sync);
                // remove(key);        // TODO: use known info
            }
        }
    }
    SDS_INFO("resize: move %ld slots", cnt);

    // level2
    for (int i = 0; i <= resize_cnt; i++) {
        for (uint64_t j = 0; j < bins_[i]; j++) {
            for (int k = 0; k < LV2_SLOTS; k++) {
                Slot old_slot, new_slot;
                // acquire slot lock
                while (true) {
                    addr.offset = tl.hash_table->lv2_bin[i] + j * sizeof(Lv2Bin) + k * sizeof(Slot);
                    rc = node_->read(old_data_buf_, addr, sizeof(Slot), Initiator::Option::Sync);
                    assert(!rc);
                    old_slot = *((Slot *)old_data_buf_);
                    if (!old_slot.raw) break;
                    new_slot = old_slot;
                    new_slot.lock = 1;
                    rc = node_->compare_and_swap(old_data_buf_, addr, old_slot.raw, new_slot.raw, Initiator::Option::Sync);
                    assert(!rc);
                    if (old_slot.raw == *((uint64_t *)old_data_buf_))  break;
                }
                if (!old_slot.raw) continue;
                
                BlockHeader *block = tl.block_buf;
                uint64_t block_len = new_slot.len * BLOCK_UNIT;
                do {
                    rc = node_->read(block, GlobalAddress(node_id_, new_slot.pointer), block_len, Initiator::Option::Sync);
                    assert(!rc);
                } while (check_kv_block_crc32(block, block_len));
                std::string key(get_kv_block_key(block)), value(get_kv_block_value(block));
                uint64_t hash2;
                uint64_t index, bin, slot_id, block_id, offset, group;
                uint8_t fp;
                for (int l = 0; l < CHOICE; l++) {
                    hash2 = lv2_hash(key, l);
                    split_hash(hash2, tl.hash_table->metadata.log_bins + LOG_GROUP, LOG_FPRINT, index, fp);
                    split_index(index, tl.hash_table->metadata.log_bins, 0, LOG_GROUP, bin, slot_id, group);
                    split_bin(bin, LOG_BINS, block_id, offset);
                    if (offset == j) {
                        break;
                    }
                }
                // uint64_t hash1 = lv1_hash(key);
                // uint64_t index, bin, slot_id, group, block_id, offset;
                // uint8_t fp;
                // split_hash(hash1, tl.hash_table->metadata.log_bins + LOG_SLOTS, LOG_FPRINT, index, fp);
                // split_index(index, tl.hash_table->metadata.log_bins, LOG_SLOTS - LOG_GROUP, LOG_GROUP, bin, slot_id, group);
                // split_bin(bin, LOG_BINS, block_id, offset);
                if (block_id < new_resize_cnt) continue;

                cnt++;
                Slot *slot_group = tl.lv2_slot_buf[0];
                uint64_t lv2_bin_addr = tl.hash_table->lv2_bin[block_id] + offset * sizeof(Lv2Bin);
                read_lv2_slot_group(block_id, offset, slot_id, 0);
                node_->sync();
                for (int l = 0; l < LV2_SLOTS; l++) {
                    int idx = (slot_id * SLOT_GROUP + group + l) % LV2_SLOTS;
                    Slot slot = slot_group[idx];
                    if (!slot.raw) {
                        rc = atomic_update_slot(lv2_bin_addr + sizeof(Slot) * idx, slot_group + idx, old_slot);
                        if (rc == 0) {
                            break;
                        }
                        guard.retry_task();
                    }
                }
                // insert(key, value); // TODO: use known info
                *(uint64_t *)new_data_buf_ = 0;
                addr.offset = tl.hash_table->lv2_bin[i] + j * sizeof(Lv2Bin) + k * sizeof(Slot);
                node_->write(new_data_buf_, addr, sizeof(Slot), Initiator::Option::Sync);
                // remove(key);        // TODO: use known info
            }
        }
    }
    SDS_INFO("resize: move %ld slots", cnt);
    */
    /*
    for (int i = 0; i < resize_cnt; i++) {
        for (uint64_t j = 0; j < bins_[i]; j += DATA_BLOCK) {
            addr.offset = tl.hash_table->lv1_bin[i] + j;
            rc = node_->read(old_data_buf_, addr, DATA_BLOCK, Initiator::Option::Sync);
            assert(!rc);
            for (int k = 0; k < DATA_BLOCK; k += LV1_SLOTS) {
                Slot *slot = (Slot *)(old_data_buf_ + k);
                for (int l = 0; l < LV1_SLOTS; l++) {
                    if (!slot[l].raw) continue;
                    BlockHeader *block = tl.block_buf;
                    uint64_t block_len = slot[l].len * BLOCK_UNIT;
                    do {
                        rc = node_->read(block, GlobalAddress(node_id_, slot[l].pointer), block_len, Initiator::Option::Sync);
                        assert(!rc);
                    } while (check_kv_block_crc32(block, block_len));
                    std::string key(get_kv_block_key(block)), value(get_kv_block_value(block));
                    uint64_t hash1 = lv1_hash(key);
                    uint64_t index, bin, slot, group, block_id, offset;
                    uint8_t fp;
                    split_hash(hash1, tl.hash_table->metadata.log_bins + LOG_SLOTS, LOG_FPRINT, index, fp);
                    split_index(index, tl.hash_table->metadata.log_bins, LOG_SLOTS - LOG_GROUP, LOG_GROUP, bin, slot, group);
                    split_bin(bin, LOG_BINS, block_id, offset);
                    if (block_id < new_resize_cnt - 1) continue;

                    insert(key, value); // TODO: use known info
                    remove(key);        // TODO: use known info
                }
            }
        }
    }
    */

    // reset resize value and release lock
    addr = ht_addr_ + offsetof(HashTable, sync_tasks);
    addr.node = 0;
    *tl.cas_buf = 0;
    rc = node_->write(tl.cas_buf, addr, sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);

    addr = ht_addr_ + offsetof(HashTable, metadata.lock);
    lock = 0;
    rc = node_->write(&lock, addr, sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);
    return 0;
}

int RemoteShard::move_bins() {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    HashTable *hash_table = tl.hash_table;
    Slot *slot_group = tl.lv1_slot_buf;
    Slot *slot_group2 = tl.lv2_slot_buf[0];
    uint64_t resize_cnt = hash_table->metadata.resize_cnt;
    bool is_lock;
    int bin_state;
    int cnt1=0,cnt2=0;

    for (int i = 0; i < resize_cnt; i++) {
        for (uint64_t j = 0; j < bins_[i]; j++) {
            read_lv1_slot_group(i, j);
            is_lock = slot_group[LOCK_ID].lock;
            bin_state = get_bin_state(slot_group, LV1_SLOTS, resize_cnt);
            if (!is_lock && bin_state == ALL_DIRTY) {
                cnt1 += move_lv1_bin(i, j, slot_group);
            }
        }
    }

    for (int i = 0; i < resize_cnt; i++) {
        for (uint64_t j = 0; j < bins_[i]; j++) {
            read_lv2_slot_group(i, j, 0);
            is_lock = slot_group2[LOCK_ID].lock;
            bin_state = get_bin_state(slot_group, LV2_SLOTS, resize_cnt);
            if (!is_lock && bin_state == ALL_DIRTY) {
                cnt2 += move_lv2_bin(i, j, slot_group2);
            }
        }
    }
    SDS_INFO("%d %d",cnt1,cnt2);

    return 0;
}

void RemoteShard::global_lock() {
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

void RemoteShard::global_unlock() {
    return;
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    auto &lock = tl.hash_table->metadata.lock;
    GlobalAddress addr = ht_addr_ + ((char *) &lock - (char *) tl.hash_table);
    lock = 0;
    int rc = node_->write(&lock, addr, sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);
}

int RemoteShard::read_block(const std::string &key, std::string &value, Slot &slot, bool retry) {
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

int RemoteShard::write_block(const std::string &key, const std::string &value, uint8_t fp, uint8_t b_id, Slot &slot) {
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
    slot.bid = b_id;
    slot.len = block_len / BLOCK_UNIT;
    slot.pointer = addr.offset;
    return 0;
}

int RemoteShard::write_block(const std::string &key, const std::string &value, uint8_t fp, uint8_t b_id,
                                   RemoteShard::Slot &slot, sds::GlobalAddress &addr) {
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
    slot.bid = b_id;
    slot.len = block_len / BLOCK_UNIT;
    slot.pointer = addr.offset;
    return 0;
}

int RemoteShard::atomic_update_slot(uint64_t addr, Slot *slot, Slot &new_val) {
    uint64_t old_val = slot->raw;
    int rc = node_->compare_and_swap(slot, GlobalAddress(node_id_, addr), old_val, new_val.raw,
                                     Initiator::Option::Sync);
    assert(!rc);
    return old_val == slot->raw ? 0 : EAGAIN;
}
