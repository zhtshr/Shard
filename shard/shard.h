
#ifndef SDS_SHARD_H
#define SDS_SHARD_H

#include <sys/param.h>

#include "util/crc.h"
#include "util/murmur.h"
#include "util/generator.h"
#include "smart/common.h"
#include "util/json_config.h"

#include "smart/global_address.h"
#include "smart/generic_cache.h"
#include "smart/initiator.h"
#include "smart/target.h"
#include "smart/backoff.h"
#include "hashutil.h"
#include "locallocktable.h"

using namespace sds;

#define USE_RESIZE

#define LOG_FPRINT  7
#define LOG_BINS    18
#define INIT_BINS   (1 << (LOG_BINS))
#define LOG_SLOTS   6
#define LOG_GROUP   3
#define LV1_SLOTS   (1 << (LOG_SLOTS))
#define LV2_SLOTS   8
#define SLOT_GROUP  (1 << (LOG_GROUP))
#define CHOICE      2
#define RESIZE_LIM  8
#define RESIZE_THR  0.8
#define LOCK_ID     0
#define OVERFLOW_ID 1
#define VER_BEGIN   2
#define VER_END     7
#define DIRTY_ID    7
#define DATA_BLOCK  4096

#define BLOCK_UNIT  64
#define BLOCK_LEN   4096

#define SYNC_T      10000
#define SYNC_TASK   1000
#define SYNC_ALPHA  0.1
#define DEFAULT_PRE (false)

#define ROUND_UP(x) ((((x) >> 2) + 1) << 2)

class RemoteShard {
public:
    static const char *Name() { return "IcrbergHash"; }

    static int Setup(JsonConfig config, Target &target) {

        size_t ht_size = roundup(sizeof(HashTable), kChunkSize);
        size_t lv1_bin_size = roundup(sizeof(Lv1Bin) * INIT_BINS, kChunkSize);
        size_t lv2_bin_size = roundup(sizeof(Lv2Bin) * INIT_BINS, kChunkSize);
        size_t lv3_bin_size = roundup(sizeof(Slot) * INIT_BINS, kChunkSize);

        HashTable *hash_table = (HashTable *) target.alloc_chunk(ht_size / kChunkSize);
        if (!hash_table)
            return -1;
        memset(hash_table, 0, sizeof(HashTable));
        hash_table->metadata.bins = INIT_BINS;
        hash_table->metadata.log_bins = LOG_BINS;
        hash_table->metadata.resize_cnt = 0;
        hash_table->lv1_balls = 0;
        hash_table->lv2_balls = 0;
        hash_table->lv3_balls = 0;

        Lv1Bin *lv1_bin = (Lv1Bin *) target.alloc_chunk(lv1_bin_size / kChunkSize);
        Lv2Bin *lv2_bin = (Lv2Bin *) target.alloc_chunk(lv2_bin_size / kChunkSize);
        Slot   *lv3_bin = (Slot *) target.alloc_chunk(lv3_bin_size / kChunkSize);
        if (!lv1_bin  || !lv2_bin || !lv3_bin)
            return -1;
        memset(lv1_bin, 0, sizeof(Lv1Bin) * INIT_BINS);
        memset(lv2_bin, 0, sizeof(Lv2Bin) * INIT_BINS);
        memset(lv3_bin, 0, sizeof(Slot) * INIT_BINS);

        hash_table->lv1_bin[0] = target.rel_ptr(lv1_bin).raw;
        hash_table->lv2_bin[0] = target.rel_ptr(lv2_bin).raw;
        hash_table->lv3_bin    = target.rel_ptr(lv3_bin).raw;
        SDS_INFO("%ld %ld %ld", hash_table->lv1_bin[0], hash_table->lv2_bin[0], hash_table->lv3_bin);

        int64_t total_tasks = config.get("total_tasks").get_int64();
        hash_table->tasks = total_tasks;
        hash_table->sync_tasks = 0;

        target.set_root_entry(0, target.rel_ptr(hash_table).raw);
        return 0;
    }

    RemoteShard(JsonConfig config, int max_threads);

    RemoteShard(JsonConfig config, int max_threads, Initiator *node, int node_id);

    RemoteShard(JsonConfig config, Initiator *node, int node_id); // for server node

    ~RemoteShard();

    RemoteShard(const RemoteShard &) = delete;

    RemoteShard &operator=(const RemoteShard &) = delete;

    int search(const std::string &key, std::string &value);

    int insert(const std::string &key, const std::string &value);

    int update(const std::string &key, const std::string &value);

    int remove(const std::string &key);

    int scan(const std::string &key, size_t count, std::vector<std::string> &value_list) {
        assert(0 && "not supported");
    }

    int rmw(const std::string &key, const std::function<std::string(const std::string &)> &transform);

    std::function<void()> get_poll_task(int &running_tasks) {
        return node_->get_poll_task(running_tasks);
    }

    void run_tasks() {
        node_->run_tasks();
    }

    Initiator *initiator() { return node_; }    

private:
    
    enum DirtyState {
        NONE_DIRTY,     // 0
        INVALID,
        PART_DIRTY,     // 2
        ALL_DIRTY       // 3
    };

    struct BlockHeader {
        uint32_t key_len;
        uint32_t value_len;
    };

    union Slot {
        struct {
            uint64_t lock: 1;
            uint64_t fp: 7;
            uint64_t bid: 4;
            uint64_t len: 4;
            uint64_t pointer: 48;
        };
        uint64_t raw;
    };

    struct Lv1Bin {
        Slot slot[LV1_SLOTS];
    };

    struct Lv2Bin {
        Slot slot[LV2_SLOTS];
    };

    struct TableMetadata {
        uint64_t bins;
        uint64_t log_bins;
        // TODO:resize
        uint64_t lock = 0;
        uint64_t resize_cnt;
    };

    struct HashTable {
        TableMetadata metadata;
        int64_t lv1_balls;
        int64_t lv2_balls;
        int64_t lv3_balls;
        uint64_t lv1_bin[RESIZE_LIM]; // TODO:overflow cnt
        uint64_t lv2_bin[RESIZE_LIM]; // TODO:overflow cnt
        uint64_t lv3_bin;
        int64_t tasks;
        int64_t sync_tasks;
        uint64_t move_bins;
    };

    struct TaskLocal {
        uint64_t lock = 0;
        uint64_t resize_flag = 0;
        uint64_t op_cnt = 0;
        uint64_t *cas_buf;
        HashTable *hash_table;
        Slot *lv1_slot_buf;
        Slot *lv2_slot_buf[CHOICE];
        Slot *lv3_slot_buf;
        Slot *new_slot_buf;
        BlockHeader *block_buf;
    };

    struct ThreadLocal {
        std::atomic<int> cnt;
        std::atomic<int64_t> lv1_cnt;
        std::atomic<int64_t> lv2_cnt;
        std::atomic<int64_t> lv3_cnt;
    };

public:
    int read_hash_table();

    int read_hash_table(int task_id);

    int move_bins();

private:
    uint64_t seed[3] = { 1234567890123456789ll, 8765432198765432123ll, 4567890123456789012ll };

    uint64_t lv1_hash(const std::string &key) {
        return MurmurHash64A(key.c_str(), key.length(), seed[0]);
    }

    uint64_t lv2_hash(const std::string &key, uint8_t i) {
        return MurmurHash64A(key.c_str(), key.length(), seed[i + 1]);
    }

    void split_hash_result(uint64_t hash, uint64_t log_bins, uint64_t log_slots, uint64_t &bin,
                           uint64_t &block, uint64_t &offset, uint64_t &slot, uint8_t &fp);

    int init_task(int thread, int task, uint64_t total_tasks);

    int64_t tot_balls();

    int64_t tot_capacity();

    double load_factor();

    int resize();

    int check_sync();

    int read_lv1_slot_group(uint64_t block, uint64_t offset);

    int read_lv2_slot_group(uint64_t block, uint64_t offset, int i);

    int read_slot_group(uint64_t block, uint64_t offset, int id);

    bool move_lv1_bin(uint64_t block, uint64_t offset, Slot *slot_group);

    bool move_lv2_bin(uint64_t block, uint64_t offset, Slot *slot_group);

    bool move_bin(uint64_t block, uint64_t offset, Slot *slot_group, int id);

    bool try_move_lv1_bin(uint64_t hash);

    bool try_move_lv2_bin(uint64_t hash);

    int get_bin_state(const Slot *slot_group, int slots, int resize_cnt);

    int get_version(const Slot *slot_group);

    int search_bin(const std::string &key, std::string &value, const Slot *slot_group, int slots, int begin, uint8_t fp);

    int search_old_blocking(const std::string &key, std::string &value, int id, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int search_new_blocking(const std::string &key, std::string &value, int id, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);
    
    int search_bins_blocking(const std::string &key, std::string &value, int id, int slots,
                            uint64_t bin, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int search_old(const std::string &key, std::string &value, int id, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int search_new(const std::string &key, std::string &value, int id, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int insert_bin(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, Slot *slot_group, uint64_t bin_addr, int slots, int begin, uint8_t fp);

    int insert_old_blocking(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int insert_new_blocking(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int insert_old(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int insert_new(BackoffGuard &guard, const std::string &key, int id, Slot new_slot, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int update_bin(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, Slot *slot_group,
                   uint64_t bin_addr, int slots, int begin, uint8_t fp);

    int update_old_blocking(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int update_new_blocking(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int update_old(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int update_new(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int remove_bin(const std::string &key, BackoffGuard &guard, int id, Slot *slot_group,
                   uint64_t bin_addr, int slots, int begin, uint8_t fp);

    int remove_old_blocking(const std::string &key, BackoffGuard &guard, int id, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int remove_new_blocking(const std::string &key, BackoffGuard &guard, int id, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int remove_old(const std::string &key, BackoffGuard &guard, int id, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int remove_new(const std::string &key, BackoffGuard &guard, int id, uint64_t bin_addr, int slots,
                   uint64_t old_block, uint64_t old_offset, uint64_t block, uint64_t offset, int slot, uint8_t fp);

    int read_block(const std::string &key, std::string &value, Slot &slot, bool retry = true);

    int write_block(const std::string &key, const std::string &value, uint8_t fp, uint8_t b_id, Slot &slot);

    int write_block(const std::string &key, const std::string &value, uint8_t fp, uint8_t b_id, Slot &slot, GlobalAddress &addr);

    int atomic_update_slot(uint64_t addr, Slot *slot, Slot &new_val);

    static inline char *get_kv_block_key(BlockHeader *block) {
        return (char *) &block[1];
    }

    static inline char *get_kv_block_value(BlockHeader *block) {
        return (char *) &block[1] + block->key_len;
    }

    static inline size_t get_kv_block_len(const std::string &key, const std::string &value) {
        return roundup(sizeof(BlockHeader) + key.size() + value.size() + sizeof(uint32_t), BLOCK_UNIT);
    }

    static inline uint32_t calc_crc32(BlockHeader *block, size_t block_len) {
        size_t length = block_len - sizeof(uint32_t);
        return CRC::Calculate(block, length, CRC::CRC_32());
    }

    static inline void update_kv_block_crc32(BlockHeader *block, size_t block_len) {
        uint32_t *crc_field = ((uint32_t *) ((uintptr_t) block + block_len)) - 1;
        *crc_field = calc_crc32(block, block_len);
    }

    static inline int check_kv_block_crc32(BlockHeader *block, size_t block_len) {
        uint32_t crc_field = ((uint32_t *) ((uintptr_t) block + block_len))[-1];
        if (crc_field != calc_crc32(block, block_len)) {
            return -1;
        } else {
            return 0;
        }
    }

    void global_lock();

    void global_unlock();

private:
    JsonConfig config_;
    Initiator *node_;
    int node_id_;
    bool is_shared_;
    GlobalAddress ht_addr_;
    uint64_t bins_[RESIZE_LIM];
    char *old_data_buf_;
    char *new_data_buf_;
    alignas(128) TaskLocal tl_data_[kMaxThreads][kMaxTasksPerThread];
    alignas(128) ThreadLocal thread_data[kMaxThreads];

    int lv1_retry_cnt_ = 0;
    int lv2_retry_cnt_ = 0;

    LocalLockTable *local_lock_table_ = nullptr;
};

class RemoteShardMultiShard {
    constexpr const static uint32_t kHashSeed = 0x1b873593;

    RemoteShard *get_shard(const std::string &key) {
        uint32_t hash = MurmurHash3_x86_32(key.c_str(), (int) key.size(), kHashSeed);
        return shard_list_[hash % nr_shards_];
    }

public:
    static const char *Name() { return "HashTableMultiShard"; }

    RemoteShardMultiShard(JsonConfig config, int max_threads) {
        node_ = new Initiator();
        if (!node_) {
            exit(EXIT_FAILURE);
        }
        nr_shards_ = (int) config.get("memory_servers").size();
        if (getenv("MEMORY_NODES")) {
            nr_shards_ = std::min(nr_shards_, atoi(getenv("MEMORY_NODES")));
        }
        shard_list_.resize(nr_shards_);
        for (int i = 0; i < nr_shards_; ++i) {
            shard_list_[i] = new RemoteShard(config, max_threads, node_, i);
        }
    }

    ~RemoteShardMultiShard() {
        for (auto &shard : shard_list_) {
            delete shard;
        }
        delete node_;
    }

    RemoteShardMultiShard(const RemoteShardMultiShard &) = delete;

    RemoteShardMultiShard &operator=(const RemoteShardMultiShard &) = delete;

    int search(const std::string &key, std::string &value) {
        return get_shard(key)->search(key, value);
    }

    int insert(const std::string &key, const std::string &value) {
        return get_shard(key)->insert(key, value);
    }

    int update(const std::string &key, const std::string &value) {
        return get_shard(key)->update(key, value);
    }

    int remove(const std::string &key) {
        return get_shard(key)->remove(key);
    }

    int scan(const std::string &key, size_t count, std::vector<std::string> &value_list) {
        assert(0 && "not supported");
    }

    int rmw(const std::string &key, const std::function<std::string(const std::string &)> &transform) {
        return get_shard(key)->rmw(key, transform);
    }

    std::function<void()> get_poll_task(int &running_tasks) {
        return node_->get_poll_task(running_tasks);
    }

    void run_tasks() {
        node_->run_tasks();
    }

    Initiator *initiator() { return node_; }

private:
    Initiator *node_;
    int nr_shards_;
    std::vector<RemoteShard *> shard_list_;
};

#endif //SDS_SHARD_H
