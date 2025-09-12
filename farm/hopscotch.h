
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


#define BUCKET_SLOTS    8
#define LOG_BUCKETS     18
#define INIT_BUCKETS    (1 << (LOG_BUCKETS))
#define LOG_FPRINT      7
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

#define H           16
#define H_BUCKET    ((H) / (BUCKET_SLOTS))

#define ROUND_UP(x) ((((x) >> 2) + 1) << 2)

class RemoteHopscotch {
public:
    static const char *Name() { return "Hopscotch"; }

    static int Setup(JsonConfig config, Target &target) {

        size_t ht_size = roundup(sizeof(HashTable), kChunkSize);
        size_t bucket_size = roundup(sizeof(Bucket) * INIT_BUCKETS, kChunkSize);

        HashTable *hash_table = (HashTable *) target.alloc_chunk(ht_size / kChunkSize);
        if (!hash_table)
            return -1;
        memset(hash_table, 0, sizeof(HashTable));
        hash_table->metadata.buckets = INIT_BUCKETS;
        hash_table->metadata.log_buckets = LOG_BUCKETS;
        hash_table->metadata.resize_cnt = 0;
        hash_table->balls = 0;

        Bucket *bucket = (Bucket *) target.alloc_chunk(bucket_size / kChunkSize);
        if (!bucket)
            return -1;
        memset(bucket, 0, sizeof(Bucket) * INIT_BUCKETS);

        hash_table->bucket = target.rel_ptr(bucket).raw;
        SDS_INFO("%ld", hash_table->bucket);

        target.set_root_entry(0, target.rel_ptr(hash_table).raw);
        return 0;
    }

    RemoteHopscotch(JsonConfig config, int max_threads);

    RemoteHopscotch(JsonConfig config, int max_threads, Initiator *node, int node_id);

    RemoteHopscotch(JsonConfig config, Initiator *node, int node_id); // for server node

    ~RemoteHopscotch();

    RemoteHopscotch(const RemoteHopscotch &) = delete;

    RemoteHopscotch &operator=(const RemoteHopscotch &) = delete;

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

    struct BlockHeader {
        uint32_t key_len;
        uint32_t value_len;
    };

    union Slot {
        struct {
            uint64_t lock: 1;
            uint64_t len: 7;        // bv in slot[0]
            uint64_t incar: 1;
            uint64_t fp: 7;         // fv in slot[0]
            uint64_t pointer: 48;
        };
        uint64_t raw;
    };

    struct Bucket {
        Slot slot[BUCKET_SLOTS];
    };


    struct TableMetadata {
        uint64_t buckets;
        uint64_t log_buckets;
        // TODO:resize
        uint64_t lock = 0;
        uint64_t resize_cnt;
    };

    struct HashTable {
        TableMetadata metadata;
        int64_t balls;
        uint64_t bucket;
    };

    struct TaskLocal {
        uint64_t lock = 0;
        uint64_t *cas_buf;
        HashTable *hash_table;
        Bucket *bucket_buf;
        Bucket *new_bucket_buf;
        BlockHeader *block_buf;
    };

    struct ThreadLocal {
    };

public:
    int read_hash_table();

    int read_hash_table(int task_id);

    int move_bins();

private:
    uint64_t seed = 8765432198765432123ll;

    uint64_t hash_fun(const std::string &key) {
        return MurmurHash64A(key.c_str(), key.length(), seed);
    }

    void split_hash_result(uint64_t hash, uint64_t log_bins, uint64_t log_slots, uint64_t &bin,
                           uint64_t &block, uint64_t &offset, uint64_t &slot, uint8_t &fp);

    int init_task(int thread, int task, uint64_t total_tasks);

    int64_t tot_balls();

    int64_t tot_capacity();

    double load_factor();

    int resize();

    int check_sync();

    int read_bucket(uint64_t bucket);

    int read_bucket_neighbour(uint64_t bucket);

    int lock_bucket(Bucket *bucket_buf, uint64_t bucket);

    int unlock_bucket(Bucket *bucket_buf, uint64_t bucket);

    int move_slot(Slot *slot_group, uint64_t bucket_pos, uint64_t pos, uint64_t begin);

    bool move_bucket(uint64_t block, uint64_t offset, Slot *slot_group);

    int search_bucket(const std::string &key, std::string &value, const Bucket bucket, uint8_t fp);

    int find_empty_slot(Slot *slot_group, uint64_t begin);

    int insert_bucket(const std::string &key, Slot new_slot, Slot *slot_group, uint64_t bucket_addr, uint8_t fp, uint64_t begin);

    int update_bucket(const std::string &key, BackoffGuard &guard, int id, Slot new_slot, Slot *slot_group,
                   uint64_t bin_addr, int slots, int begin, uint8_t fp);

    int remove_bucket(const std::string &key, BackoffGuard &guard, int id, Slot *slot_group,
                   uint64_t bin_addr, int slots, int begin, uint8_t fp);

    int read_block_kv(std::string &key, std::string &value, Slot &slot);

    int read_block(const std::string &key, std::string &value, Slot &slot, bool retry = true);

    int write_block(const std::string &key, const std::string &value, uint8_t fp, Slot &slot);

    int write_block(const std::string &key, const std::string &value, uint8_t fp, Slot &slot, GlobalAddress &addr);

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
    alignas(128) TaskLocal tl_data_[kMaxThreads][kMaxTasksPerThread];
    alignas(128) ThreadLocal thread_data[kMaxThreads];

    int lv1_retry_cnt_ = 0;
    int lv2_retry_cnt_ = 0;

    LocalLockTable *local_lock_table_ = nullptr;
};

class RemoteHopscotchMultiShard {
    constexpr const static uint32_t kHashSeed = 0x1b873593;

    RemoteHopscotch *get_shard(const std::string &key) {
        uint32_t hash = MurmurHash3_x86_32(key.c_str(), (int) key.size(), kHashSeed);
        return shard_list_[hash % nr_shards_];
    }

public:
    static const char *Name() { return "HashTableMultiShard"; }

    RemoteHopscotchMultiShard(JsonConfig config, int max_threads) {
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
            shard_list_[i] = new RemoteHopscotch(config, max_threads, node_, i);
        }
    }

    ~RemoteHopscotchMultiShard() {
        for (auto &shard : shard_list_) {
            delete shard;
        }
        delete node_;
    }

    RemoteHopscotchMultiShard(const RemoteHopscotchMultiShard &) = delete;

    RemoteHopscotchMultiShard &operator=(const RemoteHopscotchMultiShard &) = delete;

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
    std::vector<RemoteHopscotch *> shard_list_;
};

#endif //SDS_SHARD_H
