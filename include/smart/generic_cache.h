
#ifndef SDS_GENERIC_CACHE_H
#define SDS_GENERIC_CACHE_H

#include "thread.h"
#include <vector>

namespace sds {
    template<class KeyType, class ValueType>
    class GenericCache {
    public:
        GenericCache() : capacity_(1000000) {
            if (getenv("CACHE_SIZE")) {
                capacity_ = atoi(getenv("CACHE_SIZE"));
            }
            for (int i = 0; i < LOCK_COUNT; ++i) {
                version_lock[i].store(0, std::memory_order_relaxed);
            }
            valid.resize(capacity_, false);
            entries.resize(capacity_);
        }

        void add(const KeyType &key, const ValueType &value) {
            int idx = hash_func(key) % capacity_;
            if (!try_lock(idx)) return;
            valid[idx] = true;
            entries[idx].key = key;
            entries[idx].value = value;
            unlock(idx);
        }

        bool find(const KeyType &key, ValueType &value) {
            int idx = hash_func(key) % capacity_;
            uint64_t start_ver = lock_read(idx);
            if (!valid[idx] || entries[idx].key != key) {
                return false;
            }
            auto &entry = entries[idx];
            value = entry.value;
            return unlock_read(idx, start_ver);
        }

        void erase(const KeyType &key) {
            int idx = hash_func(key) % capacity_;
            if (!try_lock(idx)) return;
            valid[idx] = false;
            unlock(idx);
        }

    private:
        bool try_lock(int idx) {
            int lock_idx = idx % LOCK_COUNT;
            uint64_t old_ver = version_lock[lock_idx].load();
            if (old_ver & 1) return false;
            return version_lock[lock_idx].compare_exchange_weak(old_ver, old_ver + 1);
        }

        void unlock(int idx) {
            int lock_idx = idx % LOCK_COUNT;
            version_lock[lock_idx].fetch_add(1);
        }

        uint64_t lock_read(int idx) {
            int lock_idx = idx % LOCK_COUNT;
            return version_lock[lock_idx].load();
        }

        bool unlock_read(int idx, uint64_t start_ver) {
            int lock_idx = idx % LOCK_COUNT;
            uint64_t end_ver = version_lock[lock_idx].load();
            return start_ver == end_ver && !(start_ver & 1);
        }

    private:
        const static size_t LOCK_COUNT = 64 * 1024;

        struct Entry {
            KeyType key;
            ValueType value;
        };

        std::hash<KeyType> hash_func;
        size_t capacity_;
        std::vector<Entry> entries;
        std::vector<bool> valid;
        std::atomic<uint64_t> version_lock[LOCK_COUNT];
    };
}

#endif //SDS_GENERIC_CACHE_H
