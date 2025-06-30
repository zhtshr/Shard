#if !defined(_LOCAL_LOCK_TABLE_H_)
#define _LOCAL_LOCK_TABLE_H_

#include "smart/common.h"
#include "smart/task.h"
#include "hashutil.h"

#include <queue>
#include <set>
#include <atomic>
#include <mutex>

using namespace sds;

#define LOCAL_LOCK_NUM  (4 * 1024 * 1024)

enum HandoverType {
  READ_HANDOVER,
  WRITE_HANDOVER,
};

// const uint64_t seed = 658515750348153356ll;

struct LocalLockNode {
  // read waiting queue
  std::atomic<uint8_t> read_current;
  std::atomic<uint8_t> read_ticket;
  volatile bool read_handover;

  // write waiting queue
  std::atomic<uint8_t> write_current;
  std::atomic<uint8_t> write_ticket;
  volatile bool write_handover;

  /* ----- auxiliary variables for simplicity  TODO: dynamic allocate ----- */
  // identical time window start time
  std::atomic<bool> window_start;
  std::atomic<uint8_t> read_window;
  std::atomic<uint8_t> write_window;
  std::mutex r_lock;
  std::mutex w_lock;

  // hash conflict
  std::atomic<std::string*> unique_read_key;
  std::atomic<std::string*> unique_write_key;

  // read delegation
  int res;
  std::string ret_value;

  // write combining
  std::mutex wc_lock;
  uint64_t wc_buffer;

  LocalLockNode() : read_current(0), read_ticket(0), read_handover(0), write_current(0), write_ticket(0), write_handover(0),
                    window_start(0), read_window(0), write_window(0),
                    unique_read_key(0), unique_write_key(0) {}
};


class LocalLockTable {
public:

  // read-delegation
  std::pair<bool, bool> acquire_local_read_lock(const std::string& k);
  void release_local_read_lock(const std::string& k, std::pair<bool, bool> acquire_ret, int& res, std::string& ret_value);

  // write-combining
  std::pair<bool, bool> acquire_local_write_lock(const std::string& k, const uint64_t& v);
  bool get_combining_value(const std::string& k, uint64_t& v);
  void release_local_write_lock(const std::string& k, std::pair<bool, bool> acquire_ret);

private:
  uint64_t seed = 658515750348153356ll;
  LocalLockNode local_locks[LOCAL_LOCK_NUM];
};


// read-delegation
inline std::pair<bool, bool> LocalLockTable::acquire_local_read_lock(const std::string& k) {
  auto &node = local_locks[MurmurHash64A(k.c_str(), k.length(), seed) % LOCAL_LOCK_NUM];

  std::string* unique_key = nullptr;
  std::string* new_key = new std::string(k);
  bool res = node.unique_read_key.compare_exchange_strong(unique_key, new_key);
  if (!res) {
    delete new_key;
    if (*unique_key != k) {  // conflict keys
      return std::make_pair(false, true);
    }
  }

  uint8_t ticket = node.read_ticket.fetch_add(1);  // acquire local lock
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    YieldTask();
    current = node.read_current.load(std::memory_order_relaxed);
  }
  unique_key = node.unique_read_key.load();
  if (!unique_key || *unique_key != k) {  // conflict keys
    node.read_current.fetch_add(1);
    return std::make_pair(false, true);
  }
  if (!node.read_window) {
    node.read_handover = false;
  }
  return std::make_pair(node.read_handover, false);
}

// read-delegation
inline void LocalLockTable::release_local_read_lock(const std::string& k, std::pair<bool, bool> acquire_ret, int& res, std::string& ret_value) {
  if (acquire_ret.second) return;

  auto &node = local_locks[MurmurHash64A(k.c_str(), k.length(), seed) % LOCAL_LOCK_NUM];

  if (!node.read_handover) {  // winner
    node.res = res;
    node.ret_value = ret_value;
  }
  else {  // losers accept the ret val from winner
    res = node.res;
    ret_value = node.ret_value;
  }

  uint8_t ticket = node.read_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  bool start_window = false;
  if (!node.read_handover) {
    node.read_window = ((1UL << 8) + ticket - current) % (1UL << 8);
  }

  node.read_handover = ticket != (uint8_t)(current + 1);

  if (!node.read_handover) {  // next epoch
    node.unique_read_key = nullptr;
  }

  node.r_lock.lock();
  if (node.read_window) {
    -- node.read_window;
  }
  node.read_current.fetch_add(1);
  node.r_lock.unlock();

  return;
}

// write-combining
inline std::pair<bool, bool> LocalLockTable::acquire_local_write_lock(const std::string& k, const uint64_t& v) {
  auto &node = local_locks[MurmurHash64A(k.c_str(), k.length(), seed) % LOCAL_LOCK_NUM];

  std::string* unique_key = nullptr;
  std::string* new_key = new std::string(k);
  bool res = node.unique_write_key.compare_exchange_strong(unique_key, new_key);
  if (!res) {
    delete new_key;
    if (*unique_key != k) {  // conflict keys
      return std::make_pair(false, true);
    }
  }

  node.wc_lock.lock();
  node.wc_buffer = v;     // local overwrite (combining)
  node.wc_lock.unlock();

  uint8_t ticket = node.write_ticket.fetch_add(1);  // acquire local lock
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    YieldTask();
    current = node.write_current.load(std::memory_order_relaxed);
  }
  unique_key = node.unique_write_key.load();
  if (!unique_key || *unique_key != k) {  // conflict keys
    node.write_current.fetch_add(1);
    return std::make_pair(false, true);
  }
  if (!node.write_window) {
    node.write_handover = false;
  }
  return std::make_pair(node.write_handover, false);
}

// write-combining
inline bool LocalLockTable::get_combining_value(const std::string& k, uint64_t& v) {
  auto &node = local_locks[MurmurHash64A(k.c_str(), k.length(), seed) % LOCAL_LOCK_NUM];
  bool res = false;
  std::string* unique_key = node.unique_write_key.load();
  if (unique_key && *unique_key == k) {  // wc
    node.wc_lock.lock();
    res = node.wc_buffer != v;
    v = node.wc_buffer;
    node.wc_lock.unlock();
  }
  return res;
}

// write-combining
inline void LocalLockTable::release_local_write_lock(const std::string& k, std::pair<bool, bool> acquire_ret) {
  if (acquire_ret.second) return;

  auto &node = local_locks[MurmurHash64A(k.c_str(), k.length(), seed) % LOCAL_LOCK_NUM];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  bool start_window = false;
  if (!node.write_handover) {
    node.write_window = ((1UL << 8) + ticket - current) % (1UL << 8);
  }

  node.write_handover = ticket != (uint8_t)(current + 1);

  if (!node.write_handover) {  // next epoch
    node.unique_write_key = nullptr;
  }

  node.w_lock.lock();
  if (node.write_window) {
    -- node.write_window;
  }
  node.write_current.fetch_add(1);
  node.w_lock.unlock();

  return;
}

#endif // _LOCAL_LOCK_TABLE_H_
