#ifndef _HASHUTIL_H_
#define _HASHUTIL_H_

#include <sys/types.h>
#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <immintrin.h>
#include <tmmintrin.h>

static inline uint64_t MurmurHash64A ( const void * key, int len, unsigned int seed )
{
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t * data = (const uint64_t *)key;
  const uint64_t * end = data + (len/8);

  while(data != end)
  {
    uint64_t k = *data++;

    k *= m; 
    k ^= k >> r; 
    k *= m; 

    h ^= k;
    h *= m; 
  }

  const unsigned char * data2 = (const unsigned char*)data;

  switch(len & 7)
  {
    case 7: h ^= (uint64_t)data2[6] << 48;
    case 6: h ^= (uint64_t)data2[5] << 40;
    case 5: h ^= (uint64_t)data2[4] << 32;
    case 4: h ^= (uint64_t)data2[3] << 24;
    case 3: h ^= (uint64_t)data2[2] << 16;
    case 2: h ^= (uint64_t)data2[1] << 8;
    case 1: h ^= (uint64_t)data2[0];
            h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

static inline void split_hash(uint64_t hash, uint64_t log_index, uint64_t log_fp, uint64_t &index, uint8_t &fp) {
    fp = hash & ((1 << log_fp) - 1);
    index = (hash >> log_fp) & ((1 << log_index) - 1);
}

static inline void split_index(uint64_t index, uint64_t log_bins, uint64_t log_slots, uint64_t &bin, uint64_t &slot) {
    slot = index & ((1 << log_slots) - 1);
    bin = (index >> log_slots) & ((1 << log_bins) - 1);
}

static inline void split_bin(uint64_t bin, uint64_t init_log_bins, uint64_t &block, uint64_t &offset) {
    block = 64 - _lzcnt_u64(bin >> init_log_bins);
    offset = bin - (1 << block >> 1 << init_log_bins);
}

#endif  // #ifndef _HASHUTIL_H_
