
#ifndef SDS_SUPER_CHUNK_H
#define SDS_SUPER_CHUNK_H

#include <cstdint>
#include <cstddef>

namespace sds {
    const static size_t kMaxRootEntries = 256;

    struct SuperChunk {
        uint64_t max_chunk;
        uint64_t alloc_chunk;
        uint64_t root_entries[kMaxRootEntries];
    };

    static_assert(sizeof(SuperChunk) <= kChunkSize, "");
}

#endif //SDS_SUPER_CHUNK_H
