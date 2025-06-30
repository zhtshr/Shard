
#include "smart/target.h"

namespace sds {
    Target::Target() : listener_(nullptr), super_(nullptr) {}

    Target::~Target() {
        stop();
    }

    int Target::register_main_memory(void *addr, size_t length) {
        if (manager_.register_main_memory(addr, length, MR_FULL_PERMISSION)) {
            return -1;
        }
        super_ = (SuperChunk *) addr;
        memset(super_, 0, kChunkSize);
        super_->max_chunk = length / kChunkSize;
        super_->alloc_chunk = 1; // chunk 0 is used as the super chunk
        return 0;
    }

    int Target::register_device_memory(size_t length) {
        return manager_.register_device_memory(length, MR_FULL_PERMISSION);
    }

    void *Target::alloc_chunk(size_t count) {
        assert(super_);
        if (super_->alloc_chunk + count > super_->max_chunk) {
            return nullptr;
        }
        uint64_t old_alloc_chunks = __sync_fetch_and_add(&super_->alloc_chunk, count);
        sfence();
        if (old_alloc_chunks + count <= super_->max_chunk) {
            return (char *) super_ + old_alloc_chunks * kChunkSize;
        } else {
            __sync_fetch_and_sub(&super_->alloc_chunk, count);
            return nullptr;
        }
    }

    GlobalAddress Target::rel_ptr(void *addr) {
        assert(super_);
        if ((uint64_t) addr >= (uint64_t) super_ &&
            (uint64_t) addr < (uint64_t) super_ + super_->max_chunk * kChunkSize) {
            return GlobalAddress(0, MAIN_MEMORY_MR_ID, (uint64_t) addr - (uint64_t) super_);
        }
        SDS_INFO("cannot find relative address");
        return NULL_GLOBAL_ADDRESS;
    }

    int Target::start(uint16_t tcp_port) {
        using namespace std::placeholders;
        if (listener_) {
            return -1;
        }
        listener_ = ResourceManager::Listener::Start(&manager_, tcp_port,
                                                     [](ResourceManager::RemoteNode *) -> int { return 0; });
        if (!listener_) {
            return -1;
        }
        return 0;
    }

    int Target::stop() {
        if (listener_) {
            listener_->stop();
            delete listener_;
            listener_ = nullptr;
        }
        return 0;
    }
}