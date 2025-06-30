
#ifndef SDS_TARGET_H
#define SDS_TARGET_H

#include "resource_manager.h"
#include "super_chunk.h"

namespace sds {
    class Target {
    public:
        Target();

        ~Target();

        Target(const Target &) = delete;

        Target &operator=(const Target &) = delete;

    public:
        int register_main_memory(void *addr, size_t length);

        int register_device_memory(size_t length);

        int copy_from_device_memory(void *dst_addr, uint64_t src_offset, size_t length) {
            return manager_.copy_from_device_memory(dst_addr, src_offset, length);
        }

        int copy_to_device_memory(uint64_t dst_offset, void *src_addr, size_t length) {
            return manager_.copy_to_device_memory(dst_offset, src_addr, length);
        }

        void *alloc_chunk(size_t count);

        const void *base_address() const { return super_; }

        // void free_chunk(void *ptr, size_t count);

        GlobalAddress rel_ptr(void *addr);

        void set_root_entry(uint8_t index, uint64_t value) {
            assert(super_);
            super_->root_entries[index] = value;
        }

        uint64_t get_root_entry(uint8_t index) {
            assert(super_);
            return super_->root_entries[index];
        }

        int start(uint16_t tcp_port);

        int stop();

    private:
        ResourceManager manager_;
        ResourceManager::Listener *listener_;
        SuperChunk *super_;
    };
}

#endif //SDS_TARGET_H
