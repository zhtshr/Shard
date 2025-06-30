
#ifndef SDS_GLOBAL_ADDRESS_H
#define SDS_GLOBAL_ADDRESS_H

#include <cstdint>

namespace sds {
    using node_t = uint8_t;
    using mr_id_t = uint8_t;

    const static mr_id_t MAIN_MEMORY_MR_ID = 0;
    const static mr_id_t DEVICE_MEMORY_MR_ID = 1;
    const static size_t kMemoryRegions = 2;

    union GlobalAddress {
        struct {
            uint64_t offset: 48;
            uint64_t mr_id: 8;
            uint64_t node: 8;
        };
        uint64_t raw;

    public:
        GlobalAddress(uint64_t raw = UINT64_MAX) noexcept: raw(raw) {}

        GlobalAddress(node_t node, uint64_t offset) noexcept: node(node), mr_id(MAIN_MEMORY_MR_ID), offset(offset) {}

        GlobalAddress(node_t node, mr_id_t mr_id, uint64_t offset) noexcept: node(node), mr_id(mr_id), offset(offset) {}

        GlobalAddress(const GlobalAddress &rhs) noexcept: raw(rhs.raw) {}

        GlobalAddress &operator=(const GlobalAddress &rhs) {
            this->raw = rhs.raw;
            return *this;
        }

        GlobalAddress operator+(uint64_t delta) const {
            assert(offset + delta >= offset);       // prevent overflow
            return {raw + delta};
        }

        bool operator==(const GlobalAddress &rhs) const {
            return this->raw == rhs.raw;
        }

        bool operator!=(const GlobalAddress &rhs) const {
            return this->raw != rhs.raw;
        }
    };

    static_assert(sizeof(GlobalAddress) == sizeof(uint64_t), "");

    static const GlobalAddress NULL_GLOBAL_ADDRESS;
}

#endif //SDS_GLOBAL_ADDRESS_H
